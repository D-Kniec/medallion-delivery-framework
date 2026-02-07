import os
import sys
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, TimestampType, IntegerType, BooleanType, FloatType
from pathlib import Path

current_script_path = Path(__file__).resolve()
project_root = current_script_path.parents[3]
sys.path.append(str(project_root / "src"))

# ZMIANA: Usunięto suffix _v2 ze ścieżki
INPUT_DIR = os.path.join(project_root, "data", "silver", "dimension")
SECRET_PATH = os.path.join(project_root, "secrets", "warehouse_password.txt")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = "data_warehouse"
DB_USER = "warehouse_admin"

spark = SparkSession.builder \
    .appName("Gold_Loader_Postgres_DML") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def get_db_password():
    if os.path.exists(SECRET_PATH):
        with open(SECRET_PATH, "r") as f:
            return f.read().strip()
    return os.getenv("DB_PASSWORD", "warehouse_password")

def get_pg_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=get_db_password()
    )

def get_jdbc_url():
    return f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

def enforce_types(df, table_name):
    if "dim_customer" in table_name:
        df = df.withColumn("birth_year", col("birth_year").cast(IntegerType()))
        if "dropoff_difficulty_score" in df.columns:
            df = df.withColumn("dropoff_difficulty_score", col("dropoff_difficulty_score").cast(FloatType()))
            
    if "dim_date" in table_name:
        df = df.withColumn("full_date", col("full_date").cast(DateType()))
        
    if "dim_courier" in table_name:
        df = df.withColumn("valid_from", col("valid_from").cast(TimestampType()))
        df = df.withColumn("valid_to", col("valid_to").cast(TimestampType()))
        df = df.withColumn("insurance_expiry_date", col("insurance_expiry_date").cast(DateType()))
        for c in ["accident_risk_factor", "fatigue_resistance", "vehicle_range_km"]:
            if c in df.columns:
                df = df.withColumn(c, col(c).cast(FloatType()))
                
    if "dim_pizzeria" in table_name:
        df = df.withColumn("valid_from", col("valid_from").cast(TimestampType()))
        df = df.withColumn("valid_to", col("valid_to").cast(TimestampType()))
        df = df.withColumn("has_outdoor_seating", col("has_outdoor_seating").cast(BooleanType()))
        if "kitchen_performance_factor" in df.columns:
            df = df.withColumn("kitchen_performance_factor", col("kitchen_performance_factor").cast(FloatType()))
        if "lat" in df.columns:
            df = df.withColumn("lat", col("lat").cast(FloatType()))
        if "lon" in df.columns:
            df = df.withColumn("lon", col("lon").cast(FloatType()))

    if "dim_delivery_zone" in table_name:
        if "traffic_drag" in df.columns:
             df = df.withColumn("traffic_drag", col("traffic_drag").cast(FloatType()))

    return df

def write_to_staging(df, table_name):
    stg_table = f"silver.stg_{table_name.split('.')[-1]}"
    df.write \
        .format("jdbc") \
        .option("url", get_jdbc_url()) \
        .option("dbtable", stg_table) \
        .option("user", DB_USER) \
        .option("password", get_db_password()) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    return stg_table

def process_scd2_courier(pg_conn, stg_table):
    cur = pg_conn.cursor()
    
    cur.execute(f"""
        UPDATE gold.dim_courier
        SET valid_to = current_timestamp, 
            is_current = false
        FROM {stg_table} stg
        WHERE gold.dim_courier.courier_id = stg.courier_id
          AND gold.dim_courier.is_current = true
          AND (
               gold.dim_courier.courier_name IS DISTINCT FROM stg.courier_name
               OR gold.dim_courier.vehicle IS DISTINCT FROM stg.vehicle 
               OR gold.dim_courier.vehicle_ownership IS DISTINCT FROM stg.vehicle_ownership
               OR gold.dim_courier.equipment_set IS DISTINCT FROM stg.equipment_set
               OR gold.dim_courier.performance_level IS DISTINCT FROM stg.performance_level
               OR gold.dim_courier.employment_status IS DISTINCT FROM stg.employment_status
               OR gold.dim_courier.contract_type IS DISTINCT FROM stg.contract_type
               OR gold.dim_courier.accident_risk_factor IS DISTINCT FROM stg.accident_risk_factor
               OR gold.dim_courier.fatigue_resistance IS DISTINCT FROM stg.fatigue_resistance
               OR gold.dim_courier.vehicle_range_km IS DISTINCT FROM stg.vehicle_range_km
          );
    """)
    
    cur.execute(f"""
        INSERT INTO gold.dim_courier (
            courier_id, courier_name, vehicle, vehicle_type, vehicle_ownership, 
            equipment_set, performance_level, employment_status, contract_type, 
            insurance_expiry_date, accident_risk_factor, fatigue_resistance, 
            vehicle_range_km, valid_from, valid_to, is_current
        )
        SELECT 
            stg.courier_id, stg.courier_name, stg.vehicle, stg.vehicle_type, stg.vehicle_ownership, 
            stg.equipment_set, stg.performance_level, stg.employment_status, stg.contract_type, 
            stg.insurance_expiry_date, stg.accident_risk_factor, stg.fatigue_resistance, 
            stg.vehicle_range_km, current_timestamp, NULL, true
        FROM {stg_table} stg
        WHERE stg.courier_id NOT IN (
            SELECT courier_id FROM gold.dim_courier WHERE is_current = true
        );
    """)
    
    pg_conn.commit()
    print("Success SCD2: gold.dim_courier")

def process_scd1_generic(pg_conn, table_name, stg_table):
    cur = pg_conn.cursor()
    
    pk_map = {
        "gold.dim_pizzeria": "pizzeria_id",
        "gold.dim_customer": "customer_id",
        "gold.dim_delivery_zone": "zone_key",
        "gold.dim_order_status": "status_key",
        "gold.dim_date": "date_key"
    }
    
    join_key = pk_map.get(table_name)
    if not join_key:
        print(f"Unknown PK for {table_name}, skipping.")
        return

    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = '{stg_table.split('.')[-1]}'")
    columns = [row[0] for row in cur.fetchall()]
    
    update_set = ", ".join([f"{col} = stg.{col}" for col in columns if col != join_key])
    
    if update_set:
        cur.execute(f"""
            UPDATE {table_name} target
            SET {update_set}
            FROM {stg_table} stg
            WHERE target.{join_key} = stg.{join_key};
        """)

    cols_str = ", ".join(columns)
    cur.execute(f"""
        INSERT INTO {table_name} ({cols_str})
        SELECT {cols_str}
        FROM {stg_table} stg
        WHERE stg.{join_key} NOT IN (SELECT {join_key} FROM {table_name});
    """)
    
    pg_conn.commit()
    print(f"Success SCD1: {table_name}")

def load_to_postgres():
    files_to_load = [
        ("dim_courier.parquet", "gold.dim_courier"),
        ("dim_pizzeria.parquet", "gold.dim_pizzeria"),
        ("dim_customer.parquet", "gold.dim_customer"),
        ("dim_delivery_zone.parquet", "gold.dim_delivery_zone"),
        ("dim_order_status.parquet", "gold.dim_order_status"),
        ("dim_date.parquet", "gold.dim_date"),
    ]

    pg_conn = get_pg_connection()

    for filename, table_name in files_to_load:
        file_path = os.path.join(INPUT_DIR, filename)
        if not os.path.exists(file_path):
            print(f"Skipping {filename} (not found at {file_path})")
            continue

        try:
            sdf = spark.read.parquet(file_path)
            sdf = enforce_types(sdf, table_name)
            
            stg_table = write_to_staging(sdf, table_name)
            
            if table_name == "gold.dim_courier":
                process_scd2_courier(pg_conn, stg_table)
            else:
                process_scd1_generic(pg_conn, table_name, stg_table)

        except Exception as e:
            pg_conn.rollback()
            print(f"Failed: {table_name}. Error: {str(e)}")

    pg_conn.close()

if __name__ == "__main__":
    load_to_postgres()
    spark.stop()