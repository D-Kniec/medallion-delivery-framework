import os
import sys
import argparse
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, count, avg, current_timestamp
from pathlib import Path

current_script_path = Path(__file__).resolve()
project_root = current_script_path.parents[3]
sys.path.append(str(project_root / "src"))

parser = argparse.ArgumentParser()
parser.add_argument("--process_date", required=True)
args = parser.parse_args()
process_date = args.process_date

input_path = os.path.join(project_root, "data", "silver", "orders")
SECRETS_PATH = os.path.join(project_root, "secrets", "warehouse_password.txt")
TARGET_TABLE = "gold.daily_sales_fact"
STAGING_TABLE = "silver.stg_daily_sales_fact"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = "data_warehouse"
DB_USER = "warehouse_admin"

spark = SparkSession.builder \
    .appName(f"Batch-Daily-KPI-{process_date}") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def get_db_password():
    if os.path.exists(SECRETS_PATH):
        with open(SECRETS_PATH, "r") as f:
            return f.read().strip()
    return os.getenv("DB_PASSWORD", "warehouse_password")

def get_jdbc_url():
    return f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

def write_to_postgres(df, date_val):
    password = get_db_password()
    jdbc_url = get_jdbc_url()
    
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", STAGING_TABLE) \
        .option("user", DB_USER) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
        
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=password
        )
        cur = conn.cursor()
        
        cur.execute(f"DELETE FROM {TARGET_TABLE} WHERE order_date = '{date_val}'")
        
        cur.execute(f"""
            INSERT INTO {TARGET_TABLE} (
                order_date, total_revenue, total_delivery_fees, 
                total_orders, avg_order_value, calculated_at
            )
            SELECT 
                order_date, total_revenue, total_delivery_fees, 
                total_orders, avg_order_value, calculated_at
            FROM {STAGING_TABLE}
        """)
        
        conn.commit()
        print(f"Success: KPI for {date_val} loaded to {TARGET_TABLE}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

def main():
    try:
        partition_dir = os.path.join(input_path, f"order_date={process_date}")
        
        if not os.path.exists(partition_dir):
            print(f"No data found for date {process_date}")
            sys.exit(0)

        df = spark.read.parquet(partition_dir)
        
        df = df.withColumn("order_date", lit(process_date).cast("date"))

        if df.rdd.isEmpty():
            print(f"Partition exists but is empty for date {process_date}")
            sys.exit(0)

        daily_kpi = df.groupBy("order_date").agg(
            sum("gross_revenue").cast("decimal(10,2)").alias("total_revenue"),
            sum("delivery_charge").cast("decimal(10,2)").alias("total_delivery_fees"),
            count("order_id").cast("int").alias("total_orders"),
            avg("gross_revenue").cast("decimal(10,2)").alias("avg_order_value")
        ).withColumn("calculated_at", current_timestamp())

        write_to_postgres(daily_kpi, process_date)

    except Exception as e:
        print(f"Job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()