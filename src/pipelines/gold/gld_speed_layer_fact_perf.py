import os
import sys
import time
import logging
import psycopg2
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("GoldSpeedLayer")

class AppConfig:
    CURRENT_FILE = Path(__file__).resolve()
    PROJECT_ROOT = CURRENT_FILE.parents[3]
    
    INPUT_PATH = str(PROJECT_ROOT / "data" / "bronze" / "telemetry")
    CHECKPOINT_PATH = str(PROJECT_ROOT / "data" / "checkpoints" / "gold_speed_layer_fact")
    SECRETS_PATH = PROJECT_ROOT / "secrets" / "warehouse_password.txt"
    
    TARGET_TABLE = "gold.fact_order_performance"
    STAGING_TABLE = "silver.stg_fact_performance"
    
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5433")
    DB_NAME = "data_warehouse"
    DB_USER = "warehouse_admin"

    @staticmethod
    def get_db_password():
        if AppConfig.SECRETS_PATH.exists():
            with open(AppConfig.SECRETS_PATH, "r") as f:
                return f.read().strip()
        return os.getenv("DB_PASSWORD", "warehouse_password")

    @staticmethod
    def get_jdbc_url():
        return f"jdbc:postgresql://{AppConfig.DB_HOST}:{AppConfig.DB_PORT}/{AppConfig.DB_NAME}"

class SQLQueries:
    UPSERT = f"""
    INSERT INTO {AppConfig.TARGET_TABLE} (
        order_id, courier_key, pizzeria_key, customer_key, status_key, zone_key,
        date_key, order_timestamp, preparation_timestamp, pickup_timestamp,
        delivery_timestamp, return_timestamp, total_distance, total_time,
        outside_geofence, is_completed
    )
    SELECT
        stg.order_id,
        COALESCE(c.courier_key, -1),
        COALESCE(p.pizzeria_key, -1),
        COALESCE(cust.customer_key, -1),
        COALESCE(s.status_key, -1),
        COALESCE(stg.zone_key, -1),
        CAST(TO_CHAR(stg.order_timestamp, 'YYYYMMDD') AS INTEGER),
        stg.order_timestamp,
        stg.preparation_timestamp,
        stg.pickup_timestamp,
        stg.delivery_timestamp,
        stg.return_timestamp,
        stg.total_distance,
        stg.total_time,
        stg.is_outside_geofence,
        stg.is_completed
    FROM {AppConfig.STAGING_TABLE} stg
    LEFT JOIN gold.dim_courier c ON stg.courier_id = c.courier_id AND c.is_current = true
    LEFT JOIN gold.dim_customer cust ON stg.customer_id = cust.customer_id
    LEFT JOIN gold.dim_order_status s ON s.status_name = stg.status_key
    LEFT JOIN gold.dim_pizzeria p 
            ON p.pizzeria_id = ('PIZ-' || split_part(stg.courier_id, '-', 2) || '-001')
            AND p.is_current = true

    ON CONFLICT (order_id) DO UPDATE SET
        courier_key = EXCLUDED.courier_key,
        status_key = EXCLUDED.status_key,
        total_distance = GREATEST(fact_order_performance.total_distance, EXCLUDED.total_distance),
        total_time = GREATEST(fact_order_performance.total_time, EXCLUDED.total_time),
        outside_geofence = EXCLUDED.outside_geofence,
        is_completed = EXCLUDED.is_completed,
        
        preparation_timestamp = COALESCE(EXCLUDED.preparation_timestamp, fact_order_performance.preparation_timestamp),
        pickup_timestamp = COALESCE(EXCLUDED.pickup_timestamp, fact_order_performance.pickup_timestamp),
        delivery_timestamp = COALESCE(EXCLUDED.delivery_timestamp, fact_order_performance.delivery_timestamp),
        return_timestamp = COALESCE(EXCLUDED.return_timestamp, fact_order_performance.return_timestamp);
    """

    CLEANUP = f"""
    DELETE FROM {AppConfig.TARGET_TABLE} 
    WHERE order_timestamp < (current_timestamp - INTERVAL '24 HOURS')
    """

def get_telemetry_schema():
    return StructType([
        StructField("event_type", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("status_key", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("courier_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("zone_key", IntegerType(), True),
        StructField("is_delayed_delivery", BooleanType(), True),
        StructField("is_outside_geofence", BooleanType(), True),
        StructField("customer_id", StringType(), True),
        StructField("total_distance", DoubleType(), True),
        StructField("time_left_sec", DoubleType(), True),
        StructField("distance_left_km", DoubleType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("preparation_timestamp", StringType(), True),
        StructField("pickup_timestamp", StringType(), True),
        StructField("delivery_timestamp", StringType(), True),
        StructField("return_timestamp", StringType(), True)
    ])

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    con = None
    try:
        window_spec = Window.partitionBy("order_id").orderBy(F.col("gps_timestamp").desc())
        deduped_df = batch_df.withColumn("rn", F.row_number().over(window_spec)) \
                             .filter(F.col("rn") == 1) \
                             .drop("rn")
        
        deduped_df.write \
            .format("jdbc") \
            .option("url", AppConfig.get_jdbc_url()) \
            .option("dbtable", AppConfig.STAGING_TABLE) \
            .option("user", AppConfig.DB_USER) \
            .option("password", AppConfig.get_db_password()) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        con = psycopg2.connect(
            host=AppConfig.DB_HOST,
            port=AppConfig.DB_PORT,
            dbname=AppConfig.DB_NAME,
            user=AppConfig.DB_USER,
            password=AppConfig.get_db_password()
        )
        cur = con.cursor()
        
        cur.execute(SQLQueries.UPSERT)
        
        if batch_id % 50 == 0:
            cur.execute(SQLQueries.CLEANUP)
            logger.info(f"Batch {batch_id}: Retention policy executed.")
            
        con.commit()
        
        count = deduped_df.count()
        logger.info(f"Batch {batch_id}: Synced {count} performance records.")

    except Exception as e:
        logger.error(f"Batch {batch_id}: Critical Error - {str(e)}")
        if con:
            con.rollback()
    finally:
        if con:
            con.close()

def main():
    while not os.path.exists(AppConfig.INPUT_PATH):
        logger.warning(f"Waiting for input path: {AppConfig.INPUT_PATH}")
        time.sleep(5)

    spark = SparkSession.builder \
        .appName("Gold-SpeedLayer-FactPerformance") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stream_df = spark.readStream \
        .schema(get_telemetry_schema()) \
        .option("maxFilesPerTrigger", 1000) \
        .json(AppConfig.INPUT_PATH)

    processed_df = stream_df \
        .filter((F.col("lat") != 0) & (F.col("lon") != 0)) \
        .select(
            F.col("order_id"),
            F.col("courier_id"),
            F.col("customer_id"),
            F.col("zone_key"),
            F.col("status_key"),
            F.col("is_outside_geofence"),
            F.col("total_distance"),
            F.col("timestamp").alias("gps_timestamp"),
            F.col("order_timestamp").cast("timestamp"),
            F.col("preparation_timestamp").cast("timestamp"),
            F.col("pickup_timestamp").cast("timestamp"),
            F.col("delivery_timestamp").cast("timestamp"),
            F.col("return_timestamp").cast("timestamp"),
            
            (F.unix_timestamp(F.col("timestamp").cast("timestamp")) - 
             F.unix_timestamp(F.col("order_timestamp").cast("timestamp"))).cast("double").alias("total_time"),
             
            F.when(F.col("status_key").isin("DELIVERED", "RETURNING"), True)
             .otherwise(False).alias("is_completed")
        )

    query = processed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", AppConfig.CHECKPOINT_PATH) \
        .trigger(processingTime='1 second') \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping stream...")
        query.stop()

if __name__ == "__main__":
    main()