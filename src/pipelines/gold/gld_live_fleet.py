import os
import sys
import time
import logging
import psycopg2
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, DecimalType

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("GoldLiveFleet")

class AppConfig:
    CURRENT_FILE = Path(__file__).resolve()
    PROJECT_ROOT = CURRENT_FILE.parents[3]
    
    INPUT_PATH = str(PROJECT_ROOT / "data" / "bronze" / "telemetry")
    CHECKPOINT_PATH = str(PROJECT_ROOT / "data" / "checkpoints" / "gold_live_fleet")
    SECRETS_PATH = PROJECT_ROOT / "secrets" / "warehouse_password.txt"
    
    TARGET_TABLE = "gold.live_fleet_status"
    STAGING_TABLE = "silver.stg_live_fleet"
    
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
        courier_id, order_id, current_lat, current_lon, order_status, 
        updated_at, speed_kmh, is_outside_geofence, time_left_sec, distance_left_km
    )
    SELECT
        courier_id,
        order_id,
        lat,
        lon,
        status_key,
        timestamp,
        speed,
        is_outside_geofence,
        time_left_sec,
        distance_left_km
    FROM {AppConfig.STAGING_TABLE}
    ON CONFLICT (courier_id) DO UPDATE SET
        order_id = EXCLUDED.order_id,
        current_lat = EXCLUDED.current_lat,
        current_lon = EXCLUDED.current_lon,
        order_status = EXCLUDED.order_status,
        updated_at = EXCLUDED.updated_at,
        speed_kmh = EXCLUDED.speed_kmh,
        is_outside_geofence = EXCLUDED.is_outside_geofence,
        time_left_sec = EXCLUDED.time_left_sec,
        distance_left_km = EXCLUDED.distance_left_km;
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
        StructField("realization_percent", DoubleType(), True),
        StructField("zone_key", IntegerType(), True),
        StructField("is_delayed_delivery", BooleanType(), True),
        StructField("is_outside_geofence", BooleanType(), True),
        StructField("customer_id", StringType(), True),
        StructField("time_left_sec", DoubleType(), True),
        StructField("distance_left_km", DoubleType(), True),
        StructField("total_distance", DoubleType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("preparation_timestamp", StringType(), True),
        StructField("kitchen_acceptance_timestamp", StringType(), True),
        StructField("ready_for_pickup_timestamp", StringType(), True),
        StructField("pickup_timestamp", StringType(), True),
        StructField("delivery_timestamp", StringType(), True)
    ])

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    con = None
    try:
        window_spec = Window.partitionBy("courier_id").orderBy(F.col("timestamp").desc())
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
        con.commit()
        
        count = deduped_df.count()
        logger.info(f"Batch {batch_id}: Synced {count} couriers.")

    except Exception as e:
        logger.error(f"Batch {batch_id}: Critical Error - {str(e)}")
    finally:
        if con:
            con.close()

def main():
    while not os.path.exists(AppConfig.INPUT_PATH):
        logger.warning(f"Waiting for input path: {AppConfig.INPUT_PATH}")
        time.sleep(5)

    spark = SparkSession.builder \
        .appName("Gold-LiveFleet") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stream_df = spark.readStream \
        .schema(get_telemetry_schema()) \
        .option("maxFilesPerTrigger", 100) \
        .json(AppConfig.INPUT_PATH)

    processed_df = stream_df \
        .filter(F.col("courier_id").isNotNull()) \
        .select(
            F.col("courier_id"),
            F.col("order_id"),
            F.col("lat").cast(DecimalType(10, 4)).alias("lat"),
            F.col("lon").cast(DecimalType(10, 4)).alias("lon"),
            F.col("status_key"), 
            F.col("timestamp").cast("timestamp").alias("timestamp"), 
            F.col("speed").cast(DecimalType(10, 2)).alias("speed"),
            F.col("is_outside_geofence"),
            F.col("time_left_sec").cast(DecimalType(10, 2)).alias("time_left_sec"),
            F.col("distance_left_km").cast(DecimalType(10, 2)).alias("distance_left_km")
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