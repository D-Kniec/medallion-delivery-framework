import os
import sys
import time
import logging
import psycopg2
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("GoldLiveOrders")

class AppConfig:
    CURRENT_FILE = Path(__file__).resolve()
    PROJECT_ROOT = CURRENT_FILE.parents[3]
    
    INPUT_PATH = str(PROJECT_ROOT / "data" / "bronze" / "orders")
    CHECKPOINT_PATH = str(PROJECT_ROOT / "data" / "checkpoints" / "gold_live_orders")
    SECRETS_PATH = PROJECT_ROOT / "secrets" / "warehouse_password.txt"
    
    TARGET_TABLE = "gold.live_orders_log"
    
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
    CLEANUP = f"""
    DELETE FROM {AppConfig.TARGET_TABLE} 
    WHERE order_timestamp < (current_timestamp - INTERVAL '24 HOURS')
    """

def get_orders_schema():
    return StructType([
        StructField("event_type", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("gross_revenue", DoubleType(), True),
        StructField("delivery_charge", DoubleType(), True),
        StructField("pizzeria_lat", DoubleType(), True),
        StructField("pizzeria_lon", DoubleType(), True),
        StructField("order_timestamp", StringType(), True)
    ])

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    con = None
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", AppConfig.get_jdbc_url()) \
            .option("dbtable", AppConfig.TARGET_TABLE) \
            .option("user", AppConfig.DB_USER) \
            .option("password", AppConfig.get_db_password()) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"Batch {batch_id}: Added new orders.")
        
        if batch_id % 20 == 0:
            con = psycopg2.connect(
                host=AppConfig.DB_HOST,
                port=AppConfig.DB_PORT,
                dbname=AppConfig.DB_NAME,
                user=AppConfig.DB_USER,
                password=AppConfig.get_db_password()
            )
            cur = con.cursor()
            cur.execute(SQLQueries.CLEANUP)
            con.commit()
            logger.info(f"Batch {batch_id}: Retention policy executed.")

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
        .appName("Gold-LiveOrders") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stream_df = spark.readStream \
        .schema(get_orders_schema()) \
        .option("maxFilesPerTrigger", 100) \
        .json(AppConfig.INPUT_PATH)

    processed_df = stream_df \
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.round(F.col("gross_revenue"), 2).alias("gross_revenue"),
            F.round(F.col("delivery_charge"), 2).alias("delivery_charge"),
            F.col("pizzeria_lat"),
            F.col("pizzeria_lon"),
            F.to_timestamp("order_timestamp").alias("order_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp")
        )

    query = processed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", AppConfig.CHECKPOINT_PATH) \
        .trigger(processingTime='2 seconds') \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping stream...")
        query.stop()

if __name__ == "__main__":
    main()