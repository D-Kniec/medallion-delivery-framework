import logging
from pathlib import Path
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("MaintenanceCompaction")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_BASE_PATH = PROJECT_ROOT / "data" / "silver"

TABLES_CONFIG = [
    {"name": "orders", "partition": "order_date"},
    {"name": "telemetry", "partition": "event_date"}
]

def get_spark_session():
    return SparkSession.builder \
        .appName("Maintenance-Compaction") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def compact_table(spark, table_config):
    table_name = table_config["name"]
    partition_col = table_config["partition"]
    table_path = SILVER_BASE_PATH / table_name
    
    if not table_path.exists():
        logger.warning(f"Path does not exist: {table_path}. Skipping.")
        return

    logger.info(f"Starting compaction for table: {table_name}")
    
    try:
        input_pattern = str(table_path / f"{partition_col}=*")
        df = spark.read.option("basePath", str(table_path)).parquet(input_pattern)

        if df.rdd.isEmpty():
            logger.info(f"Table {table_name} is empty. No action taken.")
            return

        df.write \
            .mode("overwrite") \
            .option("maxRecordsPerFile", 100_000) \
            .partitionBy(partition_col) \
            .parquet(str(table_path))
            
        logger.info(f"Success: Table {table_name} optimized.")

    except Exception as e:
        logger.error(f"Critical error for table {table_name}: {e}")

def main():
    spark = get_spark_session()
    
    try:
        for config in TABLES_CONFIG:
            compact_table(spark, config)
    finally:
        spark.stop()
        logger.info("Maintenance process finished.")

if __name__ == "__main__":
    main()