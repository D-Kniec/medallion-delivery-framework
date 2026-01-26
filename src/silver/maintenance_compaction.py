import os
import sys
from pyspark.sql import SparkSession

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
path_to_compact = os.path.join(project_root, "src", "silver", "orders")

spark = SparkSession.builder \
    .appName("Maintenance-Compaction") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

try:
    if os.path.exists(path_to_compact):
        df = spark.read.parquet(path_to_compact)
        
        if not df.rdd.isEmpty():
            df.repartition(1) \
              .write \
              .mode("overwrite") \
              .option("compression", "snappy") \
              .parquet(path_to_compact)

except Exception as e:
    raise e
finally:
    spark.stop()