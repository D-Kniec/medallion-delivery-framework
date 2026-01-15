import os
from pyspark.sql import SparkSession

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
path_to_compact = os.path.join(project_root, "src", "silver", "orders")

spark = SparkSession.builder \
    .appName("Maintenance-Compaction") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

if os.path.exists(path_to_compact):
    df = spark.read.parquet(path_to_compact)
    (df.repartition(1)
       .write
       .mode("overwrite")
       .option("compression", "snappy")
       .parquet(path_to_compact))

spark.stop()