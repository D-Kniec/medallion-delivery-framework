import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

current_script_path = Path(__file__).resolve()
project_root = current_script_path.parents[3]

input_path = str(project_root / "data" / "bronze" / "orders")
output_path = str(project_root / "data" / "silver" / "orders")
checkpoint_path = str(project_root / "data" / "checkpoints" / "orders_bronze_to_silver_v2")

spark = SparkSession.builder \
    .appName("PizzaDelivery-Orders-BronzeToSilver") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .config("spark.sql.streaming.fileSource.cleanSource", "archive") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

orders_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("gross_revenue", DoubleType(), True),
    StructField("delivery_charge", DoubleType(), True),
    StructField("pizzeria_lat", DoubleType(), True),
    StructField("pizzeria_lon", DoubleType(), True)
])

raw_orders = spark.readStream \
    .schema(orders_schema) \
    .option("maxFilesPerTrigger", 10000) \
    .option("cleanSource", "delete") \
    .json(input_path)

silver_orders = raw_orders \
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp")) \
    .withColumn("order_date", F.to_date("order_timestamp")) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .filter(F.col("gross_revenue") >= 0)

query = silver_orders \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .option("maxRecordsPerFile", 100000) \
    .partitionBy("order_date") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()