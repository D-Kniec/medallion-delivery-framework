import os
import time
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))

input_path = os.path.join(project_root, "src", "bronze", "orders")
output_path = os.path.join(project_root, "src", "silver", "orders")
checkpoint_path = os.path.join(project_root, "chk", "orders_bronze_to_silver")

spark = SparkSession.builder \
    .appName("PizzaDelivery-Orders-BronzeToSilver") \
    .config("spark.sql.streaming.schemaInference", "false") \
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
    .option("maxFilesPerTrigger", 100) \
    .json(input_path)

silver_orders = raw_orders \
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp")) \
    .withColumn("order_date", F.to_date("order_timestamp")) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .filter(F.col("gross_revenue") >= 0)

query = silver_orders \
    .coalesce(1) \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .partitionBy("order_date") \
    .start()

print("Starting processing... (Press Ctrl+C to stop)")

pbar = tqdm(desc="Processing Batches (Chunks of files)", unit="batch")
last_batch_id = -1

try:
    while query.isActive:
        progress = query.lastProgress
        
        if progress:
            current_batch_id = progress['batchId']
            num_input_rows = progress['numInputRows']
            
            if current_batch_id > last_batch_id:
                pbar.update(1)
                pbar.set_postfix({"rows": num_input_rows, "latest_batch": current_batch_id})
                last_batch_id = current_batch_id
        
     
except KeyboardInterrupt:
    print("\nStopping stream...")
    query.stop()
    pbar.close()
    print("Stopped.")