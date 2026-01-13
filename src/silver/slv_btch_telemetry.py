import os
import time
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))

input_path = os.path.join(project_root, "src", "bronze", "telemetry")
output_path = os.path.join(project_root, "src", "silver", "telemetry")
checkpoint_path = os.path.join(project_root, "chk", "telemetry_bronze_to_silver")

spark = SparkSession.builder \
    .appName("PizzaDelivery-Telemetry-BronzeToSilver") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

telemetry_schema = StructType([
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
    StructField("is_delayed_delivery", BooleanType(), True)
])

raw_telemetry = spark.readStream \
    .schema(telemetry_schema) \
    .option("maxFilesPerTrigger", 200) \
    .json(input_path)


silver_telemetry = raw_telemetry \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .withColumn("event_date", F.to_date("timestamp")) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .filter((F.col("lat") != 0) & (F.col("lon") != 0))

query = silver_telemetry \
    .coalesce(1) \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .partitionBy("event_date") \
    .start()

print("Starting Telemetry processing... (Press Ctrl+C to stop)")

pbar = tqdm(desc="Processing Telemetry Batches", unit="batch")
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
        time.sleep(0.5) 

except KeyboardInterrupt:
    print("\nStopping telemetry stream...")
    query.stop()
    pbar.close()
    print("Stopped.")