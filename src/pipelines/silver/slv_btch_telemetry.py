import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

current_script_path = Path(__file__).resolve()
project_root = current_script_path.parents[3]

input_path = str(project_root / "data" / "bronze" / "telemetry")
output_path = str(project_root / "data" / "silver" / "telemetry")
checkpoint_path = str(project_root / "data" / "checkpoints" / "telemetry_bronze_to_silver_v2")

spark = SparkSession.builder \
    .appName("PizzaDelivery-Telemetry-BronzeToSilver") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .config("spark.sql.streaming.fileSource.cleanSource", "archive") \
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
    StructField("is_delayed_delivery", BooleanType(), True),
    StructField("is_outside_geofence", BooleanType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("kitchen_acceptance_timestamp", StringType(), True),
    StructField("ready_for_pickup_timestamp", StringType(), True),
    StructField("pickup_timestamp", StringType(), True),
    StructField("delivery_timestamp", StringType(), True)
])

raw_telemetry = spark.readStream \
    .schema(telemetry_schema) \
    .option("maxFilesPerTrigger", 10000) \
    .option("cleanSource", "delete") \
    .json(input_path)

silver_telemetry = raw_telemetry \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp")) \
    .withColumn("delivery_timestamp", F.to_timestamp("delivery_timestamp")) \
    .withColumn("event_date", F.to_date("timestamp")) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .filter((F.col("lat") != 0) & (F.col("lon") != 0))

query = silver_telemetry \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .option("maxRecordsPerFile", 100000) \
    .partitionBy("event_date") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()