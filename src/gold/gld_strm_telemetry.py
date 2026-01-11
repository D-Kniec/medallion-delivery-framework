import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))

input_path = os.path.join(project_root, "src", "bronze", "telemetry")
checkpoint_path = os.path.join(project_root, "chk", "gold_speed_layer")
jar_path = os.path.join(project_root, "jars", "postgresql-42.7.2.jar")

DB_URL = "jdbc:postgresql://localhost:5432/metabase_db"
DB_USER = "user"
DB_PASS = "pass"
DB_TABLE = "gold.live_courier_locations_log"

spark = SparkSession.builder \
    .appName("Gold-SpeedLayer-telemetry") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.sql.shuffle.partitions", "2") \
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
    .option("maxFilesPerTrigger", 500) \
    .json(input_path)

processed_stream = raw_telemetry \
    .filter((F.col("lat") != 0) & (F.col("lon") != 0)) \
    .select(
        F.col("courier_id"),
        F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("gps_timestamp"),
        F.col("lat"),
        F.col("lon"),
        F.col("status_key").alias("status"),
        F.col("speed"),
        F.col("realization_percent"),
        F.col("order_id"),
        F.col("customer_id"),
        
        F.col("zone_key"),
        
        F.col("is_outside_geofence"),
        F.col("is_delayed_delivery"),
        
        F.to_timestamp("order_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("order_timestamp"),
        F.to_timestamp("ready_for_pickup_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ready_for_pickup_timestamp"),
        F.to_timestamp("pickup_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("pickup_timestamp"),
        F.to_timestamp("delivery_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("delivery_timestamp"),
        F.to_timestamp("kitchen_acceptance_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("kitchen_acceptance_timestamp"),
        
        F.current_timestamp().alias("ingestion_timestamp")
    )

def write_to_postgres(df_batch, batch_id):
    if df_batch.rdd.isEmpty():
        return
    
    df_batch.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", DB_TABLE) \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

print(">>> STARTING GOLD SPEED LAYER (WITH ZONE_KEY)")

query = processed_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime='1 seconds') \
    .option("checkpointLocation", checkpoint_path) \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()