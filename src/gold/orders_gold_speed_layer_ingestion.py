import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))

input_path = os.path.join(project_root, "src", "bronze", "orders")
checkpoint_path = os.path.join(project_root, "chk", "gold_speed_layer_orders")
jar_path = os.path.join(project_root, "jars", "postgresql-42.7.2.jar")

DB_URL = "jdbc:postgresql://localhost:5432/metabase_db"
DB_USER = "user"
DB_PASS = "pass"
DB_TABLE = "gold.live_orders_log"

spark = SparkSession.builder \
    .appName("Gold-SpeedLayer-Orders") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("gross_revenue", DoubleType(), True),
    StructField("delivery_charge", DoubleType(), True),
    StructField("pizzeria_lat", DoubleType(), True),
    StructField("pizzeria_lon", DoubleType(), True),
    StructField("order_timestamp", StringType(), True) 
])

raw_orders = spark.readStream \
    .schema(orders_schema) \
    .option("maxFilesPerTrigger", 500) \
    .json(input_path)

processed_stream = raw_orders \
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("gross_revenue"),
        F.col("delivery_charge"),
        F.col("pizzeria_lat"),
        F.col("pizzeria_lon"),
        F.to_timestamp("order_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("order_ts"),
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

print(">>> STARTING GOLD ORDERS LAYER")

query = processed_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime='1 seconds') \
    .option("checkpointLocation", checkpoint_path) \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()