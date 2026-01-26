import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def get_secret(secret_name, default=None):
    try:
        with open(f"/run/secrets/{secret_name}", "r") as file:
            return file.read().strip()
    except IOError:
        return os.getenv(secret_name.upper(), default)

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))

input_path = os.path.join(project_root, "src", "bronze", "orders")
checkpoint_path = os.path.join(project_root, "chk", "gold_speed_layer_orders")
jar_path = os.path.join(project_root, "jars", "postgresql-42.7.2.jar")

db_host = os.getenv("DB_HOST", "postgres")
db_name = os.getenv("DB_NAME", "warehouse_db")
db_user = os.getenv("DB_USER", "admin_user")
db_pass = get_secret("postgres_password", "admin_password")
db_table = "gold.live_orders_log"
db_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"

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
        F.to_timestamp("order_timestamp").alias("order_timestamp"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

def write_to_postgres(df_batch, batch_id):
    if df_batch.rdd.isEmpty():
        return
    
    try:
        df_batch.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_table) \
            .option("user", db_user) \
            .option("password", db_pass) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Batch {batch_id}: Continuing...")

query = processed_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime='2 seconds') \
    .option("checkpointLocation", checkpoint_path) \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()