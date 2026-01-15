import os
import sys
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, current_timestamp

def get_env_config():
    try:
        socket.gethostbyname('postgres')
        return 'postgres', '/opt/airflow'
    except socket.gaierror:
        local_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        return 'localhost', local_root

db_host, root_path = get_env_config()
jar_path = os.path.join(root_path, "jars", "postgresql-42.7.2.jar")
input_path = os.path.join(root_path, "src", "silver", "orders")

DB_URL = f"jdbc:postgresql://{db_host}:5432/metabase_db?stringtype=unspecified"
DB_PROPS = {"user": "user", "password": "pass", "driver": "org.postgresql.Driver", "truncate": "true"}

spark = SparkSession.builder \
    .appName("Batch-Daily-KPI") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

try:
    print(f"Reading from path: {input_path}")
    
    df = spark.read \
        .option("basePath", input_path) \
        .parquet(input_path)

    if df.isEmpty():
        print("Dataset is empty. Skipping calculation.")
        sys.exit(0)

    daily_kpi = df.groupBy("order_date").agg(
        sum("gross_revenue").alias("total_revenue"),
        sum("delivery_charge").alias("total_delivery_fees"),
        count("order_id").alias("total_orders"),
        avg("gross_revenue").alias("avg_order_value")
    ).withColumn("calculated_at", current_timestamp())

    print(f"Writing results to: {DB_URL}")
    daily_kpi.write.jdbc(url=DB_URL, table="gold.daily_sales_fact", mode="overwrite", properties=DB_PROPS)
    print("Success: Fact table updated.")

except Exception as e:
    print(f"FAILED: {str(e)}")
    raise e
finally:
    spark.stop()