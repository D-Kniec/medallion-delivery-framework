import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, current_timestamp

def get_secret(secret_name, default=None):
    try:
        with open(f"/run/secrets/{secret_name}", "r") as file:
            return file.read().strip()
    except IOError:
        return os.getenv(secret_name.upper(), default)

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
jar_path = os.path.join(project_root, "jars", "postgresql-42.7.2.jar")
input_path = os.path.join(project_root, "src", "silver", "orders")

db_host = os.getenv("DB_HOST", "postgres")
db_name = os.getenv("DB_NAME", "warehouse_db")
db_user = os.getenv("DB_USER", "admin_user")
db_pass = get_secret("postgres_password", "admin_password")

DB_URL = f"jdbc:postgresql://{db_host}:5432/{db_name}?stringtype=unspecified"
DB_PROPS = {
    "user": db_user,
    "password": db_pass,
    "driver": "org.postgresql.Driver",
    "truncate": "true"
}

spark = SparkSession.builder \
    .appName("Batch-Daily-KPI") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

try:
    df = spark.read \
        .option("basePath", input_path) \
        .parquet(input_path)

    if df.isEmpty():
        sys.exit(0)

    daily_kpi = df.groupBy("order_date").agg(
        sum("gross_revenue").alias("total_revenue"),
        sum("delivery_charge").alias("total_delivery_fees"),
        count("order_id").alias("total_orders"),
        avg("gross_revenue").alias("avg_order_value")
    ).withColumn("calculated_at", current_timestamp())

    daily_kpi.write.jdbc(
        url=DB_URL, 
        table="gold.daily_sales_fact", 
        mode="overwrite", 
        properties=DB_PROPS
    )

except Exception as e:
    raise e
finally:
    spark.stop()