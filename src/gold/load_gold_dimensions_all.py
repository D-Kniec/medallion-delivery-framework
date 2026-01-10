import os
from pyspark.sql import SparkSession

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
jar_path = os.path.join(project_root, "jars", "postgresql-42.7.2.jar")
INPUT_DIR = os.path.join(project_root, "src", "silver", "dimension")

DB_URL = "jdbc:postgresql://localhost:5432/metabase_db"
DB_PROPS = {
    "user": "user",
    "password": "pass",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("Gold_Loader_JDBC") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .getOrCreate()

files_to_load = [
    ("dim_delivery_zone.parquet", "gold.dim_delivery_zone"),
    ("dim_order_status.parquet", "gold.dim_order_status"),
    ("dim_date.parquet", "gold.dim_date"),
    ("dim_pizzeria.parquet", "gold.dim_pizzeria"),
    ("dim_courier.parquet", "gold.dim_courier"),
    ("dim_customer.parquet", "gold.dim_customer")
]

for filename, table_name in files_to_load:
    file_path = os.path.join(INPUT_DIR, filename)
    
    try:
        df = spark.read.parquet(file_path)
        print(f"Writing {table_name}...")
        df.write.jdbc(url=DB_URL, table=table_name, mode="overwrite", properties=DB_PROPS)
    except Exception as e:
        print(f"Error processing {table_name}: {str(e)}")

spark.stop()