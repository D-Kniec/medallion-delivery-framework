import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import NullType, TimestampType, DateType, IntegerType, BooleanType

def get_secret(secret_name, default=None):
    try:
        with open(f"/run/secrets/{secret_name}", "r") as file:
            return file.read().strip()
    except IOError:
        return os.getenv(secret_name.upper(), default)

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
jar_path = os.path.join(project_root, "jars", "postgresql-42.7.2.jar")
INPUT_DIR = os.path.join(project_root, "src", "silver", "dimension")

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
    .appName("Gold_Loader_JDBC") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.sql.parquet.nanosAsLong", "true") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

def enforce_schema_types(df, table_name):
    if "dim_customer" in table_name:
        df = df.withColumn("birth_year", col("birth_year").cast(IntegerType()))
    
    if "dim_date" in table_name:
        df = df.withColumn("full_date", (col("full_date") / 1000000000).cast(TimestampType()).cast(DateType()))
    
    if "dim_courier" in table_name:
        df = df.withColumn("valid_from", (col("valid_from") / 1000000000).cast(TimestampType()))
        df = df.withColumn("valid_to", (col("valid_to") / 1000000000).cast(TimestampType()))
        df = df.withColumn("insurance_expiry_date", col("insurance_expiry_date").cast(DateType()))

    if "dim_pizzeria" in table_name:
        df = df.withColumn("valid_from", (col("valid_from") / 1000000000).cast(TimestampType()))
        df = df.withColumn("valid_to", (col("valid_to") / 1000000000).cast(TimestampType()))
        df = df.withColumn("has_outdoor_seating", col("has_outdoor_seating").cast(BooleanType()))

    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(field.name, col(field.name).cast("string"))
            
    return df

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
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        continue

    try:
        print(f"Loading {table_name}...")
        df = spark.read.parquet(file_path)
        df = enforce_schema_types(df, table_name)
        
        df.write.jdbc(
            url=DB_URL, 
            table=table_name, 
            mode="overwrite", 
            properties=DB_PROPS
        )
        print(f"Success: {table_name}")
        
    except Exception as e:
        print(f"Failed: {table_name}. Error: {str(e)}")

spark.stop()