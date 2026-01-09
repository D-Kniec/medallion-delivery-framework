import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, concat_ws, when, 
    dayofweek, date_format, month, quarter, year, 
    explode, sequence, to_date
)
from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType, TimestampType

# setup environment
current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
jar_path = os.path.join(project_root, "jars", "postgresql-42.7.2.jar")


PATH_COURIERS = os.path.join(project_root, "src", "bronze", "couriers", "couriers.csv")
PATH_PIZZERIAS = os.path.join(project_root, "src", "bronze", "pizzerias", "pizzerias.csv")
PATH_CUSTOMERS = os.path.join(project_root, "src", "bronze", "customers", "customers.csv")


DB_URL = "jdbc:postgresql://localhost:5432/metabase_db"
DB_PROPS = {"user": "user", "password": "pass", "driver": "org.postgresql.Driver"}

spark = SparkSession.builder \
    .appName("Gold_Dimensions_Loader") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .getOrCreate()

def write_db(df, table, mode="append"):
    print(f"Writing {df.count()} rows to {table}...")
    df.write.jdbc(url=DB_URL, table=table, mode=mode, properties=DB_PROPS)

# --- 1. Static/Generated Dimensions 

# zones
zones_data = [
    (1, "Zone 1", "Ultra-Urban", "High"),
    (2, "Zone 2", "Urban", "Medium-High"),
    (3, "Zone 3", "Residential", "Medium"),
    (4, "Zone 4", "Suburban", "Low-Medium"),
    (5, "Zone 5", "Rural", "Low")
]
df_zones = spark.createDataFrame(zones_data, ["zone_key", "zone_name", "zone_type", "average_traffic_density"])

# statuses
status_data = [
    (1, "IDLE", "Inactive", False),
    (2, "PREPARING", "In Progress", False),
    (3, "IN_REALIZATION", "In Progress", False),
    (4, "DELIVERED", "Completed", True),
    (5, "RETURNING", "Inactive", False),
    (6, "CANCELLED", "Terminated", True)
]
df_status = spark.createDataFrame(status_data, ["status_key", "status_name", "status_category", "is_final_status"])

# calendar (generated)
df_date = spark.sql("SELECT explode(sequence(to_date('2025-01-01'), to_date('2027-12-31'), interval 1 day)) as full_date") \
    .select(
        (year("full_date") * 10000 + month("full_date") * 100 + dayofweek("full_date")).alias("date_key"),
        col("full_date"),
        date_format("full_date", "EEEE").alias("day_name"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "MMMM").alias("month_name"),
        month("full_date").alias("month_number"),
        concat_ws("", lit("Q"), quarter("full_date")).alias("quarter"),
        year("full_date").alias("year"),
        when(dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        lit(False).alias("is_holiday"),
        lit("Season").alias("season")
    )

# --- 2. Main Dimensions 

# couriers
df_courier = spark.read.option("header", "true").option("inferSchema", "true").csv(PATH_COURIERS) \
    .select(
        col("courier_id"),
        concat_ws(" ", col("first_name"), col("last_name")).alias("courier_name"),
        concat_ws(" - ", col("vehicle_type"), col("vehicle_info")).alias("vehicle"),
        lit(None).cast(StringType()).alias("vehicle_ownership"),
        lit(None).cast(StringType()).alias("equipment_set"),
        lit(None).cast(StringType()).alias("performance_level"),
        col("active_status").alias("employment_status"),
        col("contract_type"),
        lit(None).cast(DateType()).alias("insurance_expiry_date"),
        to_timestamp("ingested_at").alias("valid_from"),
        lit(None).cast(TimestampType()).alias("valid_to"),
        lit(True).alias("is_current")
    )

# pizzerias
df_pizzeria = spark.read.option("header", "true").option("inferSchema", "true").csv(PATH_PIZZERIAS) \
    .select(
        col("pizzeria_id"),
        col("pizzeria_name"),
        lit(None).cast(StringType()).alias("store_format"),
        lit(None).cast(StringType()).alias("operating_hours_type"),
        lit(None).cast(StringType()).alias("kitchen_capacity_level"),
        col("manager_info").alias("manager_name"),
        col("address_raw").alias("full_address"),
        col("city"),
        lit(None).cast(StringType()).alias("region"),
        lit(None).cast(StringType()).alias("country"),
        lit(None).cast(BooleanType()).alias("has_outdoor_seating"),
        to_timestamp("ingested_at").alias("valid_from"),
        lit(None).cast(TimestampType()).alias("valid_to"),
        lit(True).alias("is_current")
    )

# customers
df_customer = spark.read.option("header", "true").option("inferSchema", "true").csv(PATH_CUSTOMERS) \
    .select(
        col("customer_id"),
        col("full_name").alias("customer_name"),
        lit(None).cast(StringType()).alias("customer_segment"),
        col("segment_tag").alias("loyalty_tier"),
        col("source_system").alias("acquisition_channel"),
        lit(None).cast(StringType()).alias("preferred_payment_method"),
        lit(None).cast(IntegerType()).alias("birth_year"),
        lit(None).cast(StringType()).alias("city"),
        lit(None).cast(StringType()).alias("postal_code")
    )

# --- 3. Execution ---

write_db(df_zones, "gold.dim_delivery_zone", "overwrite")
write_db(df_status, "gold.dim_order_status", "overwrite")
write_db(df_date, "gold.dim_date", "overwrite")

write_db(df_pizzeria, "gold.dim_pizzeria", "append")
write_db(df_courier, "gold.dim_courier", "append")
write_db(df_customer, "gold.dim_customer", "append")

spark.stop()