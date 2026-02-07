import os
import random
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType, DateType, FloatType, StringType

current_script_path = Path(__file__).resolve()
project_root = current_script_path.parents[3]

PATH_COURIERS = str(project_root / "data" / "bronze" / "couriers" / "couriers.csv")
PATH_PIZZERIAS = str(project_root / "data" / "bronze" / "pizzerias" / "pizzerias.csv")
PATH_CUSTOMERS = str(project_root / "data" / "bronze" / "customers" / "customers.csv")

OUTPUT_DIR = str(project_root / "data" / "silver" / "dimension")

spark = SparkSession.builder \
    .appName("Silver-Dimensions-Loader-Static") \
    .getOrCreate()

def create_static_dimensions():
    zones_data = [
        (1, "Zone 1", "Ultra-Urban (Center)", 0.85),
        (2, "Zone 2", "Urban", 0.60),
        (3, "Zone 3", "Residential", 0.35),
        (4, "Zone 4", "Suburban", 0.15),
        (5, "Zone 5", "Rural/Outskirts", 0.05)
    ]
    df_zones = spark.createDataFrame(zones_data, ["zone_key", "zone_name", "zone_type", "traffic_drag"])

    status_data = [
        (1, "IDLE", "Inactive", False),
        (2, "PREPARING", "In Progress", False),
        (3, "IN_REALIZATION", "In Progress", False),
        (4, "AT_CUSTOMER", "In Progress", False),
        (5, "DELIVERED", "Completed", True),
        (6, "RETURNING", "Inactive", False),
        (7, "CRITICAL_FAILURE", "Terminated", True)
    ]
    df_status = spark.createDataFrame(status_data, ["status_key", "status_name", "status_category", "is_final_status"])
    
    return df_zones, df_status

def create_date_dimension(start_date='2025-01-01', end_date='2027-12-31'):
    df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date
    """)
    
    df = df.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType())) \
           .withColumn("day_name", F.date_format("full_date", "EEEE")) \
           .withColumn("day_of_week", F.dayofweek("full_date")) \
           .withColumn("month_name", F.date_format("full_date", "MMMM")) \
           .withColumn("month_number", F.month("full_date")) \
           .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("full_date"))) \
           .withColumn("year", F.year("full_date")) \
           .withColumn("is_weekend", F.col("day_of_week").isin([1, 7])) \
           .withColumn("is_holiday", F.lit(False)) \
           .withColumn("season", F.lit("Season"))
           
    return df

def transform_couriers(path):
    df = spark.read.option("header", True).csv(path)
    
    df = df.withColumn("courier_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))) \
           .withColumn("vehicle", F.concat_ws(" - ", F.col("vehicle_type"), F.col("vehicle_info"))) \
           .withColumnRenamed("active_status", "employment_status") \
           .withColumn("valid_from", F.to_timestamp("ingested_at")) \
           .withColumn("insurance_expiry_date", F.date_add(F.current_date(), 365)) \
           .withColumn("valid_to", F.lit(None).cast("timestamp")) \
           .withColumn("is_current", F.lit(True)) \
           .withColumn("accident_risk_factor", F.col("accident_risk_factor").cast(FloatType())) \
           .withColumn("fatigue_resistance", F.col("fatigue_resistance").cast(FloatType())) \
           .withColumn("vehicle_range_km", F.col("vehicle_range_km").cast(FloatType()))

    cols = [
        "courier_id", "courier_name", "vehicle", "vehicle_type", 
        "vehicle_ownership", "equipment_set", "performance_level", 
        "employment_status", "contract_type", "insurance_expiry_date", 
        "accident_risk_factor", "fatigue_resistance", "vehicle_range_km",
        "valid_from", "valid_to", "is_current"
    ]
    return df.select(cols)

def transform_pizzerias(path):
    df = spark.read.option("header", True).csv(path)
    
    df = df.withColumnRenamed("manager_info", "manager_name") \
           .withColumnRenamed("address_raw", "full_address") \
           .withColumn("valid_from", F.to_timestamp("ingested_at")) \
           .withColumn("store_format", F.lit("Express").cast("string")) \
           .withColumn("operating_hours_type", F.lit("Standard").cast("string")) \
           .withColumn("kitchen_capacity_level", F.lit("High").cast("string")) \
           .withColumn("region", F.lit("PL-Main").cast("string")) \
           .withColumn("country", F.lit("Poland").cast("string")) \
           .withColumn("has_outdoor_seating", F.lit(False).cast(BooleanType())) \
           .withColumn("valid_to", F.lit(None).cast("timestamp")) \
           .withColumn("is_current", F.lit(True)) \
           .withColumn("kitchen_performance_factor", F.round(0.8 + F.rand() * 0.4, 2)) \
           .withColumn("lat", F.col("lat").cast(FloatType())) \
           .withColumn("lon", F.col("lon").cast(FloatType()))

    cols = [
        "pizzeria_id", "pizzeria_name", "store_format", "operating_hours_type",
        "kitchen_capacity_level", "manager_name", "full_address", "city",
        "region", "country", "has_outdoor_seating", "kitchen_performance_factor",
        "lat", "lon", "valid_from", "valid_to", "is_current"
    ]
    return df.select(cols)

def transform_customers(path):
    df = spark.read.option("header", True).csv(path)
    
    df = df.withColumnRenamed("full_name", "customer_name") \
           .withColumnRenamed("segment_tag", "loyalty_tier") \
           .withColumnRenamed("source_system", "acquisition_channel") \
           .withColumn("customer_segment", F.lit("Retail").cast("string")) \
           .withColumn("preferred_payment_method", F.lit("App").cast("string")) \
           .withColumn("birth_year", F.lit(1990).cast(IntegerType())) \
           .withColumn("city", F.lit("Warsaw").cast("string")) \
           .withColumn("postal_code", F.lit("00-001").cast("string")) \
           .withColumn("dropoff_difficulty_score", F.round(F.rand(), 2))
           
    cols = [
        "customer_id", "customer_name", "customer_segment", "loyalty_tier",
        "acquisition_channel", "preferred_payment_method", "birth_year",
        "city", "postal_code", "dropoff_difficulty_score"
    ]
    return df.select(cols)

def save_to_parquet(df, filename):
    full_path = os.path.join(OUTPUT_DIR, filename)
    df.write.mode("overwrite").parquet(full_path)

if __name__ == "__main__":
    df_zones, df_status = create_static_dimensions()
    df_date = create_date_dimension()
    
    df_courier = transform_couriers(PATH_COURIERS)
    df_pizzeria = transform_pizzerias(PATH_PIZZERIAS)
    df_customer = transform_customers(PATH_CUSTOMERS)
    
    save_to_parquet(df_zones, "dim_delivery_zone.parquet")
    save_to_parquet(df_status, "dim_order_status.parquet")
    save_to_parquet(df_date, "dim_date.parquet")
    
    save_to_parquet(df_pizzeria, "dim_pizzeria.parquet")
    save_to_parquet(df_courier, "dim_courier.parquet")
    save_to_parquet(df_customer, "dim_customer.parquet")

    spark.stop()