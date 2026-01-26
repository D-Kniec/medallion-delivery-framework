import pandas as pd
import numpy as np
import os
from datetime import datetime

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))

PATH_COURIERS = os.path.join(project_root, "src", "bronze", "couriers", "couriers.csv")
PATH_PIZZERIAS = os.path.join(project_root, "src", "bronze", "pizzerias", "pizzerias.csv")
PATH_CUSTOMERS = os.path.join(project_root, "src", "bronze", "customers", "customers.csv")

OUTPUT_DIR = os.path.join(project_root, "src", "silver", "dimension")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def create_static_dimensions():
    zones_data = [
        (1, "Zone 1", "Ultra-Urban", "High"),
        (2, "Zone 2", "Urban", "Medium-High"),
        (3, "Zone 3", "Residential", "Medium"),
        (4, "Zone 4", "Suburban", "Low-Medium"),
        (5, "Zone 5", "Rural", "Low")
    ]
    df_zones = pd.DataFrame(zones_data, columns=["zone_key", "zone_name", "zone_type", "average_traffic_density"])

    status_data = [
        (1, "IDLE", "Inactive", False),
        (2, "PREPARING", "In Progress", False),
        (3, "IN_REALIZATION", "In Progress", False),
        (4, "DELIVERED", "Completed", True),
        (5, "RETURNING", "Inactive", False),
        (6, "CANCELLED", "Terminated", True)
    ]
    df_status = pd.DataFrame(status_data, columns=["status_key", "status_name", "status_category", "is_final_status"])
    
    return df_zones, df_status

def create_date_dimension(start_date='2025-01-01', end_date='2027-12-31'):
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({'full_date': dates})
    
    df['date_key'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)
    df['day_name'] = df['full_date'].dt.day_name()
    df['day_of_week'] = df['full_date'].dt.dayofweek + 1
    df['month_name'] = df['full_date'].dt.month_name()
    df['month_number'] = df['full_date'].dt.month
    df['quarter'] = 'Q' + df['full_date'].dt.quarter.astype(str)
    df['year'] = df['full_date'].dt.year
    df['is_weekend'] = df['day_of_week'].isin([6, 7])
    df['is_holiday'] = False
    df['season'] = "Season"
    
    return df

def transform_couriers(path):
    df = pd.read_csv(path)
    
    df['courier_name'] = df['first_name'] + " " + df['last_name']
    df['vehicle'] = df['vehicle_type'] + " - " + df['vehicle_info']
    
    df = df.rename(columns={
        'active_status': 'employment_status',
        'ingested_at': 'valid_from'
    })
    
    df['vehicle_ownership'] = None
    df['equipment_set'] = None
    df['performance_level'] = None
    df['insurance_expiry_date'] = None
    df['valid_to'] = None
    df['is_current'] = True
    
    df['valid_from'] = pd.to_datetime(df['valid_from'])
    
    cols = [
        "courier_id", "courier_name", "vehicle", "vehicle_ownership", 
        "equipment_set", "performance_level", "employment_status", 
        "contract_type", "insurance_expiry_date", "valid_from", 
        "valid_to", "is_current"
    ]
    return df[cols]

def transform_pizzerias(path):
    df = pd.read_csv(path)
    
    df = df.rename(columns={
        'manager_info': 'manager_name',
        'address_raw': 'full_address',
        'ingested_at': 'valid_from'
    })
    
    df['store_format'] = None
    df['operating_hours_type'] = None
    df['kitchen_capacity_level'] = None
    df['region'] = None
    df['country'] = None
    df['has_outdoor_seating'] = None
    df['valid_to'] = None
    df['is_current'] = True
    
    df['valid_from'] = pd.to_datetime(df['valid_from'])
    
    cols = [
        "pizzeria_id", "pizzeria_name", "store_format", "operating_hours_type",
        "kitchen_capacity_level", "manager_name", "full_address", "city",
        "region", "country", "has_outdoor_seating", "valid_from", 
        "valid_to", "is_current"
    ]
    return df[cols]

def transform_customers(path):
    df = pd.read_csv(path)
    
    df = df.rename(columns={
        'full_name': 'customer_name',
        'segment_tag': 'loyalty_tier',
        'source_system': 'acquisition_channel'
    })
    
    df['customer_segment'] = None
    df['preferred_payment_method'] = None
    df['birth_year'] = None
    df['city'] = None
    df['postal_code'] = None
    
    cols = [
        "customer_id", "customer_name", "customer_segment", "loyalty_tier",
        "acquisition_channel", "preferred_payment_method", "birth_year",
        "city", "postal_code"
    ]
    return df[cols]

def save_to_parquet(df, filename):
    full_path = os.path.join(OUTPUT_DIR, filename)
    print(f"Saving {len(df)} rows to {full_path}...")
    df.to_parquet(full_path, index=False)

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