CREATE SCHEMA IF NOT EXISTS gold;

DROP VIEW IF EXISTS gold.v_order_360;
DROP TABLE IF EXISTS gold.fact_order_performance CASCADE;
DROP TABLE IF EXISTS gold.dim_courier CASCADE;
DROP TABLE IF EXISTS gold.dim_pizzeria CASCADE;
DROP TABLE IF EXISTS gold.dim_customer CASCADE;
DROP TABLE IF EXISTS gold.dim_delivery_zone CASCADE;
DROP TABLE IF EXISTS gold.dim_order_status CASCADE;
DROP TABLE IF EXISTS gold.dim_date CASCADE;
DROP TABLE IF EXISTS gold.live_fleet_status CASCADE;
DROP TABLE IF EXISTS gold.live_orders_log CASCADE;


CREATE TABLE gold.dim_courier (
    courier_key SERIAL PRIMARY KEY, 
    courier_id VARCHAR,
    courier_name VARCHAR,
    vehicle VARCHAR,
    vehicle_type VARCHAR,           
    vehicle_ownership VARCHAR,
    equipment_set VARCHAR,
    performance_level VARCHAR,
    employment_status VARCHAR,
    contract_type VARCHAR,
    insurance_expiry_date DATE,
    accident_risk_factor DECIMAL(10,4),
    fatigue_resistance DECIMAL(10,2),
    vehicle_range_km DECIMAL(10,2),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);

CREATE TABLE gold.dim_pizzeria (
    pizzeria_key SERIAL PRIMARY KEY, 
    pizzeria_id VARCHAR,
    pizzeria_name VARCHAR,
    store_format VARCHAR,
    operating_hours_type VARCHAR,
    kitchen_capacity_level VARCHAR,
    manager_name VARCHAR,
    full_address VARCHAR,
    city VARCHAR,
    region VARCHAR,
    country VARCHAR,
    has_outdoor_seating BOOLEAN,
    kitchen_performance_factor DECIMAL(10,2),
    lat DECIMAL(10,4),
    lon DECIMAL(10,4),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);

CREATE TABLE gold.dim_customer (
    customer_key SERIAL PRIMARY KEY, 
    customer_id VARCHAR,
    customer_name VARCHAR,
    customer_segment VARCHAR,
    loyalty_tier VARCHAR,
    acquisition_channel VARCHAR,
    preferred_payment_method VARCHAR,
    birth_year INTEGER,
    city VARCHAR,
    postal_code VARCHAR,
    dropoff_difficulty_score DECIMAL(10,2)
);

CREATE TABLE gold.dim_delivery_zone (
    zone_key INTEGER PRIMARY KEY, 
    zone_name VARCHAR,
    zone_type VARCHAR,
    traffic_drag DECIMAL(10,2)
);

CREATE TABLE gold.dim_order_status (
    status_key INTEGER PRIMARY KEY,
    status_name VARCHAR,
    status_category VARCHAR,
    is_final_status BOOLEAN
);

CREATE TABLE gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    day_name VARCHAR,
    day_of_week INTEGER,
    month_name VARCHAR,
    month_number INTEGER,
    quarter VARCHAR,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    season VARCHAR
);

CREATE TABLE gold.live_fleet_status (
    courier_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    current_lat DECIMAL(10,4),
    current_lon DECIMAL(10,4),
    order_status VARCHAR,          
    updated_at TIMESTAMP,
    speed_kmh DECIMAL(10,2),
    is_outside_geofence BOOLEAN,
    time_left_sec DECIMAL(10,2),
    distance_left_km DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS gold.live_orders_log (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    gross_revenue DECIMAL(10,2),
    delivery_charge DECIMAL(10,2),
    pizzeria_lat DECIMAL(10,4),
    pizzeria_lon DECIMAL(10,4),
    order_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP
);

CREATE TABLE gold.fact_order_performance (
    order_id VARCHAR PRIMARY KEY,
    courier_key INTEGER,
    pizzeria_key INTEGER,
    customer_key INTEGER,
    status_key INTEGER,
    zone_key INTEGER,
    date_key INTEGER,
    order_timestamp TIMESTAMP,
    preparation_timestamp TIMESTAMP,
    pickup_timestamp TIMESTAMP,
    delivery_timestamp TIMESTAMP,
    return_timestamp TIMESTAMP,
    total_distance DECIMAL(10,2),
    total_time DECIMAL(10,2),
    outside_geofence BOOLEAN,
    is_completed BOOLEAN,
    FOREIGN KEY (courier_key) REFERENCES gold.dim_courier(courier_key),
    FOREIGN KEY (pizzeria_key) REFERENCES gold.dim_pizzeria(pizzeria_key),
    FOREIGN KEY (customer_key) REFERENCES gold.dim_customer(customer_key),
    FOREIGN KEY (status_key) REFERENCES gold.dim_order_status(status_key),
    FOREIGN KEY (zone_key) REFERENCES gold.dim_delivery_zone(zone_key),
    FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key)
);

CREATE OR REPLACE VIEW gold.v_order_360 AS
SELECT 
    f.order_id,
    c.courier_name,
    c.vehicle AS vehicle_type,
    p.pizzeria_name,
    cust.customer_name,
    s.status_name AS raw_status,
    z.zone_name,
    
    CASE 
        WHEN s.status_name IN ('DELIVERED', 'RETURNING') THEN 100.0
        WHEN s.status_name = 'AT_CUSTOMER' THEN 99.0
        WHEN s.status_name = 'IN_REALIZATION' AND f.pickup_timestamp IS NOT NULL THEN
            LEAST(95.0, ROUND(CAST((EXTRACT(EPOCH FROM (current_timestamp - f.pickup_timestamp)) / 120.0) * 100 AS NUMERIC), 2))
        WHEN s.status_name = 'PREPARING' THEN 10.0
        ELSE 0.0 
    END AS realization_percent,

    CASE 
        WHEN s.status_name = 'PREPARING' THEN 'KITCHEN_PREP'
        WHEN s.status_name = 'IN_REALIZATION' THEN 'ON_ROUTE'
        WHEN s.status_name = 'AT_CUSTOMER' THEN 'AT_DOOR'
        WHEN s.status_name IN ('DELIVERED', 'RETURNING') THEN 'COMPLETED'
        WHEN s.status_name = 'CRITICAL_FAILURE' THEN 'ISSUE'
        ELSE 'PENDING'
    END AS simple_order_status,

    f.order_timestamp,
    f.preparation_timestamp,
    f.pickup_timestamp,
    f.delivery_timestamp,
    
    EXTRACT(EPOCH FROM (f.preparation_timestamp - f.order_timestamp)) AS preparation_time_sec,
    EXTRACT(EPOCH FROM (f.pickup_timestamp - f.preparation_timestamp)) AS pizza_idle_time_sec,
    f.total_time AS total_time_sec,

    CASE 
        WHEN f.total_time > 0 THEN ROUND(CAST((f.total_distance / (f.total_time / 3600.0)) AS NUMERIC), 2) 
        ELSE 0.00 
    END AS average_speed_kmh,
    
    f.outside_geofence,
    CASE WHEN f.total_time > 120 THEN true ELSE false END AS delayed_delivery,
    
    COALESCE(l.gross_revenue, 0.00) AS gross_revenue,
    COALESCE(l.delivery_charge, 0.00) AS delivery_charge

FROM gold.fact_order_performance f
LEFT JOIN gold.live_orders_log l ON f.order_id = l.order_id
LEFT JOIN gold.dim_courier c ON f.courier_key = c.courier_key
LEFT JOIN gold.dim_pizzeria p ON f.pizzeria_key = p.pizzeria_key
LEFT JOIN gold.dim_customer cust ON f.customer_key = cust.customer_key
LEFT JOIN gold.dim_order_status s ON f.status_key = s.status_key
LEFT JOIN gold.dim_delivery_zone z ON f.zone_key = z.zone_key;