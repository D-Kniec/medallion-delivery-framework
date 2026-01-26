CREATE DATABASE airflow_metadata;
CREATE DATABASE warehouse_db;

CREATE USER airflow_user WITH PASSWORD 'AirflowSecurePass';
GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO airflow_user;
ALTER DATABASE airflow_metadata OWNER TO airflow_user;

CREATE USER admin_user WITH PASSWORD 'SuperSecretAdminPass';
GRANT ALL PRIVILEGES ON DATABASE warehouse_db TO admin_user;
ALTER DATABASE warehouse_db OWNER TO admin_user;

\c warehouse_db

CREATE SCHEMA IF NOT EXISTS gold;
ALTER SCHEMA gold OWNER TO admin_user;

-- POPRAWNA DEFINICJA TABELI (Z wszystkimi kolumnami)
CREATE TABLE gold.live_orders_log (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_timestamp TIMESTAMP,
    gross_revenue DECIMAL(10,2),
    delivery_charge DECIMAL(10,2),
    pizzeria_lat DECIMAL(9,6),
    pizzeria_lon DECIMAL(9,6),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE gold.live_orders_log OWNER TO admin_user;
