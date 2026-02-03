CREATE DATABASE airflow_metadata;
CREATE DATABASE metabase_app_db;
CREATE USER airflow_user WITH PASSWORD 'admin';
GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO airflow_user;
\c airflow_metadata
GRANT ALL ON SCHEMA public TO airflow_user;
