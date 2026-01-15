from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'pizza_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    'pizza_daily_batch_processing',
    default_args=default_args,
    description='ETL Pipeline',
    schedule_interval='0 3 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['batch']
) as batch_dag:

    update_dimensions = SparkSubmitOperator(
        task_id='update_dimensions',
        application='/opt/airflow/src/gold/gld_btch_dims_load.py',
        conn_id='spark_default',
        jars='/opt/airflow/jars/postgresql-42.7.2.jar',
        driver_class_path='/opt/airflow/jars/postgresql-42.7.2.jar',
        conf={'spark.master': 'local[*]'}
    )

    calculate_kpi = SparkSubmitOperator(
        task_id='calculate_daily_kpi',
        application='/opt/airflow/src/gold/gld_fact_daily_kpi.py',
        conn_id='spark_default',
        jars='/opt/airflow/jars/postgresql-42.7.2.jar',
        driver_class_path='/opt/airflow/jars/postgresql-42.7.2.jar',
        conf={'spark.master': 'local[*]'}
    )

    compaction = SparkSubmitOperator(
        task_id='compact_silver_files',
        application='/opt/airflow/src/silver/maintenance_compaction.py',
        conn_id='spark_default',
        conf={'spark.master': 'local[*]'}
    )

    update_dimensions >> calculate_kpi >> compaction

with DAG(
    'pizza_stream_monitor',
    default_args=default_args,
    description='Stream Watchdog',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['monitoring']
) as monitor_dag:

    check_stream = SqlSensor(
        task_id='check_orders_freshness',
        conn_id='postgres_default',
        sql="SELECT count(*) FROM gold.live_orders_log WHERE ingestion_timestamp > NOW() - INTERVAL '30 minutes'",
        mode='poke',
        timeout=60,
        poke_interval=10
    )