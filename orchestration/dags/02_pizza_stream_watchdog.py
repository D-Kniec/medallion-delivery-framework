from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.sql import SqlSensor

default_args = {
    'owner': 'pizza_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    '02_pizza_stream_watchdog',
    default_args=default_args,
    description='MONITORING: Checks for Real-Time Data Latency',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['monitoring', 'sla']
) as dag:

    check_freshness = SqlSensor(
        task_id='check_gold_layer_freshness',
        conn_id='postgres_default',
        sql="SELECT count(*) FROM gold.live_orders_log WHERE ingestion_timestamp > NOW() - INTERVAL '30 minutes'",
        mode='reschedule',
        timeout=300,
        poke_interval=60
    )