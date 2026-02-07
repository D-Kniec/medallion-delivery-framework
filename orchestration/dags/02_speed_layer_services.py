import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

default_args = {
    'owner': 'speed_layer',
    'depends_on_past': False,
    'retries': 10,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='02_speed_layer_services',
    default_args=default_args,
    description='REALTIME: Generators & Spark Streaming Jobs',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['streaming', 'generator', 'services'],
) as dag:

    env_config = {**os.environ, "PYTHONPATH": PROJECT_ROOT}

    sim_generator = BashOperator(
        task_id='service_data_generator',
        bash_command=f'python {PROJECT_ROOT}/src/generator/gen_telemetry.py',
        env=env_config
    )

    stream_fleet = BashOperator(
        task_id='service_stream_fleet',
        bash_command=f'python {PROJECT_ROOT}/src/pipelines/gold/gld_live_fleet.py',
        env=env_config
    )

    stream_orders = BashOperator(
        task_id='service_stream_orders',
        bash_command=f'python {PROJECT_ROOT}/src/pipelines/gold/gld_live_orders.py',
        env=env_config
    )

    stream_perf = BashOperator(
        task_id='service_stream_performance',
        bash_command=f'python {PROJECT_ROOT}/src/pipelines/gold/gld_speed_layer_fact_perf.py',
        env=env_config
    )

    [sim_generator, stream_fleet, stream_orders, stream_perf]