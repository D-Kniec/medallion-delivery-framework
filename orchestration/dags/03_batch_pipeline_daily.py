import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
BRONZE_ORDERS_PATH = os.path.join(PROJECT_ROOT, "data/bronze/orders")

def _check_for_data_presence():
    if not os.path.exists(BRONZE_ORDERS_PATH):
        return False
    files = [f for f in os.listdir(BRONZE_ORDERS_PATH) if not f.startswith('.')]
    return len(files) > 0

default_args = {
    'owner': 'batch_layer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='03_batch_pipeline_daily',
    default_args=default_args,
    description='BATCH: Smart ETL Pipeline with Python Sensor',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['batch', 'etl', 'postgres'],
) as dag:

    env_config = {
        **os.environ,
        "PYTHONPATH": PROJECT_ROOT,
        "DB_HOST": "pizza_warehouse_db",
        "DB_PORT": "5432"
    }

    wait_for_data = PythonSensor(
        task_id='wait_for_bronze_orders',
        python_callable=_check_for_data_presence,
        poke_interval=30,  
        timeout=600,      
        mode='poke'
    )

    compaction = BashOperator(
        task_id='silver_maintenance_compaction',
        bash_command=f'python {PROJECT_ROOT}/src/pipelines/silver/maintenance_compaction.py',
        env=env_config
    )

    with TaskGroup("silver_processing") as silver_group:
        t_dims = BashOperator(
            task_id='silver_dims',
            bash_command=f'python {PROJECT_ROOT}/src/pipelines/silver/slv_btch_dims_load.py',
            env=env_config
        )
        t_orders = BashOperator(
            task_id='silver_orders',
            bash_command=f'python {PROJECT_ROOT}/src/pipelines/silver/slv_btch_orders.py',
            env=env_config
        )
        t_telemetry = BashOperator(
            task_id='silver_telemetry',
            bash_command=f'python {PROJECT_ROOT}/src/pipelines/silver/slv_btch_telemetry.py',
            env=env_config
        )
        [t_dims, t_orders, t_telemetry]

    gold_dims = BashOperator(
        task_id='gold_dims_scd',
        bash_command=f'python {PROJECT_ROOT}/src/pipelines/gold/gld_btch_dims_load.py',
        env=env_config
    )

    gold_kpi = BashOperator(
        task_id='gold_fact_kpi',
        bash_command=f'python {PROJECT_ROOT}/src/pipelines/gold/gld_fact_daily_kpi.py --process_date {{{{ ds }}}}',
        env=env_config
    )

    wait_for_data >> compaction >> silver_group >> gold_dims >> gold_kpi