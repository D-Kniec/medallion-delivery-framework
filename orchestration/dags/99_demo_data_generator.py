from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'pizza_admin',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    '99_demo_data_generator',
    default_args=default_args,
    description='UTILITY: Generates Mock Data & Triggers ETL',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['demo', 'test']
) as dag:

    generate_traffic = BashOperator(
        task_id='simulate_traffic_60s',
        bash_command='timeout 60s python /opt/airflow/src/generator/gen_telemetry.py || true',
        cwd='/opt/airflow'
    )

    trigger_etl = TriggerDagRunOperator(
        task_id='trigger_daily_etl',
        trigger_dag_id='01_pizza_daily_etl',
        wait_for_completion=True
    )

    generate_traffic >> trigger_etl