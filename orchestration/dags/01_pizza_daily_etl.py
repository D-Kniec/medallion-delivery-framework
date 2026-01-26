import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
SRC_DIR = os.path.join(AIRFLOW_HOME, 'src')
JAR_PATH = os.path.join(AIRFLOW_HOME, 'jars', 'postgresql-42.7.2.jar')

SPARK_CONF = {
    'spark.master': 'local[*]',
    'spark.driver.extraClassPath': JAR_PATH,
    'spark.executor.extraClassPath': JAR_PATH
}

default_args = {
    'owner': 'pizza_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '01_pizza_daily_etl',
    default_args=default_args,
    description='PRODUCTION: Main ETL Pipeline',
    schedule_interval='0 3 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'core', 'batch']
) as dag:

    def spark_task(task_id, script_subpath):
        return SparkSubmitOperator(
            task_id=task_id,
            application=os.path.join(SRC_DIR, script_subpath),
            conn_id='spark_default',
            jars=JAR_PATH,
            driver_class_path=JAR_PATH,
            conf=SPARK_CONF
        )

    ingest_silver = spark_task('ingest_bronze_to_silver', 'silver/slv_btch_orders.py')
    
    update_dims = spark_task('update_dimensions', 'gold/gld_btch_dims_load.py')
    
    calculate_kpi = spark_task('calculate_daily_kpi', 'gold/gld_fact_daily_kpi.py')
    
    maintenance = spark_task('maintenance_compaction', 'silver/maintenance_compaction.py')

    ingest_silver >> update_dims >> calculate_kpi >> maintenance