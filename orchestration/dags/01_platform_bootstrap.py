import os
import logging
from datetime import datetime
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
SQL_SCHEMA_PATH = os.path.join(PROJECT_ROOT, "models", "gold_schema_md.sql")
SECRETS_PATH = os.path.join(PROJECT_ROOT, "secrets", "warehouse_password.txt")
CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "data", "checkpoints")

def get_db_password():
    if os.path.exists(SECRETS_PATH):
        with open(SECRETS_PATH, "r") as f:
            return f.read().strip()
    return os.getenv("DB_PASSWORD", "warehouse_password")

def init_postgres_schema():
    db_host = os.getenv("DB_HOST", "pizza_warehouse_db")
    db_name = os.getenv("DB_NAME", "data_warehouse")
    db_user = os.getenv("DB_USER", "warehouse_admin")
    db_password = get_db_password()

    if not os.path.exists(SQL_SCHEMA_PATH):
        raise FileNotFoundError(f"Schema file not found: {SQL_SCHEMA_PATH}")

    with open(SQL_SCHEMA_PATH, "r") as f:
        sql_script = f.read()

    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=5432
        )
        conn.autocommit = True
        
        with conn.cursor() as cur:
            cur.execute(sql_script)
            
    except Exception as e:
        raise e
    finally:
        if conn:
            conn.close()

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='01_platform_bootstrap',
    default_args=default_args,
    description='INIT: Setup Postgres Schema & Clean Environment',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['setup', 'postgres'],
) as dag:

    clean_checkpoints = BashOperator(
        task_id='clean_stale_checkpoints',
        bash_command=f'rm -rf {CHECKPOINT_DIR}/* || true',
    )

    init_db = PythonOperator(
        task_id='init_postgres_schema',
        python_callable=init_postgres_schema
    )

    clean_checkpoints >> init_db