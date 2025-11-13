import os, sys
from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DAGS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, DAGS_FOLDER)

DBT_TARGET = 'dev'
PROJECT_ID = 'natural-events-pipeline'
GCS_RAW_DATA_PREFIX = 'raw_parquet/natural'
GCS_BUCKET = 'natural-events-staging'

default_args = {
    'owner': 'jed.lanelle',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}

@dag(
    dag_id='dag_migrate_data_duckdb_to_bigquery',
    default_args=default_args,
    description='Migrates data from DuckDB to BigQuery with GCS Bucket as intermediary',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags = ['hourly', 'gcs', 'bigquery', 'duckdb']
)
def hourly_firms_usgs_export_dag():

    check_dbt_path = BashOperator(
        task_id="check_dbt_path",
        bash_command="which dbt && dbt --version"
    )
    
    @task
    def setup_dbt_temp_directory(**context):
        import os
        import shutil

        dag_id = context['dag'].dag_id
        execution_date = context['execution_date']
        temp_root = '/opt/airflow/temp'

        temp_dir = os.path.join(
            temp_root,
            f"{dag_id}/{execution_date.strftime('%Y_%m_%d_%H_%M_%S')}"
        )

        try:
            if os.path.exists(temp_dir):
                logging.warning(f"Temp directory already exists, removing: {temp_dir}")
                shutil.rmtree(temp_dir)
            
            os.makedirs(temp_dir, exist_ok=True)

            dbt_temp_path = os.path.join(temp_dir, "dbt")

            if os.path.exists(dbt_temp_path):
                shutil.rmtree(dbt_temp_path)
            
            shutil.copytree("/opt/dbt", dbt_temp_path)

            logging.info(f"Created DBT temp directory: {dbt_temp_path}")
            return dbt_temp_path
        except Exception as e:
            logging.error(f"Failed to create temp directory: {e}")
            return
    
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command="""
            cd {{ ti.xcom_pull(task_ids='setup_dbt_temp_directory') }}
            dbt run \
            --profiles-dir {{ ti.xcom_pull(task_ids='setup_dbt_temp_directory') }} \
            --target dev \
            --select +path:models/marts \
        """,
        env = {
            "PATH": os.path.expanduser("~/.local/bin") + ":" + os.environ["PATH"]
        }
    )

    test_dbt_models = BashOperator(
        task_id='test_dbt_models',
        bash_command="""
            cd {{ ti.xcom_pull(task_ids='setup_dbt_temp_directory') }}
            dbt test \
            --select +path:models/marts \
            --target dev \
        """,
        env = {
            "PATH": os.path.expanduser("~/.local/bin") + ":" + os.environ["PATH"]
        },
        trigger_rule="all_success"
    )

    @task
    def cleanup_dbt_temp_directory(temp_path: str):
        import os
        import shutil

        try:
            if os.path.exists(temp_path):
                shutil.rmtree(temp_path)
                logging.info(f"Removed temp directory: {temp_path}")
        except Exception as e:
            logging.warning(f"Couldn't remove the temp directory: {e}")
            return
    
    run_export_script = BashOperator(
        task_id="run_export_script",
        bash_command = "python3 /opt/scripts/export_to_bq.py",
        env={
            "PATH": os.path.expanduser("~/.local/bin") + ":" + os.environ["PATH"]
        }
    )
    
    dbt_temp_path = setup_dbt_temp_directory()
    cleanup = cleanup_dbt_temp_directory(dbt_temp_path)

    check_dbt_path >> dbt_temp_path >> run_dbt_models >> test_dbt_models >> cleanup >> run_export_script

dag = hourly_firms_usgs_export_dag()