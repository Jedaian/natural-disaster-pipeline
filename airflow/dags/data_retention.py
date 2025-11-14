import os, sys
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.decorators import dag

DAGS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, DAGS_FOLDER)

default_args = {
    'owner': 'jed.lanelle',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='dag_data_retention_cleanup',
    default_args=default_args,
    description='Daily scans for retention policy',
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['daily', 'retention', 'cleanup', 'parquet']
)
def data_retention_cleanup():
    
    cleanup_partitions = BashOperator(
        task_id='cleanup_partitions',
        bash_command="python3 /opt/scripts/cleanup_partitions.py",
        env={
            "PATH": os.path.expanduser("~/.local/bin") + ":" + os.environ["PATH"]
        }
    )

dag = data_retention_cleanup()