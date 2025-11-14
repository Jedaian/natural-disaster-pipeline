import os, sys
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.decorators import dag

DAGS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, DAGS_FOLDER)

default_args = {
    'owner': 'jed.lanelle',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}
@dag(
    dag_id='dag_create_mv_and_views_once',
    default_args=default_args,
    description='A one time DAG to create the needed materialized views and views for BigQuery',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags = ['once', 'bigquery', 'script', 'sql']
)
def once_bigquery_mv_v_creation():

    create_materialized_views = BashOperator(
            task_id='create_materialized_views',
            bash_command="python3 /opt/scripts/create_mv.py",
            env={
                "PATH": os.path.expanduser("~/.local/bin") + ":" + os.environ["PATH"]
            }
        )

dag = once_bigquery_mv_v_creation()