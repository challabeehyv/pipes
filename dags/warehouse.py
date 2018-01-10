from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from dags.dim import dim_subdag

default_args = {
    'owner': 'cchq',
    'depends_on_past': False,
    'start_date': datetime(2016, 5, 16),
    'email': ['devops@dimagi.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

DAG_ID = 'update_warehouse'

dag = DAG(DAG_ID, default_args=default_args, schedule_interval='@daily')

update_app_dim = SubDagOperator(
    subdag=dim_subdag(DAG_ID, 'app', dag.start_date, dag.schedule_interval),
    task_id='update_app_dim',
    dag=dag
)
