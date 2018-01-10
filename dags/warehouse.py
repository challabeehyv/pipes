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

update_user_dim = SubDagOperator(
    subdag=dim_subdag(DAG_ID, 'user', dag.start_date, dag.schedule_interval),
    task_id='update_user_dim',
    dag=dag
)

update_location_dim = SubDagOperator(
    subdag=dim_subdag(DAG_ID, 'location', dag.start_date, dag.schedule_interval),
    task_id='update_location_dim',
    dag=dag
)

update_group_dim = SubDagOperator(
    subdag=dim_subdag(DAG_ID, 'group', dag.start_date, dag.schedule_interval),
    task_id='update_group_dim',
    dag=dag
)

update_domain_dim = SubDagOperator(
    subdag=dim_subdag(DAG_ID, 'domain', dag.start_date, dag.schedule_interval),
    task_id='update_domain_dim',
    dag=dag
)
