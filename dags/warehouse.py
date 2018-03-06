from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from dim import dim_subdag, multi_subdag, fact_subdag

default_args = {
    'owner': 'cchq',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 14),
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

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag, depends_on_past=True)

update_app_dim = SubDagOperator(
    subdag=dim_subdag(DAG_ID, 'application', dag.default_args, dag.schedule_interval),
    task_id='application',
    dag=dag
)

update_user_dims = SubDagOperator(
    subdag=multi_subdag(DAG_ID, 'user', dag.default_args, dag.schedule_interval, ['group', 'user', 'location', 'domain'], ['user_group', 'user_location', 'domain_membership'], 'dim'),
    task_id='user',
    dag=dag
)

update_app_status = SubDagOperator(
    subdag=multi_subdag(DAG_ID, 'app_status', dag.default_args, dag.schedule_interval, ['form', 'synclog'], ['app_status'], 'fact'),
    task_id='app_status',
    dag=dag
)

latest_only >> update_app_dim
latest_only >> update_user_dims
latest_only >> update_app_status

update_user_dims >> update_app_status
