from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from dim import dim_subdag, multi_subdag, fact_subdag

default_args = {
    'owner': 'cchq',
    'depends_on_past': True,
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

dag = DAG(DAG_ID, catchup=False, default_args=default_args, schedule_interval='@daily')

update_app_dim = SubDagOperator(
    subdag=dim_subdag(DAG_ID, 'application', dag.default_args, dag.schedule_interval),
    task_id='application',
    dag=dag
)

update_domain_dim = SubDagOperator(
    subdag=dim_subdag(DAG_ID, 'domain', dag.default_args, dag.schedule_interval),
    task_id='domain',
    dag=dag
)

update_form_fact = SubDagOperator(
    subdag=fact_subdag(DAG_ID, 'form', dag.default_args, dag.schedule_interval),
    task_id='form',
    dag=dag
)

update_user_group_dim = SubDagOperator(
    subdag=multi_subdag(DAG_ID, 'user_group', dag.default_args, dag.schedule_interval, ['group', 'user']),
    task_id='user_group',
    dag=dag
)

update_user_group_dim >> update_form_fact
update_domain_dim >> update_form_fact
