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
    'email': ['{}@{}'.format(name, 'dimagi.com') for name in ('cellowitz', 'mharrison')],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

WAREHOUSE_DAG_ID = 'update_warehouse'

dag = DAG(WAREHOUSE_DAG_ID, default_args=default_args, schedule_interval='30 18 * * *')

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag, depends_on_past=True)

update_app_dim = SubDagOperator(
    subdag=dim_subdag(WAREHOUSE_DAG_ID, 'application', dag.default_args, dag.schedule_interval),
    task_id='application',
    dag=dag
)

update_user_dims = SubDagOperator(
    subdag=multi_subdag(WAREHOUSE_DAG_ID, 'user', dag.default_args, dag.schedule_interval, ['group', 'user', 'location', 'domain'], ['user_group', 'user_location', 'domain_membership'], 'dim'),
    task_id='user',
    dag=dag
)

update_synclog_fact = SubDagOperator(
    subdag=fact_subdag(WAREHOUSE_DAG_ID, 'synclog', dag.default_args, dag.schedule_interval),
    task_id='synclog',
    dag=dag
)

update_form_fact = SubDagOperator(
    subdag=fact_subdag(WAREHOUSE_DAG_ID, 'form', dag.default_args, dag.schedule_interval),
    task_id='form',
    dag=dag
)

update_app_status = SubDagOperator(
    subdag=multi_subdag(WAREHOUSE_DAG_ID, 'app_status', dag.default_args, dag.schedule_interval, [], ['app_status'], 'fact', extra_staging=['app_status_form', 'app_status_synclog']),
    task_id='app_status',
    dag=dag
)

latest_only >> update_app_dim
latest_only >> update_user_dims
latest_only >> update_app_status
latest_only >> update_synclog_fact
latest_only >> update_form_fact

update_user_dims >> update_synclog_fact
update_user_dims >> update_form_fact

update_app_dim >> update_app_status
update_synclog_fact >> update_app_status
update_form_fact >> update_app_status
