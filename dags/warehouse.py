from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta
from uuid import uuid4

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

dag = DAG('update_app_status', default_args=default_args, schedule_interval='@daily')

commit_table_template = """{{ var.value.CCHQ_HOME }}/python_env/bin/python {{ var.value.CCHQ_HOME }}/manage.py commit_table {{ params.table_slug }} {{ ti.xcom_pull("start_batch") }}"""

start_batch = BashOperator(
    task_id='start_batch',
    bash_command="{{ var.value.CCHQ_HOME }}/python_env/bin/python {{ var.value.CCHQ_HOME }}/manage.py create_batch app_status",
    dag=dag,
    xcom_push=True
)


update_app_staging = BashOperator(
    task_id='update_app_staging',
    bash_command=commit_table_template,
    params={'table_slug': 'application_staging'},
    dag=dag
)

update_app_dim = BashOperator(
    task_id='load_app_dim',
    bash_command=commit_table_template,
    params={'table_slug': 'application_dim'},
    dag=dag
)

complete_batch = BashOperator(
    task_id='complete_batch',
    bash_command="{{ var.value.CCHQ_HOME }}/python_env/bin/python {{ var.value.CCHQ_HOME }}/manage.py mark_batch_complete {{ ti.xcom_pull('start_batch') }}",
    dag=dag
)

start_batch >> update_app_staging >> update_app_dim
update_app_dim >> complete_batch
