from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
from uuid import uuid4

REPORT_SLUGS = [
    'app_status',
    'forms_by_submission',
]

default_args = {
    'owner': 'cchq',
    'depends_on_past': True,
    'start_date': datetime(2016, 05, 16),
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

def get_batch_id(*args, **kwargs):
    return uuid4().hex

dag = DAG('update_fact', default_args=default_args, schedule_interval='@daily')

generate_unique_id = PythonOperator(
    task_id='generate_unique_id',
    python_callable=get_batch_id,
    dag=dag
)

create_batch_template = """{{ var.value.CCHQ_HOME }}/python_env/bin/python {{ var.value.CCHQ_HOME }}/manage.py create_batch {{ ti.xcom_pull("generate_unique_id") }} -s {{ ds }} -e {{ tomorrow_ds }}"""
commit_table_template = """{{ var.value.CCHQ_HOME }}/python_env/bin/python {{ var.value.CCHQ_HOME }}/manage.py commit_table {{ params.table_slug }} {{ ti.xcom_pull("generate_unique_id") }}"""

start_batch = BashOperator(
    task_id='start_batch',
    bash_command=create_batch_template,
    dag=dag
)


update_app_staging = BashOperator(
    task_id='update_dims',
    bash_command=commit_table_template,
    params={'table_slug': 'application_staging'},
    dag=dag
)

update_app_dim = BashOperator(
    task_id='update_fact_table',
    bash_command=commit_table_template,
    params={'table_slug': 'application_dim'},
    dag=dag
)

complete_batch = BashOperator(
    task_id='complete_batch',
    bash_command="{{ var.value.CCHQ_HOME }}/python_env/bin/python {{ var.value.CCHQ_HOME }}/manage.py mark_batch_complete {{ ti.xcom_pull('generate_unique_id') }}",
    dag=dag
)

generate_unique_id >> start_batch
start_batch >> update_app_staging >> update_app_dim
update_app_dim >> complete_batch
