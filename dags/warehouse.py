from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


REPORT_SLUGS = [
    'app_status',
    'forms_by_submission',
]

default_args = {
    'owner': 'cchq',
    'depends_on_past': False,
    'start_date': datetime(2016, 05, 16),
    'email': ['devops@dimagi.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': timedelta(seconds=1)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('update_fact', default_args=default_args)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='dump_intermediate_table',
    bash_command='echo dump',  # would actually be something like `./manage.py dump_intermediate_table <report_slug>`
    dag=dag
)

t2 = BashOperator(
    task_id='update_dims',
    bash_command='echo update_dims',
    dag=dag
)

t3 = BashOperator(
    task_id='update_fact_table',
    bash_command='echo update_fact_table',
    dag=dag
)

t2.set_upstream(t1)
t3.set_upstream(t2)
