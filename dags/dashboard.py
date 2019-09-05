from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

from dashboard_subdags import parallel_subdag, monthly_subdag, run_query_template

default_args = {
    'owner': 'cchq',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 1),
    'email': ['{}@{}'.format(name, 'dimagi.com') for name in ('dashboard-aggregation-script',)],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}

DASHBOARD_DAG_ID = 'dashboard_aggregation'

dashboard_dag = DAG(DASHBOARD_DAG_ID, default_args=default_args, schedule_interval='0 18 * * *')

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dashboard_dag, depends_on_past=True)

setup_aggregation = BashOperator(
    task_id='setup_aggregation',
    bash_command=run_query_template,
    params={'query': 'setup_aggregation'},
    dag=dashboard_dag
)

prev_month = SubDagOperator(
    subdag=monthly_subdag(
        DASHBOARD_DAG_ID,
        'prev_month',
        dashboard_dag.default_args,
        dashboard_dag.schedule_interval,
        interval=-1
    ),
    task_id='prev_month',
    dag=dashboard_dag
)

current_month = SubDagOperator(
    subdag=monthly_subdag(
        DASHBOARD_DAG_ID,
        'current_month',
        dashboard_dag.default_args,
        dashboard_dag.schedule_interval,
        interval=0
    ),
    task_id='current_month',
    dag=dashboard_dag
)

success_email = EmailOperator(
    task_id='success_email',
    to='{}@{}'.format('dashboard-aggregation-script', 'dimagi.com'),
    subject='Aggregation Complete',
    html_content="""Aggregation has completed for {{ ds }} """,
    dag=dashboard_dag
)

latest_only >> setup_aggregation >> prev_month >> current_month >> success_email
