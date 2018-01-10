from airflow import DAG
from airflow.operators import BashOperator


def dim_subdag(parent_dag, child_dag, start_date, schedule_interval):

    dim_slug = '{}_dim'.format(child_dag)
    
    dag = DAG(
        '{}.{}'.format(parent_dag, child_dag),
        start_date=start_date,
        schedule_interval=schedule_interval
    )

    commit_table_template = """{{ var.value.CCHQ_HOME }}/python_env/bin/python {{ var.value.CCHQ_HOME }}/manage.py commit_table {{ params.table_slug }} {{ ti.xcom_pull("start_batch") }}"""

    start_batch = BashOperator(
        task_id='start_batch',
        bash_command="{{ var.value.CCHQ_HOME }}/python_env/bin/python {{ var.value.CCHQ_HOME }}/manage.py create_batch {{ params.dim_slug }}",
        params={'dim_slug': dim_slug}
        dag=dag,
        xcom_push=True
    )


    update_staging = BashOperator(
        task_id='update_{}_staging'.format(child_dag),
        bash_command=commit_table_template,
        params={'table_slug': 'application_staging'},
        dag=dag
    )

    update_dim = BashOperator(
        task_id='load_{}_dim'.format(child_dag),
        bash_command=commit_table_template,
        params={'table_slug': 'application_dim'},
        dag=dag
    )

    complete_batch = BashOperator(
        task_id='complete_batch',
        bash_command="{{ var.value.CCHQ_HOME }}/python_env/bin/python {{ var.value.CCHQ_HOME }}/manage.py mark_batch_complete {{ ti.xcom_pull('start_batch') }}",
        dag=dag
    )

    start_batch >> update_staging >> update_dim >> complete_batch
