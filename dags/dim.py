from airflow import DAG
from airflow.operators import BashOperator


commit_table_template = """{{ var.value.CCHQ_HOME }}/python_env-3.6/bin/python {{ var.value.CCHQ_HOME }}/manage.py commit_table {{ params.table_slug }} {{ ti.xcom_pull(params.start_id) }}"""


def linear_subdag(parent_dag, child_dag, default_args, schedule_interval, final_type):

    final_slug = '{}_{}'.format(child_dag, final_type)
    staging_slug = '{}_staging'.format(child_dag)
    start_id = 'start_{}_batch'.format(child_dag)

    dag = DAG(
        '{}.{}'.format(parent_dag, child_dag),
        default_args=default_args,
        schedule_interval=schedule_interval
    )

    start_batch = BashOperator(
        task_id=start_id,
        bash_command="{{ var.value.CCHQ_HOME }}/python_env-3.6/bin/python {{ var.value.CCHQ_HOME }}/manage.py create_batch {{ params.table_slug }} '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S') }}'",
        params={'table_slug': final_slug},
        dag=dag,
        xcom_push=True
    )


    update_staging = BashOperator(
        task_id='update_{}_staging'.format(child_dag),
        bash_command=commit_table_template,
        params={'table_slug': staging_slug, 'start_id': start_id},
        dag=dag
    )

    update_dim = BashOperator(
        task_id='load_{}_{}'.format(child_dag, final_type),
        bash_command=commit_table_template,
        params={'table_slug': final_slug, 'start_id': start_id},
        dag=dag
    )

    complete_batch = BashOperator(
        task_id='complete_batch',
        bash_command="{{ var.value.CCHQ_HOME }}/python_env-3.6/bin/python {{ var.value.CCHQ_HOME }}/manage.py mark_batch_complete {{ ti.xcom_pull(params.start_id) }}",
        params={'start_id': start_id},
        dag=dag
    )

    start_batch >> update_staging >> update_dim >> complete_batch

    return dag


def dim_subdag(parent_dag, child_dag, default_args, schedule_interval):
    return linear_subdag(parent_dag, child_dag, default_args, schedule_interval, 'dim')


def fact_subdag(parent_dag, child_dag, default_args, schedule_interval):
    return linear_subdag(parent_dag, child_dag, default_args, schedule_interval, 'fact')


def multi_subdag(parent_dag, child_dag, default_args, schedule_interval, dim_dependencies, final_dims, final_type, extra_staging=None):

    start_id = 'start_{}_batch'.format(child_dag)
    batch_slug = '{}_batch'.format(child_dag)

    dag = DAG(
        '{}.{}'.format(parent_dag, child_dag),
        default_args=default_args,
        schedule_interval=schedule_interval
    )

    start_batch = BashOperator(
        task_id=start_id,
        bash_command="{{ var.value.CCHQ_HOME }}/python_env-3.6/bin/python {{ var.value.CCHQ_HOME }}/manage.py create_batch {{ params.batch_slug }} '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S') }}'",
        params={'batch_slug': batch_slug},
        dag=dag,
        xcom_push=True
    )

    dims = []
    for dim in dim_dependencies:
        dep_dim_slug = '{}_{}'.format(dim, final_type)
        dep_staging_slug = '{}_staging'.format(dim)

        update_staging = BashOperator(
            task_id='update_{}_staging'.format(dim),
            bash_command=commit_table_template,
            params={'table_slug': dep_staging_slug, 'start_id': start_id},
            dag=dag
        )
        update_staging.set_upstream(start_batch)

        update_dim = BashOperator(
            task_id='load_{}_{}'.format(dim, final_type),
            bash_command=commit_table_template,
            params={'table_slug': dep_dim_slug, 'start_id': start_id},
            dag=dag
        )
        dims.append(update_dim)
        update_dim.set_upstream(update_staging)

    staging = []
    if extra_staging is not None:
        for staging_table in extra_staging:
            staging_slug = '{}_staging'.format(staging_table)
            update_staging = BashOperator(
                task_id='update_{}_staging'.format(staging_table),
                bash_command=commit_table_template,
                params={'table_slug': staging_slug, 'start_id': start_id},
                dag=dag
            )
            staging.append(update_staging)
            for dim in dims:
                update_staging.set_upstream(dim)

    multi_dims = []
    for dim in final_dims:
        dim_slug = '{}_{}'.format(dim, final_type)
        update_multi_dim = BashOperator(
            task_id='load_{}_{}'.format(dim, final_type),
            bash_command=commit_table_template,
            params={'table_slug': dim_slug, 'start_id': start_id},
            dag=dag
        )
        multi_dims.append(update_multi_dim)
        for table in staging:
            update_multi_dim.set_upstream(table)
        for dim in dims:
            update_multi_dim.set_upstream(dim)

    complete_batch = BashOperator(
        task_id='complete_batch',
        bash_command="{{ var.value.CCHQ_HOME }}/python_env-3.6/bin/python {{ var.value.CCHQ_HOME }}/manage.py mark_batch_complete {{ ti.xcom_pull(params.start_id) }}",
        params={'start_id': start_id},
        dag=dag
    )

    for dim in multi_dims:
        complete_batch.set_upstream(dim)

    return dag
