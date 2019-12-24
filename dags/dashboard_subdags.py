import uuid

from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from airflow.operators.subdag_operator import SubDagOperator


run_query_template = """cd {{ var.value.CCHQ_HOME }}; {{ var.value.CCHQ_PY_ENV }}/bin/python {{ var.value.CCHQ_HOME }}/manage.py run_aggregation_query {{ params.query }} {{ ti.xcom_pull('get_uuid') }}"""


def parallel_subdag(parent_dag, child_dag, default_args, schedule_interval, tasks):

    subdag_query_template = """cd {{ var.value.CCHQ_HOME }}; {{ var.value.CCHQ_PY_ENV }}/bin/python {{ var.value.CCHQ_HOME }}/manage.py run_aggregation_query {{ params.query }} {{ ti.xcom_pull(dag_id=params.parent_dag_id, task_ids='get_uuid') }}"""

    parallel_dag = DAG(
        '{}.{}'.format(parent_dag, child_dag),
        default_args=default_args,
        schedule_interval=schedule_interval
    )

    for task_slug in tasks:
        run_task = BashOperator(
            task_id=task_slug,
            bash_command=subdag_query_template,
            params={'query': task_slug, 'parent_dag_id': parent_dag},
            dag=parallel_dag
        )

    return parallel_dag


def monthly_subdag(parent_dag, child_dag, default_args, schedule_interval, interval):

    def generate_uuid():
        return uuid.uuid4().hex

    MONTHLY_DAG_ID = '{}.{}'.format(parent_dag, child_dag)
    monthly_dag = DAG(
        MONTHLY_DAG_ID,
        default_args=default_args,
        schedule_interval=schedule_interval
    )

    get_agg_id = PythonOperator(
        task_id='get_uuid',
        python_callable=generate_uuid,
        dag=monthly_dag,
        xcom_push=True
    )

    create_aggregation_record = BashOperator(
        task_id='create_aggregation_record',
        bash_command="""cd {{ var.value.CCHQ_HOME }}; {{ var.value.CCHQ_PY_ENV }}/bin/python {{ var.value.CCHQ_HOME }}/manage.py create_aggregation_record {{ params.query }} {{ ti.xcom_pull('get_uuid') }} {{ tomorrow_ds }} {{ params.interval }}""",
        params={'interval': interval},
        dag=monthly_dag
    )

    daily_attendance = BashOperator(
        task_id='daily_attendance',
        bash_command=run_query_template,
        params={'query': 'daily_attendance'},
        dag=monthly_dag
    )

    update_months_table = BashOperator(
        task_id='update_months_table',
        bash_command=run_query_template,
        params={'query': 'update_months_table'},
        dag=monthly_dag
    )

    stage_1_slugs = [
        'aggregate_gm_forms',
        'aggregate_df_forms',
        'aggregate_cf_forms',
        'aggregate_ccs_cf_forms',
        'aggregate_child_health_thr_forms',
        'aggregate_ccs_record_thr_forms',
        'aggregate_child_health_pnc_forms',
        'aggregate_ccs_record_pnc_forms',
        'aggregate_delivery_forms',
        'aggregate_bp_forms',
        'aggregate_awc_infra_forms',
        'aggregate_ag_forms'
    ]

    stage_1_tasks = SubDagOperator(
        subdag=parallel_subdag(
            MONTHLY_DAG_ID,
            'stage_1_tasks',
            monthly_dag.default_args,
            monthly_dag.schedule_interval,
            stage_1_slugs
        ),
        task_id='stage_1_tasks',
        dag=monthly_dag
    )

    child_health_monthly = BashOperator(
        task_id='child_health_monthly',
        bash_command=run_query_template,
        params={'query': 'child_health_monthly'},
        dag=monthly_dag
    )

    update_child_health_monthly_table = BashOperator(
        task_id='update_child_health_monthly_table',
        bash_command=run_query_template,
        params={'query': 'update_child_health_monthly_table'},
        dag=monthly_dag
    )

    agg_child_health = BashOperator(
        task_id='agg_child_health',
        bash_command=run_query_template,
        params={'query': 'agg_child_health'},
        dag=monthly_dag
    )

    ccs_record_monthly = BashOperator(
        task_id='ccs_record_monthly',
        bash_command=run_query_template,
        params={'query': 'ccs_record_monthly'},
        dag=monthly_dag
    )

    agg_ccs_record = BashOperator(
        task_id='agg_ccs_record',
        bash_command=run_query_template,
        params={'query': 'agg_ccs_record'},
        dag=monthly_dag
    )

    agg_awc_table = BashOperator(
        task_id='agg_awc_table',
        bash_command=run_query_template,
        params={'query': 'agg_awc_table'},
        dag=monthly_dag
    )

    ls_slugs = [
        'agg_ls_awc_mgt_form',
        'agg_ls_vhnd_form',
        'agg_beneficiary_form',
    ]

    ls_tasks = SubDagOperator(
        subdag=parallel_subdag(
            MONTHLY_DAG_ID,
            'ls_tasks',
            monthly_dag.default_args,
            monthly_dag.schedule_interval,
            ls_slugs
        ),
        task_id='ls_tasks',
        dag=monthly_dag
    )

    agg_ls_table = BashOperator(
        task_id='agg_ls_table',
        bash_command=run_query_template,
        params={'query': 'agg_ls_table'},
        dag=monthly_dag
    )

    get_agg_id >> create_aggregation_record >> daily_attendance
    daily_attendance >> stage_1_tasks
    daily_attendance >> update_months_table
    stage_1_tasks >> child_health_monthly
    stage_1_tasks >> ccs_record_monthly
    update_months_table >> child_health_monthly
    update_months_table >> ccs_record_monthly
    child_health_monthly >> update_child_health_monthly_table
    child_health_monthly >> agg_child_health
    ccs_record_monthly >> agg_ccs_record
    agg_child_health >> agg_awc_table
    child_health_monthly >> agg_awc_table
    agg_ccs_record >> agg_awc_table
    agg_awc_table >> ls_tasks
    ls_tasks >> agg_ls_table

    if interval == 0:
        aggregate_awc_daily = BashOperator(
            task_id='aggregate_awc_daily',
            bash_command=run_query_template,
            params={'query': 'aggregate_awc_daily'},
            dag=monthly_dag
        )
        agg_awc_table >> aggregate_awc_daily
        create_mbt = BashOperator(
            task_id='create_mbt_for_month',
            bash_command=run_query_template,
            params={'query': 'create_mbt_for_month'},
            dag=monthly_dag
        )
        aggregate_awc_daily >> create_mbt
        update_child_health_monthly_table >> create_mbt
        agg_ls_table >> create_mbt

    return monthly_dag
