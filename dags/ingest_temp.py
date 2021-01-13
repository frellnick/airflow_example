"""Demo Ingest to copy files in Bucket and Save CSV to appropriate BigQuery Dataset via DataFlow"""

import airflow
from airflow import DAG
from datetime import timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

from dependencies import get_file_operations


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'ingest_temp',
    default_args=default_args,
    description='Stage .csv datasets for mapping and archive.',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20)
    )


prepare = PythonOperator(
    task_id='Prepare_File_Operations',
    python_callable=get_file_operations,
    provide_context=True,
    dag=dag,
)


def branch_if_fileops(**kwargs):
    if len(kwargs['ti'].xcom_pull(key='fileops')) > 0:
        return 'totable'  # Change for file operations
    return 'join'


branching = BranchPythonOperator(
    task_id='branching',
    python_callable=branch_if_fileops,
    provide_context=True,
    dag=dag
)


join = DummyOperator(
    task_id='join',
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

totable = DummyOperator(  ## Create task for all partners + adhoc, same with copy
    task_id='totable',
    dag=dag,
)

falsetask = DummyOperator(
    task_id='falsetask',
    dag=dag,
)
prepare >> branching
branching >> totable >> join 
branching >> falsetask >> join