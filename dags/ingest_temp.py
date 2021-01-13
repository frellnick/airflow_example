"""Demo Ingest to copy files in Bucket and Save CSV to appropriate BigQuery Dataset via DataFlow"""

import airflow
from airflow import DAG
from datetime import timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

from dependencies.file_handling.staging import get_file_operations
from dependencies.file_handling.archive import copy_files 
from dependencies.file_handling.bigquery import files_to_bigquery


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['udrc@utah.gov'],
    'email_on_failure': False,  # Set to True in production
    'email_on_retry': False,  # Set to True in production
    # 'on_failure_callback': SomeFunction,
    # 'on_success_callback': SomeFunction,  ## Notify User
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
        return 'files_to_bigquery'
    return 'falsetask'


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


filestobigquery = PythonOperator(
    task_id='tobigquery',
    python_callable=files_to_bigquery,
    provide_context=True,
    dag=dag,
)


copyfiles = PythonOperator(
    task_id='copyfiles',
    python_callable=copy_files,
    provide_context=True,
    dag=dag,
)


falsetask = DummyOperator(
    task_id='falsetask',
    dag=dag,
)


prepare >> branching
branching >> files_to_bigquery >> copyfiles >> join 
branching >> falsetask