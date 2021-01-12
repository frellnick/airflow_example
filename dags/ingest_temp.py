"""Demo Ingest to copy files in Bucket and Save CSV to appropriate BigQuery Dataset via DataFlow"""

import airflow
from airflow import DAG
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ingest_temp',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20)
    )


