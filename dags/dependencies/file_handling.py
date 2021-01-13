# file_inference.py

import time
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorage

from airflow.models import Variable

from .partners import get_partner


"""
Staging Operations

Create file operation profiles for downstream steps.
Check staging bucket and build standard names, GUID, etc. Pass as jobs to 
local jobs server via xcom_push().
"""
def define_table_name(filename:str) -> str:
    def _gen_guid():
        return hash(time.time())

    def _strip_end(name:str) -> str:
        return ''.join(name.split('.')[0:-1])

    return _strip_end(filename) + f'_{_gen_guid()}'


def define_file_operation(filename: str) -> dict:
    return {
        'filename': filename,
        'partner': get_partner(filename),
        'table_name': define_table_name(filename),
    }


def get_file_operations(*args, **kwargs):
    getter = GoogleCloudStorageListOperator(
        task_id = 'GCS_List_Staged_Files',
        bucket=Variable.get('bucket_tmp1'),
        delimiter='.csv',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        depends_on_past=False,
    )
    files = getter.execute(kwargs)

    payload = []
    for f in files:
        payload.append(define_file_operation(f))

    kwargs['ti'].xcom_push(key='fileops', value=payload)
    return payload

"""
File Copy

Copy files per instruction from staging bucket to long term storage.
"""
def copy_file(fileop:dict, **kwargs):
    pass


def copy_files(*args, **kwargs):
    fileops = kwargs['ti'].xcom_pull(key='fileops')
    for f in fileops:
        copy_file(f, **kwargs)