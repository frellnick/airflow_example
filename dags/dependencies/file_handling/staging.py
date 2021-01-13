"""
Staging Operations

Create file operation profiles for downstream steps.
Check staging bucket and build standard names, GUID, etc. Pass as jobs to 
local jobs server via xcom_push().
"""

import time
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.models import Variable

from .partners import get_partner
from .profile import get_schema
from .load import load_from_bucket



def define_table_name(filename:str) -> str:
    def _gen_guid():
        return hash(time.time())

    def _strip_end(name:str) -> str:
        return ''.join(name.split('.')[0:-1])

    return _strip_end(filename) + f'_{_gen_guid()}'


def define_file_operation(filename: str, **kwargs) -> dict:
    partner = get_partner(filename)
    table_name = define_table_name(filename)
    extension = filename.split('.')[-1]
    destination_filename = f"{partner}/{table_name}.{extension}"
    return {
        'filename': filename,
        'destination_filename': destination_filename,
        'partner': get_partner(filename),
        'table_name': define_table_name(filename),
        'schema': get_schema(
            load_from_bucket(filename, Variable.get('bucket_tmp1'), **kwargs)
            )
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
        payload.append(define_file_operation(f, **kwargs))

    kwargs['ti'].xcom_push(key='fileops', value=payload)
    return payload
