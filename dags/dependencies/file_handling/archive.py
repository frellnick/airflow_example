
"""
Archive

Copy files per instruction from staging bucket to long term storage.
"""
from airflow.models import Variable

from ..operators import GoogleCloudStorageToGoogleCloudStorageOperator
from ..log import get_logger


copy_log = get_logger('copy')


def copy_file(fileop:dict, **kwargs):
    operator = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id='copy_single_file',
        source_bucket=Variable.get('bucket_tmp1'),
        source_object=fileop['filename'],
        destination_bucket=Variable.get('bucket_lts1'),
        destination_object=fileop['destination_filename'],
        move_object=True,
        google_cloud_storage_conn_id='google_cloud_storage_default',
    )

    try:
        operator.execute(kwargs)
        copy_log.info(f"Copy Successful:\n{fileop}")
    except Exception as e:
        copy_log.error(f"Copy failed:\n{fileop}")
        ## Non blocking.  raise e if this step should error out the workflow.


def copy_files(*args, **kwargs):
    fileops = kwargs['ti'].xcom_pull(key='fileops')
    for f in fileops:
        copy_file(f, **kwargs)