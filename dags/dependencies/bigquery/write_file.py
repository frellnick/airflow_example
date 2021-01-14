"""
Load

Load methods to get data into bigquery
"""

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable

from ..log import get_logger

logger = get_logger(__name__)


def files_to_bigquery(*args, **kwargs):
    fileops = kwargs['ti'].xcom_pull(key='fileops')
    logger.info(f'{fileops}')
    for op in fileops:
        write_file(op, **kwargs)
    

def write_file(fileop:dict, **kwargs):
    op = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=Variable.get('bucket_tmp1'),
        source_objects=[fileop['filename']],
        destination_project_dataset_table=f"{Variable.get('bq_project')}.{fileop['partner']}.{fileop['tablename']}",
        schema_fields=fileop['schema'],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )
    try:
        op.execute(context=kwargs)
        logger.info(f"BigQuery write executed on: {fileop['tablename']}")
    except Exception as e:
        logger.error(f'{e}')
        raise e
