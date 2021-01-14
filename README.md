# airflow_example
Apache Airflow DAG repository example


## Requirements
* Linux OS - Ubuntu 20.04 or more recent (mac OS minimally supported.  Windows installation not supported outside of containers.)
* airflow 1.10.12+
* pytest
* pandas
* numpy
* gcsfs

## Tests
Tests are written with pytest module.  Modules may not test outside of cloud emulated environment.

Run tests with:

```python
pytest
```

## Environment Maintenance
Create/Delete Composer environment from the CLI

### Create this composer environment
...

#### Environment Variables
* bucket_tmp1: <BUCKET_NAME_TEMPORARY_STORAGE(INGEST)>
* bucket_lts1: <BUCKET_NAME_LONG_TERM_STORAGE(ARCHIVE)>
* bq_project: <BIGQUERY_PROJECT_NAME>

### Delete this composer environment
...


## CI/CD
Repository (GIT_REPO_LINK) is mirrored to GCS Bucket via Cloud Build...

