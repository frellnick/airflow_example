# airflow_example
Apache Airflow DAG repository example


## Requirements

### Operating System Requirements
* Linux OS - Ubuntu 20.04 or more recent (mac OS minimally supported.  Windows installation not supported outside of containers.)

### PyPy Module Requirements
* airflow 1.10.12+
* pytest
* pandas
* numpy
* gcsfs

### Asset Requirements
* Bucket (minimum 1 to test) for staging and archiving.  tmp1 can equal lts1.
* BigQuery API active in project available to Composer
* SendGrid API active

## Tests
Tests are written with pytest module.  Modules may not test outside of cloud emulated environment.

Run avaiable unittests tests with:

```python
pytest
```

### Running a DAG from CLI

GCP Composer CLI:
```bash
gcloud composer environments run COMPOSER_ENVIRONMENT_NAME
    --location LOCATION trigger_dag -- DAG_NAME
    --run_id= RUN_ID
```
* COMPOSER_ENVIRONMENT_NAME is set on environment creation.  See dashboard or build script.
* LOCATION is set on environment creation - refers to region of underlying compute engine.  See dashboard or build script.
* RUN_ID can be any number


## Environment Maintenance
Create/Delete Composer environment from the CLI

### Create this composer environment
```bash
gcloud composer environments create ENVIRONMENT_NAME
    --location=LOCATION
    --disk-size="50GB"
    --env-variables=[NAME=VALUE,…]
    --machine-type=n1-standard-2
    --zone=us-west-1a
    --node-count=3
    --python-version=3
```

Example:

```bash
gcloud composer environments create ingesttemp --location=us-west1 --disk-size="30GB" --env-variables=BUCKET_TEMP1=uswest_tmp1,BUCKET_LTS1=uswest_tmp1,BQ_PROJECT=<DEV_PROJECT_NAME> --machine-type=n1-standard-2 --zone=us-west-1a --node-count=3 --python-version=3
```

#### Environment Variables
* BUCKET_TMP1: <BUCKET_NAME_TEMPORARY_STORAGE(INGEST)>
* BUCKET_LTS1: <BUCKET_NAME_LONG_TERM_STORAGE(ARCHIVE)>
* BQ_PROJECT: <BIGQUERY_PROJECT_NAME>
* SENDGRID_MAIL_FROM: <The From: email address, such as noreply-composer@your-domain> --NOT IN USE V0.1
* SENDBRID_API_KEY: <SendGrid API key> -- NOT IN USE V0.1

** Email may be configured via external SMTP as well (gmail).  See [creating](https://cloud.google.com/composer/docs/how-to/managing/creating) documentation.
### Delete this composer environment
...


## CI/CD
Repository (GIT_REPO_LINK) is mirrored to GCS Bucket via Cloud Build...

