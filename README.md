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

## CI/CD
Repository (GIT_REPO_LINK) is mirrored to GCS Bucket via Cloud Build...
