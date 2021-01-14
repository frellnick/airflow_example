# load.py

import pandas as pd

def read_head(filename, bucket, nrows=5, **kwargs):
    path = f'gs://{bucket}/{filename}'
    return pd.read_csv(path, low_memory=False, nrows=nrows)