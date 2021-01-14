"""
Profile

Profile .csv or dataframe object and return schema for loading into BigQuery
"""

import pandas as pd
import numpy as np
from dateutil.parser import parse


### Profiler Class ###
# Profiler
# Infer data types from file and translate to endpoint system schema

class Profiler():
    def __init__(self, endpoint='bq'):
        self.translate = pandas_to_bigquery_translation

    def profile(self, data: pd.DataFrame):
        dtypes = _get_dtypes(data)
        return self.translate(dtypes, data)


## Utility Function

def get_schema(data):
    plr = Profiler(endpoint='bq')
    schema = plr.profile(data)
    return _format_schema_dict(schema)

def _format_schema_dict(schema_dict, default_mode='NULLABLE'):
    fs = []
    for col in schema_dict:
        fs.append(
            {
                'name': col, 
                'type': schema_dict[col], 
                'mode': default_mode})
    return fs

## Profiler Helper Functions

def _is_date(col, data):
    d = data[col][0]
    try:
        parse(d)
        return True 
    except:
        return False


def _get_base_type(description: pd.Series, data) -> dict:
    if np.issubdtype(description.dtype, np.number):
        return 'numeric'
    elif _is_date(description.name, data):
        return 'date'
    else:
        return 'text'


def _get_description(series: pd.Series) -> pd.Series:
    return series.describe()


def _get_descriptions(dataframe: pd.DataFrame) -> list:
    return list(map(
        _get_description,
        [dataframe[col] for col in dataframe.columns]))


def _get_dtypes(data: pd.DataFrame) -> dict:
    descriptions = _get_descriptions(data)
    dtypes = {}
    for d in descriptions:
        dtypes[d.name] = _get_base_type(d, data)
    return dtypes


def pandas_to_bigquery_translation(dtypes: dict, data: pd.DataFrame) -> dict:
    lookup = {
        'numeric': 'NUMERIC',
        'text': 'STRING',
        'date': 'DATE',
    }

    for d in dtypes:
        dtypes[d] = lookup[dtypes[d]]
    return dtypes