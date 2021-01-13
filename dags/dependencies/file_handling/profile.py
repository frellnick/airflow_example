"""
Profile

Profile .csv or dataframe object and return schema for loading into BigQuery
"""

import pandas as pd
import numpy as np


def get_schema(filename, **kwargs):
    data = read_from_bucket(filename, **kwargs)
    schema = profile(data)
    return schema


def read_from_bucket(filename, **kwargs):
    pass


def profile(data):
    plr = Profiler(endpoint='bq')
    return plr.profile(data)



### Profiler Class ###
# Profiler
# Infer data types from file and translate to endpoint system schema

translators = {
    'bq': pandas_to_bigquery_translation,
}

class Profiler():
    def __init__(self, endpoint):
        self.translate = translators[endpoint]

    def _get_dtypes(self, data: pd.DataFrame) -> dict:
        if np.issubdtype(profile.dtype, np.number):
            return 'numeric'
        else:
            return 'text'


    def _get_description(series: pd.Series) -> pd.Series:
        return series.describe()


    def _get_descriptions(dataframe: pd.DataFrame) -> list:
        return list(map(
            _get_description,
            [dataframe[col] for col in dataframe.columns]))


    def profile(self, data: pd.DataFrame):
        dtypes = self._get_dtypes(data)
        return self.translate(dtypes, self.data)


def pandas_to_bigquery_translation(dtypes: dict, data: pd.DataFrame) -> dict:
    def _numeric(data):
        return 'NUMERIC'

    def _text(data):
        if _is_date(data):
            return 'DATE'
        else:
            return 'STRING'

    fn_lookup = {
        'numeric': _numeric,
        'text': _text,
    }

    def lookup(d:str, data: pd.DataFrame, fn_lookup=fn_lookup)->str:
        return pd_to_bq_functions[d](data)

    for d in dtypes:
        dtypes[d] = lookup(d, data: pd.DataFrame)
    return d