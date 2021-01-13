# test_file_profile.py


import pytest 
import pandas as pd

from ..file_handling.profile import Profiler

@pytest.fixture
def frame():
    return pd.DataFrame(
        {
            'a':[0,1,2],
            'b':['a','b','c'],
            'c':['2020/01/11', '2019/01/11', '2018/01/11'],
            'd':['1/1/99', '1/1/98', '1/1/97'],
            'e':['1/1/1988', '1/1/1986', '1/1/1985']
            })


def test_create_profiler():
    plr = Profiler()


def test_profile_frame(frame):
    plr = Profiler()
    prf = plr.profile(frame)
    assert prf['a'] == 'NUMERIC'
    assert prf['b'] == 'STRING'
    assert prf['c'] == 'DATE'