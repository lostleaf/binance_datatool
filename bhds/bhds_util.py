import os
from datetime import date

from dateutil import parser as date_parser


def mp_env_init():
    os.environ['POLARS_MAX_THREADS'] = '1'
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['MKL_NUM_THREADS'] = '1'
    os.environ['OPENBLAS_NUM_THREADS'] = '1'
    os.environ['NUMEXPR_NUM_THREADS'] = '1'
    os.environ['NUMBA_NUM_THREADS'] = '1'
    os.environ['VECLIB_MAXIMUM_THREADS'] = '1'


def convert_date(dt) -> date:
    if isinstance(dt, str):
        return date_parser.parse(dt).date()
    return dt
