import os


def mp_env_init():
    os.environ['POLARS_MAX_THREADS'] = '1'
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['MKL_NUM_THREADS'] = '1'
    os.environ['OPENBLAS_NUM_THREADS'] = '1'
    os.environ['NUMEXPR_NUM_THREADS'] = '1'
    os.environ['NUMBA_NUM_THREADS'] = '1'
    os.environ['VECLIB_MAXIMUM_THREADS'] = '1'