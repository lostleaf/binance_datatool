import os

_DEFAULT_BASE_DIR = os.path.join(os.path.expanduser('~'), 'crypto_data')
_BASE_DIR = os.getenv('CRYPTO_BASE_DIR', _DEFAULT_BASE_DIR)
_CUR_DIR = os.path.dirname(os.path.realpath(__file__))


class Config:
    BINANCE_DATA_DIR = os.path.join(_BASE_DIR, 'binance_data')
    BINANCE_QUANTCLASS_DIR = os.path.join(_BASE_DIR, 'binance_quantclass')

    N_JOBS = int(os.getenv('CRYPTO_NJOBS', os.cpu_count() - 1))
