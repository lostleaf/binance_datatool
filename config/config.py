import os

_DEFAULT_BASE_DIR = os.path.join(os.path.expanduser('~'), 'crypto_data')
_BASE_DIR = os.getenv('CRYPTO_BASE_DIR', _DEFAULT_BASE_DIR)
_CUR_DIR = os.path.dirname(os.path.realpath(__file__))


class CFG:
    BINANCE_DATA_DIR = os.path.join(_BASE_DIR, 'binance_data')
    BINANCE_QUANTCLASS_DIR = os.path.join(_BASE_DIR, 'binance_quantclass')
    BHDS_EXTRA_EXGINFO_DIR = os.path.join(_CUR_DIR, 'bhds_extra_exginfo')
    BHDS_SPLIT_CONFIG_PATH = os.path.join(_CUR_DIR, 'binance_candle_split.json')
    N_JOBS = int(os.getenv('CRYPTO_NJOBS', os.cpu_count() - 1))


Config = CFG()
