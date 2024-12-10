import os
from pathlib import Path

_DEFAULT_BASE_DIR = os.path.join(os.path.expanduser('~'), 'crypto_data')
_BASE_DIR = Path(os.getenv('CRYPTO_BASE_DIR', _DEFAULT_BASE_DIR))
_CUR_DIR = Path(os.path.dirname(os.path.realpath(__file__)))


class CFG:
    BINANCE_DATA_DIR = _BASE_DIR / 'binance_data'

    BHDS_EXTRA_EXGINFO_DIR = _CUR_DIR / 'bhds_extra_exginfo'
    BHDS_KLINE_GAPS_DIR = _CUR_DIR / 'bhds_kline_gaps'

    N_JOBS = int(os.getenv('CRYPTO_NJOBS', os.cpu_count() - 2))


Config = CFG()
