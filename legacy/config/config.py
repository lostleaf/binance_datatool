import os
from enum import Enum
from pathlib import Path

_DEFAULT_BASE_DIR = os.path.join(os.path.expanduser('~'), 'crypto_data')
_BASE_DIR = Path(os.getenv('CRYPTO_BASE_DIR', _DEFAULT_BASE_DIR))
_CUR_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

BINANCE_DATA_DIR = _BASE_DIR / 'binance_data'

BHDS_EXTRA_EXGINFO_DIR = _CUR_DIR / 'bhds_extra_exginfo'
BHDS_KLINE_GAPS_DIR = _CUR_DIR / 'bhds_kline_gaps'

N_JOBS = int(os.getenv('CRYPTO_NJOBS', os.cpu_count() - 2))

HTTP_TIMEOUT_SEC = 15


class TradeType(str, Enum):
    spot = 'spot'
    um_futures = 'um_futures'
    cm_futures = 'cm_futures'


class ContractType(str, Enum):
    perpetual = 'PERPETUAL'
    delivery = 'DELIVERY'


class DataFrequency(Enum):
    """
    Data frequency enumeration class, used to represent the partition type of data.

    Enum values:
        yearly: Yearly partition, indicating data is divided by year.
        monthly: Monthly partition, indicating data is divided by month.
        daily: Daily partition, indicating data is divided by day.
    """
    yearly = 'yearly'  # Yearly partition, data divided by year
    monthly = 'monthly'  # Monthly partition, data divided by month
    daily = 'daily'  # Daily partition, data divided by day
