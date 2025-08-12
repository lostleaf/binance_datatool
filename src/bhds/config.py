import os
from enum import Enum
from pathlib import Path

_DEFAULT_BASE_DIR = os.path.join(os.path.expanduser("~"), "crypto_data")
_BASE_DIR = Path(os.getenv("CRYPTO_BASE_DIR", _DEFAULT_BASE_DIR))
_CUR_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

BINANCE_DATA_DIR = _BASE_DIR / "binance_data"
N_JOBS = int(os.getenv("CRYPTO_NJOBS", os.cpu_count() - 2))
