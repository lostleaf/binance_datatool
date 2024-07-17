import json
import os

from config import Config
from util.common import filter_symbols


def read_candle_splits():
    return json.load(open(Config.BHDS_SPLIT_CONFIG_PATH))


def get_filtered_symbols(input_dir):
    symbols = sorted(os.path.splitext(x)[0] for x in os.listdir(input_dir))
    symbols = filter_symbols(symbols)
    return symbols
