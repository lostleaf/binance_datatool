import json

from config import Config

def read_candle_splits():
    return json.load(open(Config.BHDS_SPLIT_CONFIG_PATH))