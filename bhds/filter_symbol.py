import os

from util.common import filter_symbols


def get_filtered_symbols(input_dir):
    symbols = sorted(os.path.splitext(x)[0] for x in os.listdir(input_dir))
    symbols = filter_symbols(symbols)
    return symbols
