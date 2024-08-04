import os
import shutil
from collections import defaultdict
from datetime import timedelta
from glob import glob

import pandas as pd
from joblib import Parallel, delayed

from config import Config
from util import convert_interval_to_timedelta, get_logger

logger = get_logger()


def _read_quantclass_csv(p):
    df = pd.read_csv(p, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
    df['candle_begin_time'] = df['candle_begin_time'].dt.tz_localize('UTC')
    return df


def convert_quantclass_candle_csv(type_, time_interval):
    logger.info('Convert quantclass candle %s %s', type_, time_interval)

    csv_dir = _get_csv_dir(type_, time_interval)
    output_dir = _create_output_dir(type_, time_interval)

    sym_files = _group_csv_files(csv_dir)

    delta = convert_interval_to_timedelta(time_interval)

    def _convert(symbol, files):
        df = pd.concat([_read_quantclass_csv(p) for p in files])

        df.sort_values('candle_begin_time', inplace=True)
        df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')

        df['candle_end_time'] = df['candle_begin_time'] + delta
        df.set_index('candle_end_time', inplace=True)

        cols = [
            'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'avg_price_1m'
        ]
        df = df[cols]

        df.to_parquet(os.path.join(output_dir, f'{symbol}.pqt'), compression='zstd')

    Parallel(
        n_jobs=Config.N_JOBS,
        verbose=1,
    )(delayed(_convert)(symbol, files) for symbol, files in sym_files.items())


def _group_csv_files(csv_dir) -> dict[str, list]:
    csv_files = glob(os.path.join(csv_dir, f'*.csv')) + glob(os.path.join(csv_dir, '*', f'*.csv'))

    sym_files = defaultdict(list)
    for p in csv_files:
        symbol = os.path.splitext(os.path.basename(p))[0].replace('-', '')
        sym_files[symbol].append(p)
    return sym_files


def _create_output_dir(type_, time_interval):
    dir_name = 'candle_parquet'

    output_dir = os.path.join(Config.BINANCE_QUANTCLASS_DIR, dir_name, type_, time_interval)

    if os.path.exists(output_dir):  # Remove dir if exists
        logger.warn(f'Output dir {output_dir} exists, deleting')
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def _get_csv_dir(type_, time_interval):
    if type_ == 'usdt_futures':
        dir_name = f'swap_binance_{time_interval}'
    elif type_ == 'spot':
        dir_name = f'spot_binance_{time_interval}'
    else:
        raise ValueError(f'{type_} not supported')

    csv_dir = os.path.join(Config.BINANCE_QUANTCLASS_DIR, 'csv_data', dir_name)
    return csv_dir
