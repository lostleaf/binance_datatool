import logging
import os
import shutil
from collections import defaultdict
from glob import glob

import pandas as pd
from joblib import Parallel, delayed

from config import Config


def _read_quantclass_csv(p):
    df = pd.read_csv(p, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
    df.sort_values(by='candle_begin_time', inplace=True)
    df['candle_begin_time'] = df['candle_begin_time'].dt.tz_localize('UTC')
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')
    df.reset_index(drop=True, inplace=True)
    return df


def convert_symbol(symbol, output_dir, files):
    dfs = [_read_quantclass_csv(p) for p in files]
    df = pd.concat(dfs)
    df.sort_values('candle_begin_time', inplace=True)
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last', ignore_index=True)
    output_path = os.path.join(output_dir, f'{symbol}.fea')
    df.to_feather(output_path, compression='zstd')


def convert_quantclass_candle_csv(type_, time_interval):
    logging.info('Convert quantclass candle %s %s', type_, time_interval)

    if type_ == 'usdt_futures':
        dir_name = f'swap_binance_{time_interval}'
    elif type_ == 'spot':
        dir_name = f'spot_binance_{time_interval}'
    else:
        raise ValueError(f'{type_} not supported')

    csv_dir = os.path.join(Config.BINANCE_QUANTCLASS_DIR, 'csv_data', dir_name)
    odir = os.path.join(Config.BINANCE_QUANTCLASS_DIR, 'candle_feather', type_, time_interval)

    if os.path.exists(odir):  # Remove dir if exists
        logging.warn(f'Output feather dir {odir} exists, deleting')
        shutil.rmtree(odir)
    os.makedirs(odir, exist_ok=True)

    csv_files = glob(os.path.join(csv_dir, f'*.csv')) + glob(os.path.join(csv_dir, '*', f'*.csv'))
    sym_files = defaultdict(list)

    for p in csv_files:
        symbol = os.path.splitext(os.path.basename(p))[0].replace('-', '')
        sym_files[symbol].append(p)

    Parallel(n_jobs=Config.N_JOBS, verbose=1)(delayed(convert_symbol)(sym, odir, fs) for sym, fs in sym_files.items())
