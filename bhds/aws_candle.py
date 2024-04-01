import logging
import os
import hashlib
import shutil
from collections import defaultdict
from glob import glob
from pathlib import Path

import pandas as pd
from joblib import delayed, Parallel

from bhds.aws_util import aws_download_symbol_files
from config import Config

from .aws_util import aws_get_candle_dir, aws_batch_list_dir
from util import convert_interval_to_timedelta


async def get_aws_candle(type_, time_interval, symbols):
    symbol_to_dpath = {sym: aws_get_candle_dir(type_, sym, time_interval) for sym in symbols}
    prefix_dir = os.path.join(Config.BINANCE_DATA_DIR, 'aws_data')
    symbol_to_lddir = {
        sym: os.path.join(prefix_dir, aws_get_candle_dir(type_, sym, time_interval, local=True))
        for sym in symbols
    }
    dpath_to_aws_paths = await aws_batch_list_dir(symbol_to_dpath.values())
    aws_download_symbol_files(symbol_to_dpath, symbol_to_lddir, dpath_to_aws_paths)


def _read_aws_futures_candle_csv(p):
    columns = [
        'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ]
    df = pd.read_csv(p, names=columns)
    if df['candle_begin_time'].at[0] == 'open_time':
        df.drop(index=0, inplace=True)
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'].astype(int), unit='ms', utc=True)
    df['close_time'] = pd.to_datetime(df['close_time'].astype(int), unit='ms', utc=True)
    df.drop(columns='ignore', inplace=True)
    columns = [
        'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume'
    ]
    for c in columns:
        df[c] = df[c].astype(float)
    return df


def _verify(data_path, candles_per_day):
    checksum_path = data_path + '.CHECKSUM'
    if not os.path.exists(checksum_path):
        logging.error('Checksum file not exists %s', data_path)
        return False
    try:
        with open(checksum_path, 'r') as fin:
            text = fin.read()
        checksum_standard, _ = text.strip().split()
    except:
        logging.error('Error reading checksum file', checksum_path)
        return False

    with open(data_path, 'rb') as file_to_check:
        data = file_to_check.read()
        checksum_value = hashlib.sha256(data).hexdigest()

    if checksum_value != checksum_standard:
        logging.error('Checksum error %s', data_path)
        return False

    try:
        df = _read_aws_futures_candle_csv(data_path)
    except:
        logging.error('Cannot read csv %s', data_path)
        return False

    if candles_per_day is not None and len(df) != candles_per_day:
        logging.error('Num of candles error %d %s', len(df), data_path)
        return False

    return True


def verify_aws_candle(type_, time_interval, verify_num):
    local_dirs = glob(
        os.path.join(
            Config.BINANCE_DATA_DIR,
            'aws_data',
            aws_get_candle_dir(type_, '*', time_interval, local=True),
        ))
    symbols = [Path(os.path.normpath(d)).parts[-2] for d in local_dirs]
    for symbol in symbols:
        verify_candle(type_, symbol, time_interval, verify_num)


def verify_candle(type_, symbol, time_interval, verify_num):
    local_dir = os.path.join(Config.BINANCE_DATA_DIR, 'aws_data',
                             aws_get_candle_dir(type_, symbol, time_interval, local=True))
    logging.info('Local directory %s', local_dir)

    paths = sorted(glob(os.path.join(local_dir, '*.zip')))
    unverified_paths = []

    for p in paths:
        verify_file = p + '.verified'
        if not os.path.exists(verify_file):
            unverified_paths.append(p)

    if verify_num:
        candles_per_day = round(pd.Timedelta(days=1) / convert_interval_to_timedelta(time_interval))
        logging.info('Time interval %s, %d candles per day', time_interval, candles_per_day)
    else:
        candles_per_day = None
        logging.info('Will not be verified number of candles')

    logging.info('%d files to be verified', len(unverified_paths))
    if not unverified_paths:
        return

    if unverified_paths[0] == paths[0]:
        # Don't verify num of candles for the first day
        tasks = [delayed(_verify)(unverified_paths[0], None)]
    else:
        tasks = [delayed(_verify)(unverified_paths[0], candles_per_day)]

    tasks.extend([delayed(_verify)(p, candles_per_day) for p in unverified_paths[1:]])

    results = Parallel(n_jobs=Config.N_JOBS)(tasks)
    for unverified_path, verify_success in zip(unverified_paths, results):
        if verify_success:
            with open(unverified_path + '.verified', 'w') as fout:
                fout.write('')
        else:
            logging.warning('%s failed to verify, deleting', unverified_path)
            if os.path.exists(unverified_path):
                os.remove(unverified_path)
            checksum_path = unverified_path + '.CHECKSUM'
            if os.path.exists(checksum_path):
                os.remove(checksum_path)


def convert_symbol(symbol, output_dir, paths):
    dfs = [_read_aws_futures_candle_csv(p) for p in paths]
    df = pd.concat(dfs)
    df.sort_values('candle_begin_time', inplace=True, ignore_index=True)
    df.drop_duplicates('candle_begin_time', keep='last', inplace=True, ignore_index=True)
    output_path = os.path.join(output_dir, f'{symbol}.fea')
    df.to_feather(output_path, compression='zstd')


def convert_aws_candle_csv(type_, time_interval):
    paths = glob(
        os.path.join(
            Config.BINANCE_DATA_DIR,
            'aws_data',
            aws_get_candle_dir(type_, '*', time_interval, local=True),
            '*.zip',
        ))
    paths = sorted(paths)

    sym_paths = defaultdict(list)
    for p in paths:
        sym = p.split(os.sep)[-3]
        sym_paths[sym].append(p)

    logging.info('Symbols %s', list(sym_paths.keys()))

    odir = os.path.join(Config.BINANCE_DATA_DIR, 'candle_feather', type_, time_interval)
    if os.path.exists(odir):
        logging.warning('%s exists, deleting', odir)
        shutil.rmtree(odir)
    os.makedirs(odir)

    Parallel(n_jobs=Config.N_JOBS, verbose=1)(delayed(convert_symbol)(s, odir, ps) for s, ps, in sym_paths.items())
