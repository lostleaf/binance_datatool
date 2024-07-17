import json
import logging
import os
import shutil

import pandas as pd
from joblib import Parallel, delayed

from config import Config
from util import convert_interval_to_timedelta

from .bhds_util import get_filtered_symbols, read_candle_splits


def check(df, symbol, hours_threshold):
    df['time_diff'] = df['candle_begin_time'].diff()

    splits = []
    idxes = df[df['time_diff'] > df['time_diff'].min()].index

    for idx in idxes:
        tail = df.loc[:idx].tail(2)
        end_time_before = tail.index[0]
        begin_time_after = tail.iloc[1]['candle_begin_time']
        time_gap = begin_time_after - end_time_before
        if time_gap > pd.Timedelta(hours=hours_threshold):
            splits.append((end_time_before, begin_time_after))

    if not splits:
        return None
    last_begin = None
    segs = []
    for i, (end_time_before, begin_time_after) in enumerate(splits, 1):
        symbol_new = f'{symbol[:-4]}{i}{symbol[-4:]}'
        segs.append([last_begin, str(end_time_before), symbol_new])
        last_begin = str(begin_time_after)
    segs.append([last_begin, None, symbol])
    return segs


def check_gaps(source, type_, time_interval, hours_threshold):
    input_dir = _get_input_dir(source, type_, time_interval)
    logging.info('Check candle data %s, hours_threshold=%d', input_dir, hours_threshold)
    symbols = get_filtered_symbols(input_dir)
    results = dict()
    for symbol in symbols:
        candle_path = os.path.join(input_dir, f'{symbol}.pqt')
        df = pd.read_parquet(candle_path)
        ret = check(df, symbol, hours_threshold)
        if ret is not None:
            results[symbol] = ret
    print(json.dumps(results))


def _get_input_dir(source, type_, time_interval):
    if source == 'aws':
        source_dir = Config.BINANCE_DATA_DIR
    elif source == 'quantclass':
        source_dir = Config.BINANCE_QUANTCLASS_DIR
    else:
        raise ValueError('%s is not supported', source)

    input_dir = os.path.join(source_dir, 'candle_parquet', type_, time_interval)
    return input_dir


def _fill_gap(df: pd.DataFrame, delta: pd.Timedelta, symbol: str) -> pd.DataFrame:

    # Create a benchmark from begin to end with no gaps
    first = df['candle_begin_time'].min()
    last = df['candle_begin_time'].max()
    benchmark = pd.DataFrame({'candle_begin_time': pd.date_range(first, last, freq=delta)})

    # Merge with benchmark
    df = pd.merge(left=benchmark, right=df, on='candle_begin_time', how='left', sort=True, indicator=True)

    # df_left_only = df[df['_merge'] == 'left_only']
    # if len(df_left_only) > 0:
    #     logging.warning('%s has %d missings', symbol, len(df_left_only))

    df.drop(columns=['_merge'], inplace=True)

    # Fill prices with previous close
    df['close'] = df['close'].ffill()
    df['open'] = df['open'].fillna(df['close'])
    df['high'] = df['high'].fillna(df['close'])
    df['low'] = df['low'].fillna(df['close'])

    # Fill Vwaps with open
    if 'avg_price_1m' in df.columns:
        df['avg_price_1m'] = df['avg_price_1m'].fillna(df['open'])

    # Fill volumes with 0
    df['volume'] = df['volume'].fillna(0)
    df['quote_volume'] = df['quote_volume'].fillna(0)
    df['trade_num'] = df['trade_num'].fillna(0)
    df['taker_buy_base_asset_volume'] = df['taker_buy_base_asset_volume'].fillna(0)
    df['taker_buy_quote_asset_volume'] = df['taker_buy_quote_asset_volume'].fillna(0)

    df['candle_end_time'] = df['candle_begin_time'] + delta
    df.set_index('candle_end_time', inplace=True)
    return df


def fix_candle(source, type_, time_interval):
    logging.info('Fix candles for %s %s %s', source, type_, time_interval)

    input_dir = _get_input_dir(source, type_, time_interval)
    logging.info('Input dir %s', input_dir)

    output_dir = _create_fixed_output_dir(input_dir)
    logging.info('Output dir %s', output_dir)

    if type_ == 'coin_futures':
        symbols = sorted(os.path.splitext(x)[0] for x in os.listdir(input_dir))
    else:
        symbols = get_filtered_symbols(input_dir)
    logging.info('Symbols %s', symbols)

    delta = convert_interval_to_timedelta(time_interval)

    def _split_and_fill(symbol):
        candle_path = os.path.join(input_dir, f'{symbol}.pqt')
        df = pd.read_parquet(candle_path)
        df = df[df['volume'] > 0]

        binance_candle_splits = read_candle_splits()

        if type_ not in binance_candle_splits or symbol not in binance_candle_splits[type_]:
            output_path = os.path.join(output_dir, f'{symbol}.pqt')
            df_fixed = _fill_gap(df, delta, symbol)
            df_fixed.to_parquet(output_path, compression='zstd')
            return

        splits = binance_candle_splits[type_][symbol]
        for begin_time, end_time, symbol_new in splits:
            output_path = os.path.join(output_dir, f'{symbol_new}.pqt')
            logging.warning('Split %s %s - %s to %s', symbol, begin_time, end_time, output_path)
            df_split = df.loc[begin_time:end_time]
            if len(df_split) == 0:
                continue
            df_split = _fill_gap(df_split, delta, symbol_new)
            df_split.to_parquet(output_path, compression='zstd')

    Parallel(Config.N_JOBS)(delayed(_split_and_fill)(symbol) for symbol in symbols)


def _create_fixed_output_dir(input_dir):
    output_dir = input_dir.replace('candle_parquet', 'candle_parquet_fixed')
    if os.path.exists(output_dir):
        logging.warning('%s exists, deleting', output_dir)
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    return output_dir
