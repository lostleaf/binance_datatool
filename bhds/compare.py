import os

import pandas as pd

from config import Config
from util import get_logger

logger = get_logger()

def compare_aws_quantclass_candle(type_, time_interval, symbol):
    logger.info('Compare AWS with Quantclass candlestick %s %s %s', type_, time_interval, symbol)

    path_aws = os.path.join(Config.BINANCE_DATA_DIR, 'candle_parquet_fixed', type_, time_interval, f'{symbol}.pqt')
    logger.info('Path %s AWS', path_aws)

    path_qtc = os.path.join(Config.BINANCE_QUANTCLASS_DIR, 'candle_parquet_fixed', type_, time_interval, f'{symbol}.pqt')
    logger.info('Path %s Quantclass', path_qtc)

    df_aws = pd.read_parquet(path_aws)
    logger.info('Time %s -- %s AWS', df_aws['candle_begin_time'].min(), df_aws['candle_begin_time'].max())

    df_qtc = pd.read_parquet(path_qtc)
    logger.info('Time %s -- %s Quantclass', df_qtc['candle_begin_time'].min(), df_qtc['candle_begin_time'].max())

    begin_ts = max(df_aws['candle_begin_time'].min(), df_qtc['candle_begin_time'].min())
    end_ts = min(df_aws['candle_begin_time'].max(), df_qtc['candle_begin_time'].max())
    logger.info('Time %s -- %s', begin_ts, end_ts)

    df_aws = df_aws[df_aws['candle_begin_time'].between(begin_ts, end_ts)]
    logger.info('Trimmed shape %s AWS', df_aws.shape)

    df_qtc = df_qtc[df_qtc['candle_begin_time'].between(begin_ts, end_ts)]
    logger.info('Trimmed shape %s Quantclass', df_qtc.shape)

    ts_intersect = set(df_aws['candle_begin_time']).intersection(set(df_qtc['candle_begin_time']))
    logger.info('Intersecion num candle_begin_time %s', len(ts_intersect)) 
 
    df = df_aws.join(df_qtc.set_index('candle_begin_time'), on='candle_begin_time', rsuffix='_qtc')

    cols = [
        'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume'
    ]

    error_begin_time = None
    for c in cols:
        df['diff'] = (df[c] - df[f'{c}_qtc']).abs()
        max_diff = df['diff'].max()
        diff_num = (df['diff'] > 1e-4).sum()
        logger.info('Column: %s, max diff %f, diff num %d', c, max_diff, diff_num)
        if max_diff > 1e-4:
            error_begin_time = df[df['diff'] == max_diff].iloc[0]['candle_begin_time']
        df.drop(columns='diff', inplace=True)

    if error_begin_time is not None:
        df_err = pd.concat([
            df_aws.loc[df_aws['candle_begin_time'] == error_begin_time, cols],
            df_qtc.loc[df_qtc['candle_begin_time'] == error_begin_time, cols]
        ])
        logger.error('%s\n%s', error_begin_time, df_err)
