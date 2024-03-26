import logging
import os

import pandas as pd

from config import Config

from .util import TYPE_OUTPUT_DIR


def check_candle(type_, time_interval, symbol):
    logging.info('Check aws with xbx candlestick %s %s %s', type_, time_interval, symbol)

    path_aws = os.path.join(Config.BINANCE_DATA_DIR, TYPE_OUTPUT_DIR[type_].format(time_interval), f'{symbol}.fea')
    df_aws = pd.read_feather(path_aws)
    symbol_xbx = symbol.replace('USDT', '-USDT')
    path_xbx = os.path.join(Config.BINANCE_DATA_XBX_DIR, TYPE_OUTPUT_DIR[type_].format(time_interval),
                            f'{symbol_xbx}.fea')
    df_xbx = pd.read_feather(path_xbx)

    logging.info('Aws candle time from %s to %s', df_aws['candle_begin_time'].min(), df_aws['candle_begin_time'].max())
    logging.info('Xbx candle time from %s to %s', df_xbx['candle_begin_time'].min(), df_xbx['candle_begin_time'].max())

    begin_ts = max(df_aws['candle_begin_time'].min(), df_xbx['candle_begin_time'].min())
    end_ts = min(df_aws['candle_begin_time'].max(), df_xbx['candle_begin_time'].max())

    logging.info('Time from %s to %s', begin_ts, end_ts)

    df_aws = df_aws[df_aws['candle_begin_time'].between(begin_ts, end_ts)]
    df_xbx = df_xbx[df_xbx['candle_begin_time'].between(begin_ts, end_ts)]

    logging.info('Trimmed aws shape %s', df_aws.shape)
    logging.info('Trimmed xbx shape %s', df_xbx.shape)

    df = df_aws.join(df_xbx.set_index('candle_begin_time'), on='candle_begin_time', rsuffix='_xbx')
    columns = [
        'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume'
    ]

    error_example = None
    for c in columns:
        df['diff'] = (df[c] - df[f'{c}_xbx']).abs()
        max_diff = df['diff'].max()
        diff_num = (df['diff'] > 1e-4).sum()
        logging.info('Column: %s, max diff %f, diff num %d', c, max_diff, diff_num)
        if max_diff > 1e-4:
            error_example = str(df[df['diff'] == max_diff].iloc[0])
        df.drop(columns='diff', inplace=True)

    if error_example is not None:
        logging.error('\n%s', error_example)
