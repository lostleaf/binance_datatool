import json
import logging
import os

import simplejson

from api.binance import (BinanceMarketCMDapi, BinanceMarketSpotApi, BinanceMarketUMFapi)
from config import Config
from fetcher import BinanceFetcher
from util import create_aiohttp_session, remove_exponent

TYPE_API_MAP = {
    'spot': BinanceMarketSpotApi,
    'coin_futures': BinanceMarketCMDapi,
    'usdt_futures': BinanceMarketUMFapi,
}

BINANCE_TIMEOUT_SEC = 15


def _get_info(x):
    i = {
        'price_tick': format(remove_exponent(x['price_tick']), 'f'),
        'lot_size': format(remove_exponent(x['lot_size']), 'f')
    }

    if 'min_notional_value' in x:
        i['min_notional_value'] = format(remove_exponent(x['min_notional_value']), 'f')
    return i


async def update_exchange_info(type_):
    async with create_aiohttp_session(BINANCE_TIMEOUT_SEC) as session:
        market_api = TYPE_API_MAP[type_](session)
        fetcher = BinanceFetcher(market_api)
        logging.info('Update exchange info for Binance %s', fetcher.trade_type)
        exg_info = await fetcher.get_exchange_info()

    if type_.endswith('_futures'):
        exg_info = {k: v for k, v in exg_info.items() if v['contract_type'] == 'PERPETUAL'}
    info_new = {symbol: _get_info(x) for symbol, x in exg_info.items()}
    
    if os.path.exists(Config.BINANCE_EXGINFO_PATH[type_]):
        info: dict = Config.BINANCE_EXGINFO[type_]
        info.update(info_new)
    else:
        info = info_new
    simplejson.dump(info, open(Config.BINANCE_EXGINFO_PATH[type_], 'w'), indent=2)
