import json
import logging
import os

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


def read_extra_exginfo(type_):
    p = os.path.join(Config.BHDS_EXTRA_EXGINFO_DIR, f'{type_}.json')
    logging.info('Read extra exginfo %s', p)
    if os.path.exists(p):
        return json.load(open(p))
    return dict()


async def update_exchange_info(type_):
    async with create_aiohttp_session(BINANCE_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(type_, session)
        logging.info('Update exchange info for Binance %s', fetcher.trade_type)
        exg_info = await fetcher.get_exchange_info()

    if type_.endswith('_futures'):
        exg_info = {k: v for k, v in exg_info.items() if v['contract_type'] == 'PERPETUAL'}

    info_new = {symbol: _get_info(x) for symbol, x in exg_info.items()}
    extra_info = read_extra_exginfo(type_)
    info_new.update(extra_info)

    output_dir = os.path.join(Config.BINANCE_DATA_DIR, 'exginfo')
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f'{type_}.json')
    if os.path.exists(output_path):
        logging.info('Load existing exchange info %s', output_path)
        info: dict = json.load(open(output_path, 'r'))
        info.update(info_new)
    else:
        info = info_new

    logging.info('Output exchange info to %s', output_path)
    json.dump(info, open(output_path, 'w'), indent=2)
