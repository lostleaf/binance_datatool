import asyncio
import logging
import sys
import json
import os

from crawler import Crawler, TradingUsdtSwapFilter, TradingCoinSwapFilter
from dingding import DingDingSender
from market_api import BinanceUsdtFutureMarketApi, BinanceCoinFutureMarketApi
from util import create_aiohttp_session
from candle_manager import CandleFeatherManager

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')

MARKET_API_DICT = {'usdt_swap': BinanceUsdtFutureMarketApi, 'coin_swap': BinanceCoinFutureMarketApi}

SYMBOL_FILTER_DICT = {'usdt_swap': TradingUsdtSwapFilter, 'coin_swap': TradingCoinSwapFilter}


async def main(argv):
    base_dir = argv[1]
    cfg = json.load(open(os.path.join(base_dir, 'config.json')))

    interval = cfg['interval']
    http_timeout_sec = cfg['http_timeout_sec']
    candle_close_timeout_sec = cfg['candle_close_timeout_sec']
    trade_type = cfg['trade_type']
    keep_symbols = cfg.get('keep_symbols', None)

    market_api_cls = MARKET_API_DICT[trade_type]
    symbol_filter_cls = SYMBOL_FILTER_DICT[trade_type]

    while True:
        try:
            async with create_aiohttp_session(http_timeout_sec) as session:
                market_api = market_api_cls(session, candle_close_timeout_sec)
                symbol_filter = symbol_filter_cls(keep_symbols)
                candle_mgr = CandleFeatherManager(os.path.join(base_dir, f'{trade_type}_{interval}'))
                exginfo_mgr = CandleFeatherManager(os.path.join(base_dir, f'exginfo_{interval}'))
                crawler = Crawler(interval, exginfo_mgr, candle_mgr, market_api, symbol_filter)

                await crawler.init_history()
                while True:
                    await crawler.run_loop()
        except Exception as e:
            logging.error(f'An error occurred {str(e)}')
            import traceback
            traceback.print_exc()
            raise e


if __name__ == '__main__':
    asyncio.run(main(sys.argv))