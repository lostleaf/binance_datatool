import asyncio
import json
import logging
import os
import sys

from candle_manager import CandleFeatherManager
from crawler import Crawler, TradingCoinSwapFilter, TradingUsdtSwapFilter
from dingding import DingDingSender
from market_api import BinanceCoinFutureMarketApi, BinanceUsdtFutureMarketApi
from util import create_aiohttp_session, now_time

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')

MARKET_API_DICT = {'usdt_swap': BinanceUsdtFutureMarketApi, 'coin_swap': BinanceCoinFutureMarketApi}

SYMBOL_FILTER_DICT = {'usdt_swap': TradingUsdtSwapFilter, 'coin_swap': TradingCoinSwapFilter}


async def main(argv):
    #从 argv 中获取根目录
    base_dir = argv[1]

    # 读取 config.json，获取配置
    cfg = json.load(open(os.path.join(base_dir, 'config.json')))

    interval = cfg['interval']
    http_timeout_sec = cfg['http_timeout_sec']
    candle_close_timeout_sec = cfg['candle_close_timeout_sec']
    trade_type = cfg['trade_type']
    keep_symbols = cfg.get('keep_symbols', None)
    dingding_cfg = cfg.get('dingding', None)

    market_api_cls = MARKET_API_DICT[trade_type]
    symbol_filter_cls = SYMBOL_FILTER_DICT[trade_type]

    while True:
        try:
            async with create_aiohttp_session(http_timeout_sec) as session:
                # 实例化所有涉及的类
                market_api = market_api_cls(session, candle_close_timeout_sec)
                symbol_filter = symbol_filter_cls(keep_symbols)
                candle_mgr = CandleFeatherManager(os.path.join(base_dir, f'{trade_type}_{interval}'))
                exginfo_mgr = CandleFeatherManager(os.path.join(base_dir, f'exginfo_{interval}'))
                crawler = Crawler(interval, exginfo_mgr, candle_mgr, market_api, symbol_filter)
                msg_sender = DingDingSender(dingding_cfg, session) if dingding_cfg is not None else None

                # 首先获取历史数据
                await crawler.init_history()

                # 无限循环，每周期获取最新K线
                while True:
                    msg = await crawler.run_loop()
                    if msg and msg_sender:
                        msg['localtime'] = str(now_time())
                        await msg_sender.send_message(json.dumps(msg, indent=1), 'error')
        except Exception as e:
            # 出错则通过钉钉报错
            logging.error(f'An error occurred {str(e)}')
            import traceback
            traceback.print_exc()
            if dingding_cfg is not None:
                try:
                    error_stack_str = traceback.format_exc()
                    async with create_aiohttp_session(http_timeout_sec) as session:
                        msg_sender = DingDingSender(dingding_cfg, session)
                        msg = f'An error occurred {str(e)}\n' + error_stack_str
                        await msg_sender.send_message(msg, 'error')
                except:
                    pass
            await asyncio.sleep(10)


if __name__ == '__main__':
    asyncio.run(main(sys.argv))