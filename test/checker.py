import asyncio
from datetime import timedelta
import logging
import sys
import json
import os
import pandas as pd
from msg_sender.dingding import DingDingSender
from market_api import BinanceUsdtFutureMarketApi, BinanceCoinFutureMarketApi
from util import async_sleep_until_run_time, create_aiohttp_session, next_run_time
from candle_manager import CandleFeatherManager
from util.time import now_time

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')


async def wait_until_ready(mgr: CandleFeatherManager, symbol, run_time, expire_time):
    while not mgr.check_ready(symbol, run_time):
        await asyncio.sleep(0.01)
        if now_time() > expire_time:
            return False

    return True


def check_diff(symbol, diff_max: pd.Series):
    for i, v in diff_max.items():
        if 'time' in i:
            v = v.total_seconds()
        if v > 0:
            raise RuntimeError(f'{symbol} mismatch, {i} {v}')


class Checker:

    def __init__(self, interval, exginfo_mgr, candle_mgr, market_api):
        self.interval = interval
        self.exginfo_mgr: CandleFeatherManager = exginfo_mgr
        self.candle_mgr: CandleFeatherManager = candle_mgr
        self.expire_delta = timedelta(seconds=30)
        self.market_api: BinanceUsdtFutureMarketApi = market_api

    async def check_symbol_ready_in_time(self, symbol, run_time):
        is_ready = await wait_until_ready(self.candle_mgr, symbol, run_time, run_time + self.expire_delta)
        if not is_ready:
            return None
        return now_time()

    async def check_ready_in_time(self, run_time):
        is_ready = await wait_until_ready(self.exginfo_mgr, 'exginfo', run_time, run_time + self.expire_delta)

        if not is_ready:
            raise RuntimeError(f'{run_time} exginfo not ready in time')

        logging.info(f'{run_time} exginfo ready at {now_time()}')

        df_exginfo = self.exginfo_mgr.read_candle('exginfo')

        tasks = [self.check_symbol_ready_in_time(sym, run_time) for sym in df_exginfo['symbol']]
        results = await asyncio.gather(*tasks)

        not_ready_symbols = []
        last_ready_symbol, last_ready_time = None, None

        for symbol, result in zip(df_exginfo['symbol'], results):
            if result is None:
                not_ready_symbols.append(symbol)
            else:
                if last_ready_time is None or result > last_ready_time:
                    last_ready_symbol, last_ready_time = symbol, result

        if not_ready_symbols:
            raise RuntimeError(f'{run_time} {not_ready_symbols} not ready in time')

        logging.info(f'{run_time}, {len(results)} symbols ready, last ready {last_ready_symbol} {last_ready_time}')

    async def run_loop(self):
        run_time = next_run_time(self.interval)
        logging.info(f'Next checker run at {run_time}')
        await async_sleep_until_run_time(run_time)
        await self.check_ready_in_time(run_time)
        sleep_sec = (run_time + timedelta(minutes=3) - now_time()).total_seconds()
        await asyncio.sleep(sleep_sec)
        df_exginfo = self.exginfo_mgr.read_candle('exginfo')
        symbols = self.candle_mgr.get_all_symbols()
        symbols_match = set(df_exginfo['symbol']) == set(symbols)

        if symbols_match:
            logging.info(f'Symbols match {symbols_match}')
        else:
            raise RuntimeError(
                f'Syms mismatch {set(df_exginfo["symbol"]) - set(symbols)} {set(symbols) - set(df_exginfo["symbol"])}')

        for i, symbol in enumerate(symbols):
            df_candle = await self.market_api.get_candle(symbol, self.interval, limit=self.market_api.MAX_ONCE_CANDLES)
            df_candle_api = df_candle.iloc[:-1].tail(1400).reset_index(drop=True)
            df_candle_fea = self.candle_mgr.read_candle(symbol).tail(1400).reset_index(drop=True)
            diff_max = (df_candle_api - df_candle_fea).max()
            check_diff(symbol, diff_max)

        logging.info('All symbols candle correct')


async def main(argv):
    base_dir = argv[1]
    cfg = json.load(open(os.path.join(base_dir, 'config.json')))

    interval = cfg['interval']
    http_timeout_sec = cfg['http_timeout_sec']
    trade_type = cfg['trade_type']
    dingding_cfg = cfg.get('dingding', None)

    while True:
        try:
            async with create_aiohttp_session(http_timeout_sec) as session:

                candle_mgr = CandleFeatherManager(os.path.join(base_dir, f'{trade_type}_{interval}'))
                exginfo_mgr = CandleFeatherManager(os.path.join(base_dir, f'exginfo_{interval}'))
                market_api = BinanceUsdtFutureMarketApi(session, 12)
                checker = Checker(interval, exginfo_mgr, candle_mgr, market_api)
                # await checker.check_ready_in_time(pd.to_datetime('2023-04-06 22:30:00+08:00'))
                while True:
                    await checker.run_loop()
        except Exception as e:
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


if __name__ == '__main__':
    asyncio.run(main(sys.argv))