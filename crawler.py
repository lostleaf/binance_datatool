import asyncio
import logging
import os
from itertools import islice

import pandas as pd

from candle_manager import CandleFeatherManager
from market_api import BinanceMarketApi, BinanceUsdtFutureMarketApi
from util import async_sleep_until_run_time, next_run_time, now_time


def batched(iterable, n):
    # batched('ABCDEFG', 3) --> ABC DEF G https://docs.python.org/3/library/itertools.html#itertools-recipes
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


class TradingUsdtSwapFilter:
    def __init__(self, keep_symbols=None):
        self.keep_symbols = set(keep_symbols) if keep_symbols else None
    
    @classmethod
    def is_trading_usdt_swap(cls, x):
        return x['quote_asset'] == 'USDT' and x['status'] == 'TRADING' and x['contract_type'] == 'PERPETUAL'

    def __call__(self, syminfo:dict) -> list:
        symbols = [info['symbol'] for info in syminfo.values() if self.is_trading_usdt_swap(info)]
        if self.keep_symbols is not None:
            symbols = [sym for sym in symbols if sym in self.keep_symbols]
        return symbols
    
class TradingCoinSwapFilter:
    def __init__(self, keep_symbols=None):
        self.keep_symbols = set(keep_symbols) if keep_symbols else None
    
    @classmethod
    def is_trading_usdt_swap(cls, x):
        return x['quote_asset'] == 'USD' and x['status'] == 'TRADING' and x['contract_type'] == 'PERPETUAL'

    def __call__(self, syminfo:dict) -> list:
        symbols = [info['symbol'] for info in syminfo.values() if self.is_trading_usdt_swap(info)]
        if self.keep_symbols is not None:
            symbols = [sym for sym in symbols if sym in self.keep_symbols]
        return symbols

class Crawler:

    def __init__(self, interval, exginfo_mgr, candle_mgr, market_api, symbol_filter):
        self.interval = interval
        self.market_api: BinanceMarketApi = market_api
        self.candle_mgr: CandleFeatherManager = candle_mgr
        self.exginfo_mgr: CandleFeatherManager = exginfo_mgr
        self.symbol_filter = symbol_filter
        self.exginfo_mgr.clear_all()
        self.candle_mgr.clear_all()

    async def init_history(self):
        syminfo = await self.market_api.get_syminfo()
        symbols_trading = self.symbol_filter(syminfo)
        limit = self.market_api.MAX_ONCE_CANDLES
        cnt = 0

        for sym_batch in batched(symbols_trading, 20):
            server_time, weight = await self.market_api.get_timestamp_and_weight()
            logging.info(f'Saved symbols: {cnt}, Server time:, {server_time}, Used weight: {weight}')
            if weight > self.market_api.MAX_MINUTE_WEIGHT * 0.9:
                await async_sleep_until_run_time(next_run_time('1m'))
            tasks = [self.market_api.get_candle(symbol, self.interval, limit=limit) for symbol in sym_batch]
            candles = await asyncio.gather(*tasks)
            for symbol, df in zip(sym_batch, candles):
                self.candle_mgr.set_candle(symbol, now_time(), df.iloc[:-1])
            cnt += len(sym_batch)

        server_time, weight = await self.market_api.get_timestamp_and_weight()
        logging.info(f'Saved symbols: {cnt}, Server time:, {server_time}, Used weight: {weight}')

    async def fetch_and_save_recent_closed_candle(self, symbol, run_time):
        df_new, is_closed = await self.market_api.fetch_recent_closed_candle(symbol, self.interval, run_time)
        df_old = self.candle_mgr.read_candle(symbol)
        df: pd.DataFrame = pd.concat([df_old, df_new]).drop_duplicates(subset='candle_begin_time', keep='last')
        df.sort_values('candle_begin_time', inplace=True)
        df = df.iloc[-self.market_api.MAX_ONCE_CANDLES:]
        self.candle_mgr.set_candle(symbol, run_time, df)
        return is_closed

    async def run_loop(self):
        run_time = next_run_time(self.interval)
        logging.info(f'Next candle crawler run at {run_time}')
        await async_sleep_until_run_time(run_time)

        syminfo = await self.market_api.get_syminfo()
        symbols_trading = self.symbol_filter(syminfo)
        symbols_last = self.candle_mgr.get_all_symbols()
        notradings = set(symbols_last) - set(symbols_trading)

        infos_trading = [info for sym, info in syminfo.items() if sym in symbols_trading]
        df_syminfo = pd.DataFrame.from_records(infos_trading)
        self.exginfo_mgr.set_candle('exginfo', run_time, df_syminfo)

        if notradings:
            logging.info(f'Remove not trading symbols {notradings}')
            for symbol in notradings:
                self.candle_mgr.remove_symbol(symbol)

        tasks = []
        for symbol in symbols_trading:
            tasks.append(self.fetch_and_save_recent_closed_candle(symbol, run_time))

        is_closed_list = await asyncio.gather(*tasks)
        may_not_closed = []
        for symbol, is_closed in zip(symbols_trading, is_closed_list):
            if not is_closed:
                may_not_closed.append(symbol)
        
        if may_not_closed:
            logging.warning(f'Candle may not closed: {may_not_closed}')

        server_time, weight = await self.market_api.get_timestamp_and_weight()
        num_symbols = len(self.candle_mgr.get_all_symbols())
        logging.info(f'Saved symbols: {num_symbols}. Server time: {server_time}, used weight: {weight}')
