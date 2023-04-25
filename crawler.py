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
        '''
        筛选出所有USDT本位的，正在被交易的(TRADING)，永续合约（PERPETUAL）
        '''
        return x['quote_asset'] == 'USDT' and x['status'] == 'TRADING' and x['contract_type'] == 'PERPETUAL'

    def __call__(self, syminfo: dict) -> list:
        symbols = [info['symbol'] for info in syminfo.values() if self.is_trading_usdt_swap(info)]
        if self.keep_symbols is not None:  # 如有白名单，则只保留白名单内的
            symbols = [sym for sym in symbols if sym in self.keep_symbols]
        return symbols


class TradingCoinSwapFilter:

    def __init__(self, keep_symbols=None):
        self.keep_symbols = set(keep_symbols) if keep_symbols else None

    @classmethod
    def is_trading_coin_swap(cls, x):
        '''
        筛选出所有币本位的，正在被交易的(TRADING)，永续合约（PERPETUAL）
        '''
        return x['quote_asset'] == 'USD' and x['status'] == 'TRADING' and x['contract_type'] == 'PERPETUAL'

    def __call__(self, syminfo: dict) -> list:
        symbols = [info['symbol'] for info in syminfo.values() if self.is_trading_coin_swap(info)]
        if self.keep_symbols is not None:  # 如有白名单，则只保留白名单内的
            symbols = [sym for sym in symbols if sym in self.keep_symbols]
        return symbols


class Crawler:

    def __init__(self, interval, exginfo_mgr, candle_mgr, market_api, symbol_filter, fetch_funding_rate):
        '''
        interval: K线周期
        exginfo_mgr: 用于管理 exchange info（合约交易规则）的 CandleFeatherManager
        candle_mgr: 用于管理 K线的 CandleFeatherManager
        market_api: BinanceMarketApi 的子类，用于请求币本位或 USDT 本位合约公有 API
        symbol_filter: 用于过滤出 symbol

        初始化阶段，exginfo_mgr 和 candle_mgr，会清空历史数据并建立数据目录
        '''
        self.interval = interval
        self.market_api: BinanceMarketApi = market_api
        self.candle_mgr: CandleFeatherManager = candle_mgr
        self.exginfo_mgr: CandleFeatherManager = exginfo_mgr
        self.symbol_filter = symbol_filter
        self.fetch_funding_rate = fetch_funding_rate
        self.exginfo_mgr.clear_all()
        self.candle_mgr.clear_all()

    async def init_history(self):
        '''
        初始化历史阶段 init_history
        1. 通过调用 self.market_api.get_syminfo 获取所有交易的 symbol, 并根据 symbol_filter 过滤出我们想要的 symbol
        2. 通过调用 self.market_api.get_candle，请求每个 symbol 最近 1500 根K线（币安最大值）
        3. 将每个 symbol 获取的 1500 根近期的 K线通过 self.candle_mgr.set_candle 写入文件
        '''
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
        self.candle_mgr.update_candle(symbol, run_time, df_new)
        return is_closed

    async def run_loop(self):
        '''
        定时获取 K线 run_loop
        '''

        # 1. 计算出 self.interval 周期下次运行时间 run_time, 并 sleep 到 run_time
        run_time = next_run_time(self.interval)
        msg = dict()
        logging.info(f'Next candle crawler run at {run_time}')
        await async_sleep_until_run_time(run_time)

        # 2. 通过调用 self.market_api.get_syminfo 获取所有交易的 symbol 及交易规则, 并根据 symbol_filter 过滤出我们想要的 symbol
        syminfo = await self.market_api.get_syminfo()
        symbols_trading = self.symbol_filter(syminfo)
        symbols_last = self.candle_mgr.get_all_symbols()
        notradings = set(symbols_last) - set(symbols_trading)

        # 3. 删除之前有交易，但目前没有交易的 symbol（可能可以防止 BNXUSDT 拆分之类的事件），这些停止交易的 symbol 会发送钉钉警告
        infos_trading = [info for sym, info in syminfo.items() if sym in symbols_trading]
        df_syminfo = pd.DataFrame.from_records(infos_trading)
        self.exginfo_mgr.set_candle('exginfo', run_time, df_syminfo)

        if notradings:
            logging.info(f'Remove not trading symbols {notradings}')
            msg['not_trading'] = list(notradings)
            for symbol in notradings:
                self.candle_mgr.remove_symbol(symbol)

        # 4. 获取资金费率（如果需要）
        if self.fetch_funding_rate:
            df_funding = await self.market_api.get_funding_rate()
            df_funding['time'] = run_time
            if self.exginfo_mgr.has_symbol('funding'):
                df_funding_old = self.exginfo_mgr.read_candle('funding')
                df_funding = pd.concat([df_funding_old, df_funding])
            self.exginfo_mgr.set_candle('funding', run_time, df_funding)

        # 5. 对所有这在交易的 symbol, 调用 self.market_api.fetch_recent_closed_candle 获取最近 5 根 K线
        # 将获取的 K线通过 self.candle_mgr.update_candle 写入 feather，并更新 ready file，未闭合 K线也会被写入，并发送钉钉警告
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
            msg['not_closed'] = list(may_not_closed)

        # 6. 获取并打印本轮运行结束服务器时间以及消耗的权重
        server_time, weight = await self.market_api.get_timestamp_and_weight()
        num_symbols = len(self.candle_mgr.get_all_symbols())
        logging.info(f'Saved symbols: {num_symbols}. Server time: {server_time}, used weight: {weight}')
        return msg
