import asyncio
import logging
import os
from itertools import islice

import pandas as pd
from candle_manager import CandleFileManager
from market_api import BinanceMarketApi

from util import (DEFAULT_TZ, async_sleep_until_run_time, batched, next_run_time, parse_interval_str, now_time)


class Crawler:

    def __init__(self, interval, exginfo_mgr, candle_mgr, market_api, symbol_filter, fetch_funding_rate, num_candles):

        self.interval = interval
        self.market_api: BinanceMarketApi = market_api
        self.candle_mgr: CandleFileManager = candle_mgr
        self.exginfo_mgr: CandleFileManager = exginfo_mgr
        self.symbol_filter = symbol_filter
        self.fetch_funding_rate = fetch_funding_rate
        self.num_candles = num_candles
        self.exginfo_mgr.clear_all()
        self.candle_mgr.clear_all()



    async def fetch_and_save_history_candle(self, symbol, end_timestamp):

        limit = self.market_api.WEIGHT_EFFICIENT_ONCE_CANDLES
        if end_timestamp is None:
            candles = await self.market_api.get_candle(symbol, self.interval, limit=limit)
        else:
            candles = await self.market_api.get_candle(symbol, self.interval, limit=limit, endTime=end_timestamp)
        not_enough = candles.shape[0] < limit  # 已经没有足够的 K 线
        df_candle = self.candle_mgr.update_candle(symbol, now_time(), candles, self.num_candles)
        begin_time = df_candle['candle_begin_time'].min()
        num = df_candle.shape[0]
        return not_enough, begin_time, num

    async def fetch_and_save_recent_closed_candle(self, symbol, run_time):
        df_new, is_closed = await self.market_api.fetch_recent_closed_candle(symbol, self.interval, run_time)
        self.candle_mgr.update_candle(symbol, run_time, df_new, self.num_candles)
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
                interval_delta = parse_interval_str(self.interval)
                df_funding_old = self.exginfo_mgr.read_candle('funding')
                df_funding = pd.concat([df_funding_old, df_funding])
                min_time = run_time - interval_delta * self.market_api.MAX_ONCE_CANDLES
                df_funding = df_funding[df_funding['time'] >= min_time]
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
