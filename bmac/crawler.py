import asyncio
import logging
import os
from itertools import islice

import pandas as pd

from candle_manager import CandleFeatherManager
from market_api import BinanceMarketApi, BinanceUsdtFutureMarketApi
from util import async_sleep_until_run_time, next_run_time, parse_interval_str, DEFAULT_TZ
from util.time import now_time


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

class TradingUsdtSpotFilter:

    def __init__(self, keep_symbols=None):
        self.keep_symbols = set(keep_symbols) if keep_symbols else None

    @classmethod
    def is_trading_usdt_spot(cls, x):
        '''
        筛选出所有USDT本位的，正在被交易的(TRADING)，现货（Spot）
        '''
        return x['quote_asset'] == 'USDT' and x['status'] == 'TRADING' and not x['base_asset'].endswith(('UP', 'DOWN', 'BEAR', 'BULL'))

    def __call__(self, syminfo: dict) -> list:
        symbols = [info['symbol'] for info in syminfo.values() if self.is_trading_usdt_spot(info)]
        if self.keep_symbols is not None:  # 如有白名单，则只保留白名单内的
            symbols = [sym for sym in symbols if sym in self.keep_symbols]
        return symbols

class Crawler:

    def __init__(self, interval, exginfo_mgr, candle_mgr, market_api, symbol_filter, fetch_funding_rate, num_candles):
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
        self.num_candles = num_candles
        self.exginfo_mgr.clear_all()
        self.candle_mgr.clear_all()

    async def init_history(self):
        '''
        初始化历史阶段 init_history
        '''

        # 1. 通过调用 self.market_api.get_syminfo 获取所有交易的 symbol, 并根据 symbol_filter 过滤出我们想要的 symbol
        syminfo = await self.market_api.get_syminfo()
        all_symbols: list = self.symbol_filter(syminfo)
        limit = self.market_api.WEIGHT_EFFICIENT_ONCE_CANDLES
        cnt = 0
        last_begin_time = dict()
        interval_delta = parse_interval_str(self.interval)

        # 2. 循环分批初始化每个 symbol 历史数据
        while all_symbols:
            # 2.1 获取权重和服务器时间，若使用权重到达临界点，sleep 到下一分钟
            server_time, weight = await self.market_api.get_timestamp_and_weight()
            if weight > self.market_api.MAX_MINUTE_WEIGHT * 0.9:
                await async_sleep_until_run_time(next_run_time('1m'))
                continue

            # 2.2 每批获取还未获取完毕的 80 个 symbol，预计消耗权重 160
            fetch_symbols = all_symbols[:80]
            cnt += 1
            logging.info('Round %d, Server time: %s, Used weight: %d, Symbol num %d, %s - %s', cnt, str(server_time),
                         weight, len(all_symbols), fetch_symbols[0], fetch_symbols[-1])

            tasks = []
            for symbol in fetch_symbols:
                # 默认还没有被获取过
                end_timestamp = None

                # 已经获取过，接着上次比上次已经获取过更旧的 limit 根
                if symbol in last_begin_time:
                    end_timestamp = (last_begin_time[symbol] - interval_delta).value // 1000000

                tasks.append(self.fetch_and_save_history_candle(symbol, end_timestamp))

            results = await asyncio.gather(*tasks)

            # 2.3 更新 symbol 状态
            round_finished_symbols = []
            for symbol, (not_enough, begin_time, num) in zip(fetch_symbols, results):
                last_begin_time[symbol] = begin_time

                # 如果已经获取了足够的 K 线，或 K 线已不足（标的上市时间过短），则不需要继续获取
                if num >= self.num_candles or not_enough:
                    all_symbols.remove(symbol)
                    if num < self.num_candles:
                        logging.warn('%s finished not enough, candle num: %d', symbol, num)
                    else:
                        round_finished_symbols.append(symbol)

            if round_finished_symbols:
                logging.info('%s finished', str(round_finished_symbols))

        server_time, weight = await self.market_api.get_timestamp_and_weight()
        logging.info(f'Init history finished, Server time:, {server_time}, Used weight: {weight}')

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
