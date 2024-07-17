import asyncio

import aiohttp
import pandas as pd

from bmac.candle_manager import CandleFileManager
from fetcher import BinanceFetcher
from util import (DEFAULT_TZ, async_sleep_until_run_time, convert_interval_to_timedelta, next_run_time, now_time)
from util.log_kit import divider

from .bmac_util import bmac_init_conns
from .handler import BmacHandler


async def fetch_and_save_history_candle(interval, candle_mgr: CandleFileManager, fetcher: BinanceFetcher, symbol,
                                        num_candles, end_timestamp, run_time):
    max_minute_weight, once_candles = fetcher.get_api_limits()
    if end_timestamp is None:
        candles = await fetcher.get_candle(symbol, interval, limit=once_candles)
    else:
        candles = await fetcher.get_candle(symbol, interval, limit=once_candles, endTime=end_timestamp)
    not_enough = candles.shape[0] < once_candles  # 已经没有足够的 K 线
    candles = candles.loc[:run_time]
    df_candle = candle_mgr.update_candle(symbol, run_time, candles, num_candles)
    begin_time = df_candle['candle_begin_time'].min()
    num = df_candle.shape[0]
    return not_enough, begin_time, num


async def init_history(handler: BmacHandler, session: aiohttp.ClientSession):
    '''
    初始化历史阶段 init_history
    '''
    fetcher, senders = bmac_init_conns(handler, session)
    candle_mgr = handler.candle_mgr
    exginfo_mgr = handler.exginfo_mgr
    interval_delta = convert_interval_to_timedelta(handler.interval)
    max_minute_weight, once_candles = fetcher.get_api_limits()

    run_time = next_run_time(handler.interval) - interval_delta

    # 防止当前时间距 run_time 太近导致 K 线不闭合
    if now_time() - run_time < pd.Timedelta(seconds=30):
        t = 30 - (now_time() - run_time).total_seconds()
        await asyncio.sleep(t)

    # 0. 清除所有历史数据
    handler.logger.info('Candle data dir %s, initializing', candle_mgr.base_dir)
    candle_mgr.clear_all()

    handler.logger.info('Exchange info data dir %s, initializing', exginfo_mgr.base_dir)
    exginfo_mgr.clear_all()

    # 1. 通过调用 fetcher.get_exchange_info 获取所有交易的 symbol, 并根据 symbol_filter 过滤出我们想要的 symbol
    exginfo = await fetcher.get_exchange_info()
    symbols_trading: list = sorted(handler.symbol_filter(exginfo))
    if handler.keep_symbols is not None:
        symbols_trading = [x for x in symbols_trading if x in handler.keep_symbols]

    infos_trading = [info for sym, info in exginfo.items() if sym in symbols_trading]
    df_exginfo = pd.DataFrame.from_records(infos_trading)
    exginfo_mgr.set_candle('exginfo', run_time, df_exginfo)

    cnt = 0
    last_begin_time = dict()

    # 2. 循环分批初始化每个 symbol 历史数据
    while symbols_trading:
        # 2.1 获取权重和服务器时间，若使用权重到达临界点，sleep 到下一分钟
        server_time, weight = await fetcher.get_time_and_weight()
        if weight > max_minute_weight * 0.9:
            await async_sleep_until_run_time(next_run_time('1m'))
            continue

        # 2.2 每批获取还未获取完毕的 80 个 symbol，预计消耗权重 160
        fetch_symbols = symbols_trading[:80]
        cnt += 1

        divider(f'Init history round {cnt}', sep='-', logger_=handler.logger)
        handler.logger.debug(f'Server time: {server_time.tz_convert(DEFAULT_TZ)}, Used weight: {weight}')
        handler.logger.debug(f'Symbol range: {fetch_symbols[0]} -- {fetch_symbols[-1]}')

        tasks = []
        for symbol in fetch_symbols:
            # 默认还没有被获取过
            end_timestamp = None

            # 已经获取过，接着上次比上次已经获取过更旧的 limit 根
            if symbol in last_begin_time:
                end_timestamp = (last_begin_time[symbol] - interval_delta).value // 1000000
            t = fetch_and_save_history_candle(handler.interval, candle_mgr, fetcher, symbol, handler.num_candles,
                                              end_timestamp, run_time)
            tasks.append(t)

        results = await asyncio.gather(*tasks)

        # 2.3 更新 symbol 状态
        round_finished_symbols = []
        for symbol, (not_enough, begin_time, num) in zip(fetch_symbols, results):
            last_begin_time[symbol] = begin_time

            # 如果已经获取了足够的 K 线，或 K 线已不足（标的上市时间过短），则不需要继续获取
            if num >= handler.num_candles or not_enough:
                df = candle_mgr.read_candle(symbol)
                symbols_trading.remove(symbol)
                if num < handler.num_candles:
                    handler.logger.warning('%s finished not enough, candle num: %d', symbol, num)
                else:
                    round_finished_symbols.append(symbol)

        if round_finished_symbols:
            handler.logger.ok(f'{len(round_finished_symbols)} finished, {len(symbols_trading)} left')

    server_time, weight = await fetcher.get_time_and_weight()
    handler.logger.ok(f'History initialized, Server time: {server_time.tz_convert(DEFAULT_TZ)}, Used weight: {weight}')
    return run_time
