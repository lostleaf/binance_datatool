import asyncio
import json
import logging
import os
import sys

import pandas as pd

from msg_sender.dingding import DingDingSender
from util import (async_sleep_until_run_time, convert_interval_to_timedelta, create_aiohttp_session, next_run_time,
                  now_time, DEFAULT_TZ)

from .handler import BmacHandler

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')


async def fetch_and_save_history_candle(handler: BmacHandler, symbol, end_timestamp):
    fetcher = handler.fetcher
    max_minute_weight, once_candles = fetcher.get_api_limits()
    if end_timestamp is None:
        candles = await fetcher.get_candle(symbol, handler.interval, limit=once_candles)
    else:
        candles = await fetcher.get_candle(symbol, handler.interval, limit=once_candles, endTime=end_timestamp)
    not_enough = candles.shape[0] < once_candles  # 已经没有足够的 K 线
    df_candle = handler.candle_mgr.update_candle(symbol, now_time(), candles, handler.num_candles)
    begin_time = df_candle['candle_begin_time'].min()
    num = df_candle.shape[0]
    return not_enough, begin_time, num


async def init_history(handler: BmacHandler):
    '''
    初始化历史阶段 init_history
    '''

    fetcher = handler.fetcher
    candle_mgr = handler.candle_mgr
    exginfo_mgr = handler.exginfo_mgr
    interval_delta = convert_interval_to_timedelta(handler.interval)
    max_minute_weight, once_candles = fetcher.get_api_limits()

    # 0. 清除所有历史数据
    logging.info('Candle data dir %s, deleting outdated data', candle_mgr.base_dir)
    candle_mgr.clear_all()

    logging.info('Exchange info data dir %s, deleting outdated data', exginfo_mgr.base_dir)
    exginfo_mgr.clear_all()

    # 1. 通过调用 handler.fetcher.get_exchange_info 获取所有交易的 symbol, 并根据 symbol_filter 过滤出我们想要的 symbol
    exginfo = await fetcher.get_exchange_info()
    all_symbols: list = handler.symbol_filter(exginfo)

    cnt = 0
    last_begin_time = dict()

    # 2. 循环分批初始化每个 symbol 历史数据
    while all_symbols:
        # 2.1 获取权重和服务器时间，若使用权重到达临界点，sleep 到下一分钟
        server_time, weight = await fetcher.get_time_and_weight()
        if weight > max_minute_weight * 0.9:
            await async_sleep_until_run_time(next_run_time('1m'))
            continue

        # 2.2 每批获取还未获取完毕的 80 个 symbol，预计消耗权重 160
        fetch_symbols = all_symbols[:80]
        cnt += 1
        logging.info('Round %d, Server time: %s, Used weight: %d, Symbol num %d, %s - %s', cnt,
                     str(server_time.tz_convert(DEFAULT_TZ)), weight, len(all_symbols), fetch_symbols[0],
                     fetch_symbols[-1])

        tasks = []
        for symbol in fetch_symbols:
            # 默认还没有被获取过
            end_timestamp = None

            # 已经获取过，接着上次比上次已经获取过更旧的 limit 根
            if symbol in last_begin_time:
                end_timestamp = (last_begin_time[symbol] - interval_delta).value // 1000000

            tasks.append(fetch_and_save_history_candle(handler, symbol, end_timestamp))

        results = await asyncio.gather(*tasks)

        # 2.3 更新 symbol 状态
        round_finished_symbols = []
        for symbol, (not_enough, begin_time, num) in zip(fetch_symbols, results):
            last_begin_time[symbol] = begin_time

            # 如果已经获取了足够的 K 线，或 K 线已不足（标的上市时间过短），则不需要继续获取
            if num >= handler.num_candles or not_enough:
                all_symbols.remove(symbol)
                if num < handler.num_candles:
                    logging.warn('%s finished not enough, candle num: %d', symbol, num)
                else:
                    round_finished_symbols.append(symbol)

        if round_finished_symbols:
            logging.info('%s finished', str(round_finished_symbols))

    server_time, weight = await fetcher.get_time_and_weight()
    logging.info('Init history finished, Server time: %s, Used weight: %d', str(server_time.tz_convert(DEFAULT_TZ)),
                 weight)


async def fetch_and_save_recent_closed_candle(handler: BmacHandler, symbol, run_time):
    '''
    获取 run_time 周期闭合K线，原理为反复获取K线，直到K线闭合或超时
    返回值为 tuple(K线df, 是否闭合布尔值)
    '''
    expire_sec = handler.candle_close_timeout_sec
    fetcher = handler.fetcher
    interval = handler.interval
    num_candles = handler.num_candles
    candle_mgr = handler.candle_mgr
    is_closed = False
    while True:
        df = await fetcher.get_candle(symbol, interval, limit=5)

        if df['candle_begin_time'].max() >= run_time:
            is_closed = True
            break

        if now_time() - run_time > pd.Timedelta(seconds=expire_sec):
            # logging.warning(f'Candle may not closed in {expire_sec}sec {symbol} {interval}')
            break

        await asyncio.sleep(1)
    df_new = df[df['candle_begin_time'] < run_time]

    candle_mgr.update_candle(symbol, run_time, df_new, num_candles)
    return is_closed


async def update_candle_period(handler: BmacHandler):
    '''
    定时获取 K线 update_candle_period
    '''
    interval = handler.interval
    interval_delta = convert_interval_to_timedelta(interval)
    fetcher = handler.fetcher
    exginfo_mgr = handler.exginfo_mgr
    candle_mgr = handler.candle_mgr
    symbol_filter = handler.symbol_filter
    is_fetch_funding_rate = handler.fetch_funding_rate
    max_minute_weight, once_candles = fetcher.get_api_limits()

    # 1. 计算出 self.interval 周期下次运行时间 run_time, 并 sleep 到 run_time
    run_time = next_run_time(interval)
    msg = dict()
    logging.info(f'Next candle update run at {run_time}')
    await async_sleep_until_run_time(run_time)

    # 2. 通过调用 self.market_api.get_syminfo 获取所有交易的 symbol 及交易规则, 并根据 symbol_filter 过滤出我们想要的 symbol
    syminfo = await fetcher.get_exchange_info()
    symbols_trading = symbol_filter(syminfo)
    symbols_last = candle_mgr.get_all_symbols()
    notradings = set(symbols_last) - set(symbols_trading)

    # 3. 删除之前有交易，但目前没有交易的 symbol（可能可以防止 BNXUSDT 拆分之类的事件），这些停止交易的 symbol 会发送钉钉警告
    infos_trading = [info for sym, info in syminfo.items() if sym in symbols_trading]
    df_syminfo = pd.DataFrame.from_records(infos_trading)
    exginfo_mgr.set_candle('exginfo', run_time, df_syminfo)

    if notradings:
        logging.info(f'Remove not trading symbols {notradings}')
        msg['not_trading'] = list(notradings)
        for symbol in notradings:
            candle_mgr.remove_symbol(symbol)

    # 4. 获取资金费率（如果需要）
    if is_fetch_funding_rate:
        df_funding = await fetcher.get_funding_rate()
        df_funding['time'] = run_time
        if exginfo_mgr.has_symbol('funding'):
            interval_delta = convert_interval_to_timedelta(interval)
            df_funding_old = exginfo_mgr.read_candle('funding')
            df_funding = pd.concat([df_funding_old, df_funding])
            min_time = run_time - interval_delta * handler.num_candles
            df_funding = df_funding[df_funding['time'] >= min_time]
        exginfo_mgr.set_candle('funding', run_time, df_funding)

    # 5. 对所有这在交易的 symbol, 调用 self.market_api.fetch_recent_closed_candle 获取最近 5 根 K线
    # 将获取的 K线通过 self.candle_mgr.update_candle 写入 feather，并更新 ready file，未闭合 K线也会被写入，并发送钉钉警告
    tasks = []
    for symbol in symbols_trading:
        tasks.append(fetch_and_save_recent_closed_candle(handler, symbol, run_time))

    is_closed_list = await asyncio.gather(*tasks)
    may_not_closed = []
    for symbol, is_closed in zip(symbols_trading, is_closed_list):
        if not is_closed:
            may_not_closed.append(symbol)

    if may_not_closed:
        logging.warning(f'Candle may not closed: {may_not_closed}')
        msg['not_closed'] = list(may_not_closed)

    # 6. 获取并打印本轮运行结束服务器时间以及消耗的权重
    server_time, weight = await fetcher.get_time_and_weight()
    num_symbols = len(candle_mgr.get_all_symbols())
    logging.info('Saved symbols: %d Server time: %s, used weight: %d', num_symbols, server_time.tz_convert(DEFAULT_TZ),
                 weight)
    return msg


async def main(base_dir):
    # 读取 config.json，获取配置
    cfg = json.load(open(os.path.join(base_dir, 'config.json')))

    handler = BmacHandler(base_dir, cfg)

    logging.info('interval=%s, type=%s, funding_rate=%r, keep_symbols=%r', handler.interval, handler.trade_type,
                 handler.fetch_funding_rate, handler.keep_symbols)

    while True:
        try:
            async with create_aiohttp_session(handler.http_timeout_sec) as session:
                # 实例化所有涉及的网络连接
                handler.init_conns(session)

                await init_history(handler)

                # 无限循环，每周期获取最新K线
                while True:
                    msg = await update_candle_period(handler)
                    if msg and handler.senders and 'error' in handler.senders:
                        msg['localtime'] = str(now_time())
                        sender = handler.senders['error']
                        await sender.send_message(json.dumps(msg, indent=1), 'error')
        except Exception as e:
            await report_error(handler, e)
            await asyncio.sleep(10)


async def report_error(handler: BmacHandler, e: Exception):
    # 出错则通过钉钉报错
    logging.error(f'An error occurred {str(e)}')
    import traceback
    traceback.print_exc()
    if handler.dingding is not None and 'error' in handler.dingding:
        dingding_err = handler.dingding['error']
        try:
            error_stack_str = traceback.format_exc()
            async with create_aiohttp_session(handler.http_timeout_sec) as session:
                msg_sender = DingDingSender(session, dingding_err['secret'], dingding_err['access_token'])
                msg = f'An error occurred {str(e)}\n' + error_stack_str
                await msg_sender.send_message(msg, 'error')
        except:
            pass
