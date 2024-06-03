import asyncio
import json
import logging
import os
import sys
import time
import aiohttp
import orjson
import pandas as pd

from fetcher import BinanceFetcher
from msg_sender.dingding import DingDingSender
from util import (DEFAULT_TZ, async_sleep_until_run_time, convert_interval_to_timedelta, create_aiohttp_session,
                  next_run_time, now_time)

from .candle_listener import CandleListener
from .candle_manager import CandleFileManager
from .handler import BmacHandler

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')


def init_conns(handler: BmacHandler,
               session: aiohttp.ClientSession) -> tuple[BinanceFetcher, dict[str, DingDingSender]]:
    senders = dict()
    if handler.dingding is not None:
        for channel_name, dcfg in handler.dingding.items():
            access_token = dcfg['access_token']
            secret = dcfg['secret']
            senders[channel_name] = DingDingSender(session, secret, access_token)
    fetcher = BinanceFetcher(handler.trade_type, session)
    return fetcher, senders


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
    fetcher, senders = init_conns(handler, session)
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
    logging.info('Candle data dir %s, initializing', candle_mgr.base_dir)
    candle_mgr.clear_all()

    logging.info('Exchange info data dir %s, initializing', exginfo_mgr.base_dir)
    exginfo_mgr.clear_all()

    # 1. 通过调用 fetcher.get_exchange_info 获取所有交易的 symbol, 并根据 symbol_filter 过滤出我们想要的 symbol
    exginfo = await fetcher.get_exchange_info()
    symbols_trading: list = handler.symbol_filter(exginfo)

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
        logging.info('Round %d, Server time: %s, Used weight: %d, Symbol num %d, %s - %s', cnt,
                     str(server_time.tz_convert(DEFAULT_TZ)), weight, len(symbols_trading), fetch_symbols[0],
                     fetch_symbols[-1])

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
                    logging.warn('%s finished not enough, candle num: %d', symbol, num)
                else:
                    round_finished_symbols.append(symbol)

        if round_finished_symbols:
            logging.info('%s finished', str(round_finished_symbols))

    server_time, weight = await fetcher.get_time_and_weight()
    logging.info('Init history finished, Server time: %s, Used weight: %d', str(server_time.tz_convert(DEFAULT_TZ)),
                 weight)
    return run_time


async def period_alarm(handler: BmacHandler, main_que: asyncio.Queue):
    while True:
        # 计算出 self.interval 周期下次运行时间 run_time, 并 sleep 到 run_time
        run_time = next_run_time(handler.interval)
        logging.info(f'Next candle update run at {run_time}')
        await async_sleep_until_run_time(run_time)

        req = {'type': 'update_exginfo', 'run_time': run_time}
        await main_que.put(req)
        if handler.fetch_funding_rate:
            await main_que.put({'type': 'update_funding_fee', 'run_time': run_time})

        candle_timeout = pd.Timedelta(seconds=handler.candle_close_timeout_sec)
        while now_time() - run_time < candle_timeout:
            await main_que.put({'type': 'check_candle', 'run_time': run_time, 'report': False})
            await asyncio.sleep(1)

        for i in range(3):
            await main_que.put({'type': 'check_candle', 'run_time': run_time, 'report': True})
            if now_time() - run_time > pd.Timedelta(seconds=40):
                break
            if i < 2:
                await asyncio.sleep(10)


def check_candle(handler: BmacHandler, rest_que: asyncio.Queue, run_time, report):
    candle_mgr = handler.candle_mgr
    exginfo_mgr = handler.exginfo_mgr

    df_exginfo = exginfo_mgr.read_candle('exginfo')
    symbols = df_exginfo['symbol'].tolist()

    not_readys = []
    for symbol in symbols:
        if not candle_mgr.check_ready(symbol, run_time):
            not_readys.append(symbol)

    if not_readys:
        if report:
            logging.warning('Symbols not ready for runtime %s %s', run_time, str(not_readys))
            for symbol in not_readys:
                rest_que.put_nowait({'run_time': run_time, 'symbol': symbol})
        else:
            logging.info('Run time %s, %d/%d symbols ready', run_time, len(symbols) - len(not_readys), len(symbols))
        return False

    logging.info('Run time %s, all symbols ready', run_time)
    return True


def update_candle_data(handler: BmacHandler, df_new: pd.DataFrame, rest_que: asyncio.Queue, symbol, run_time):
    candle_mgr = handler.candle_mgr

    if not candle_mgr.has_symbol(symbol):
        candle_mgr.set_candle(symbol, run_time, df_new)
        return

    df_old = candle_mgr.read_candle(symbol)

    max_old_begin_time = df_old['candle_begin_time'].max()
    min_new_begin_time = df_new['candle_begin_time'].min()
    max_new_begin_time = df_new['candle_begin_time'].max()

    interval_delta = convert_interval_to_timedelta(handler.interval)
    if max_old_begin_time >= max_new_begin_time:
        return

    # 确保能接上
    if min_new_begin_time - max_old_begin_time <= interval_delta:
        candle_mgr.update_candle(symbol, run_time.astimezone(DEFAULT_TZ), df_new, handler.num_candles)
    else:
        rest_que.put_nowait({'run_time': run_time, 'symbol': symbol})


async def dispatcher(handler: BmacHandler, fetcher: BinanceFetcher, senders: dict[str, DingDingSender],
                     listeners: list[CandleListener], main_que: asyncio.Queue, rest_que: asyncio.Queue,
                     last_complete_run_time):
    last = None, None
    while True:
        req = await main_que.get()
        run_time = req['run_time']
        req_type = req['type']
        if req_type == 'update_exginfo':
            logging.info('Update exchange infos %s', run_time)
            await update_exginfo(handler, fetcher, senders, listeners, run_time)
        elif req_type == 'update_funding_fee':
            logging.info('Update funding fees %s', run_time)
            await update_funding_fee(handler, fetcher, run_time)
        elif req_type == 'check_candle':
            if last_complete_run_time >= run_time:
                continue
            all_ready = check_candle(handler, rest_que, run_time, req['report'])
            if all_ready:
                last_complete_run_time = run_time
                logging.info('Last updated %s %s', last[0], last[1])
        elif req_type == 'candle_data':
            update_candle_data(handler, req['data'], rest_que, req['symbol'], run_time)
            last = req['symbol'], now_time()
        else:
            logging.warning('Unknown request %s %s', req_type, run_time)


async def update_exginfo(handler: BmacHandler, fetcher: BinanceFetcher, senders: dict[str, DingDingSender],
                         listeners: list[CandleListener], run_time):
    symbol_filter = handler.symbol_filter
    candle_mgr = handler.candle_mgr
    exginfo_mgr = handler.exginfo_mgr

    # 0. 通过调用 fetcher.get_syminfo 获取 exchange info,
    syminfo = await fetcher.get_exchange_info()

    # 1. 根据 symbol_filter 过滤 symbol
    symbols_trading = symbol_filter(syminfo)
    symbols_last = candle_mgr.get_all_symbols()
    notrading_symbols = set(symbols_last) - set(symbols_trading)
    new_symbols = set(symbols_trading) - set(symbols_last)

    # 2. 保存过滤出的 exginfo
    infos_trading = [info for sym, info in syminfo.items() if sym in symbols_trading]
    df_syminfo = pd.DataFrame.from_records(infos_trading)
    exginfo_mgr.set_candle('exginfo', run_time, df_syminfo)

    changed_groups = set()
    msg = dict()

    # 3. 删除之前有交易，但目前没有交易的 symbol
    if notrading_symbols:
        logging.info(f'Remove not trading symbols {notrading_symbols}')
        msg['not_trading'] = list(notrading_symbols)
        for symbol in notrading_symbols:
            candle_mgr.remove_symbol(symbol)
            group_id = hash(symbol) % handler.num_socket_listeners
            listener: CandleListener = listeners[group_id]
            listener.remove_symbols(symbol)
            changed_groups.add(group_id)

    # 4. 添加新上市的 symbol
    if new_symbols:
        logging.info('Add listening new symbols %s', str(new_symbols))
        msg['new_symbols'] = list(new_symbols)
        for symbol in new_symbols:
            group_id = hash(symbol) % handler.num_socket_listeners
            listener: CandleListener = listeners[group_id]
            listener.add_symbols(symbol)
            changed_groups.add(group_id)

    # 5. Listener 重启重新订阅
    for group_id in changed_groups:
        listener: CandleListener = listeners[group_id]
        listener.req_reconnect()

    # 6. 最后对停止交易的和新的 symbol 会发送钉钉警告
    if len(msg):
        try:
            senders['error'].send_message(msg)
        except Exception as err:
            logging.error('Failed send message with error %s', str(err))


async def update_funding_fee(handler: BmacHandler, fetcher: BinanceFetcher, run_time):
    exginfo_mgr = handler.exginfo_mgr
    # 获取资金费率
    df_funding = await fetcher.get_funding_rate()
    df_funding['time'] = run_time
    if exginfo_mgr.has_symbol('funding'):
        interval_delta = convert_interval_to_timedelta(handler.interval)
        df_funding_old = exginfo_mgr.read_candle('funding')
        df_funding = pd.concat([df_funding_old, df_funding])
        min_time = run_time - interval_delta * handler.num_candles
        df_funding = df_funding[df_funding['time'] >= min_time]
    exginfo_mgr.set_candle('funding', run_time, df_funding)


async def fetch_recent_closed_candle(handler: BmacHandler, fetcher: BinanceFetcher, symbol, run_time):
    '''
    获取 run_time 周期闭合K线，原理为反复获取K线，直到K线闭合或超时
    返回值为 tuple(K线df, 是否闭合布尔值)
    '''
    expire_sec = handler.candle_close_timeout_sec
    interval = handler.interval
    is_closed = False
    while True:
        df = await fetcher.get_candle(symbol, interval, limit=10)

        if df['candle_begin_time'].max() >= run_time:
            is_closed = True
            break

        if now_time() - run_time > pd.Timedelta(seconds=expire_sec):
            # logging.warning(f'Candle may not closed in {expire_sec}sec {symbol} {interval}')
            break

        await asyncio.sleep(1)
    df_new = df[df['candle_begin_time'] < run_time]
    return df_new, is_closed


async def restful_candle_fetcher(handler: BmacHandler, fetcher: BinanceFetcher, main_que: asyncio.Queue,
                                 rest_que: asyncio.Queue):
    while True:
        req = await rest_que.get()
        run_time = req['run_time']
        symbol = req['symbol']
        logging.warning('Fetch candle with restful API %s %s', symbol, run_time)
        df_new, is_closed = await fetch_recent_closed_candle(handler, fetcher, symbol, run_time)
        await main_que.put({
            'type': 'candle_data',
            'data': df_new,
            'closed': is_closed,
            'run_time': run_time,
            'symbol': symbol,
            'recv_time': now_time()
        })


def create_listeners(trade_type, time_interval, symbols, n_groups, main_que) -> list[CandleListener]:
    groups = [[] for i in range(n_groups)]
    for sym in symbols:
        group_id = hash(sym) % n_groups
        groups[group_id].append(sym)

    for idx, grp in enumerate(groups):
        num = len(grp)
        if num > 0:
            logging.info('Listen group %d, %d symbols', idx, num)
    listeners = [CandleListener(trade_type, syms_grp, time_interval, main_que) for syms_grp in groups]
    return listeners


async def update_candle(handler: BmacHandler, session: aiohttp.ClientSession, last_complete_run_time):
    '''
    定时获取 K线 update_candle_period
    '''
    fetcher, senders = init_conns(handler, session)

    main_que = asyncio.Queue()
    rest_que = asyncio.Queue()

    rest_fetchers = [
        restful_candle_fetcher(handler, fetcher, main_que, rest_que) for _ in range(handler.num_rest_fetchers)
    ]

    symbols = handler.candle_mgr.get_all_symbols()
    listeners = create_listeners(handler.trade_type, handler.interval, symbols, handler.num_socket_listeners, main_que)
    listen_tasks = [l.start_listen() for l in listeners]

    alarm_task = period_alarm(handler, main_que)
    dispatcher_task = dispatcher(handler, fetcher, senders, listeners, main_que, rest_que, last_complete_run_time)

    tasks = rest_fetchers + [alarm_task, dispatcher_task] + listen_tasks
    await asyncio.gather(*tasks)


async def main(base_dir):
    # 读取 config.json，获取配置
    cfg = json.load(open(os.path.join(base_dir, 'config.json')))

    handler = BmacHandler(base_dir, cfg)

    logging.info('interval=%s, type=%s, funding_rate=%r, keep_symbols=%r', handler.interval, handler.trade_type,
                 handler.fetch_funding_rate, handler.keep_symbols)

    while True:
        try:
            async with create_aiohttp_session(handler.http_timeout_sec) as session:

                last_complete_run_time = await init_history(handler, session)
                await update_candle(handler, session, last_complete_run_time)
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
