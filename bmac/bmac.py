import asyncio
import json
import os

import aiohttp
import pandas as pd

from fetcher import BinanceFetcher
from msg_sender.dingding import DingDingSender
from util import (DEFAULT_TZ, async_sleep_until_run_time, convert_interval_to_timedelta, create_aiohttp_session,
                  next_run_time, now_time)
from util.log_kit import divider

from .bmac_util import bmac_init_conns, report_error
from .candle_listener import CandleListener
from .handler import BmacHandler
from .init_history import init_history


async def period_alarm(handler: BmacHandler, main_que: asyncio.Queue):
    while True:
        # 计算出 self.interval 周期下次运行时间 run_time, 并 sleep 到 run_time
        run_time = next_run_time(handler.interval)
        divider(f'Bmac {handler.interval} {handler.trade_type} update Runtime={run_time}',
                logger_=handler.logger,
                display_time=False)
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
            handler.logger.warning('Symbols not ready for runtime %s %s', run_time, str(not_readys))
            for symbol in not_readys:
                rest_que.put_nowait({'run_time': run_time, 'symbol': symbol})
        else:
            n_symbols = len(symbols)
            n_readys = n_symbols - len(not_readys)
            handler.logger.debug(f'{now_time()}, {n_readys}/{n_symbols} symbols ready')
        return False

    handler.logger.ok(f'{now_time()}, all symbols ready')
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
            handler.logger.ok(f'{now_time()}, Exchange infos updated')
            await update_exginfo(handler, fetcher, senders, listeners, run_time)
        elif req_type == 'update_funding_fee':
            handler.logger.ok(f'{now_time()}, Funding fees updated')
            await update_funding_fee(handler, fetcher, run_time)
        elif req_type == 'check_candle':
            if last_complete_run_time >= run_time:
                continue
            all_ready = check_candle(handler, rest_que, run_time, req['report'])
            if all_ready:
                last_complete_run_time = run_time
                handler.logger.info('Last updated %s %s', last[0], last[1])
        elif req_type == 'candle_data':
            update_candle_data(handler, req['data'], rest_que, req['symbol'], run_time)
            last = req['symbol'], now_time()
        else:
            handler.logger.warning('Unknown request %s %s', req_type, run_time)


async def update_exginfo(handler: BmacHandler, fetcher: BinanceFetcher, senders: dict[str, DingDingSender],
                         listeners: list[CandleListener], run_time):
    candle_mgr = handler.candle_mgr
    exginfo_mgr = handler.exginfo_mgr

    # 0. 通过调用 fetcher.get_syminfo 获取 exchange info,
    syminfo = await fetcher.get_exchange_info()

    # 1. 根据 symbol_filter 过滤 symbol
    symbols_trading = handler.symbol_filter(syminfo)
    if handler.keep_symbols is not None:
        symbols_trading = [x for x in symbols_trading if x in handler.keep_symbols]
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
        handler.logger.info(f'Remove not trading symbols {notrading_symbols}')
        msg['not_trading'] = list(notrading_symbols)
        for symbol in notrading_symbols:
            candle_mgr.remove_symbol(symbol)
            group_id = hash(symbol) % handler.num_socket_listeners
            listener: CandleListener = listeners[group_id]
            listener.remove_symbols(symbol)
            changed_groups.add(group_id)

    # 4. 添加新上市的 symbol
    if new_symbols:
        handler.logger.info('Add listening new symbols %s', str(new_symbols))
        msg['new_symbols'] = list(new_symbols)
        for symbol in new_symbols:
            group_id = hash(symbol) % handler.num_socket_listeners
            listener: CandleListener = listeners[group_id]
            listener.add_symbols(symbol)
            changed_groups.add(group_id)

    # 5. Listener 重启重新订阅
    for group_id in changed_groups:
        listener: CandleListener = listeners[group_id]
        listener.reconnect()

    # 6. 最后对停止交易的和新的 symbol 会发送钉钉警告
    if len(msg):
        try:
            senders['error'].send_message(msg)
        except Exception as err:
            handler.logger.error('Failed send message with error %s', str(err))


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
        try:
            df = await fetcher.get_candle(symbol, interval, limit=99)

            if df['candle_begin_time'].max() >= run_time:
                is_closed = True
                break
        except BinanceAPIException as e:
            if e.code in err_filter_dict:
                break

        if now_time() - run_time > pd.Timedelta(seconds=expire_sec):
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
        handler.logger.warning(f'{now_time()} Fetch candle with restful API {symbol}')
        df_new, is_closed = await fetch_recent_closed_candle(handler, fetcher, symbol, run_time)
        await main_que.put({
            'type': 'candle_data',
            'data': df_new,
            'closed': is_closed,
            'run_time': run_time,
            'symbol': symbol,
            'recv_time': now_time()
        })


def create_listeners(handler: BmacHandler, symbols, main_que) -> list[CandleListener]:
    trade_type = handler.api_trade_type
    time_interval = handler.interval
    n_listeners = handler.num_socket_listeners

    groups = [[] for i in range(n_listeners)]
    for sym in symbols:
        group_id = hash(sym) % n_listeners
        groups[group_id].append(sym)

    for idx, grp in enumerate(groups):
        num = len(grp)
        if num > 0:
            handler.logger.debug(f'Create WS listen group {idx}, {num} symbols')
    listeners = [CandleListener(trade_type, syms_grp, time_interval, main_que) for syms_grp in groups]
    return listeners


async def update_candle(handler: BmacHandler, session: aiohttp.ClientSession, last_complete_run_time):
    '''
    定时获取 K线 update_candle_period
    '''
    fetcher, senders = bmac_init_conns(handler, session)

    main_que = asyncio.Queue()
    rest_que = asyncio.Queue()

    rest_fetchers = [
        restful_candle_fetcher(handler, fetcher, main_que, rest_que) for _ in range(handler.num_rest_fetchers)
    ]

    symbols = handler.candle_mgr.get_all_symbols()
    listeners = create_listeners(handler, symbols, main_que)
    listen_tasks = [l.start_listen() for l in listeners]

    alarm_task = period_alarm(handler, main_que)
    dispatcher_task = dispatcher(handler, fetcher, senders, listeners, main_que, rest_que, last_complete_run_time)

    tasks = rest_fetchers + [alarm_task, dispatcher_task] + listen_tasks
    await asyncio.gather(*tasks)


async def main(base_dir):
    # 阶段1：读取配置
    cfg = json.load(open(os.path.join(base_dir, 'config.json')))

    # 将配置保存在 BmacHandler 中
    handler = BmacHandler(base_dir, cfg)

    divider('Start Bmac V2', logger_=handler.logger)

    # 输出核心配置
    handler.logger.info('interval=%s, type=%s, num_candles=%r, funding_rate=%r, keep_symbols=%r', handler.interval,
                        handler.trade_type, handler.num_candles, handler.fetch_funding_rate, handler.keep_symbols)

    while True:
        try:
            async with create_aiohttp_session(handler.http_timeout_sec) as session:
                # 阶段2: 初始化历史数据
                last_complete_run_time = await init_history(handler, session)

                # 阶段3: 更新数据
                await update_candle(handler, session, last_complete_run_time)
        except Exception as e:
            await report_error(handler, e)
            divider('Restart after 1 minute', logger_=handler.logger)
            await asyncio.sleep(60)
