import asyncio
import json
import logging
import os
import sys

from msg_sender.dingding import DingDingSender
from util import (async_sleep_until_run_time, convert_interval_to_timedelta, create_aiohttp_session, next_run_time,
                  now_time)

from .handler import BmacHandler

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')


async def init_history(handler: BmacHandler):
    '''
    初始化历史阶段 init_history
    '''

    fetcher = handler.fetcher
    candle_mgr = handler.candle_mgr
    exginfo_mgr = handler.exginfo_mgr
    interval_delta = convert_interval_to_timedelta(handler.interval)
    max_min_weight, once_candles = fetcher.get_api_limits()

    # 0. 清除所有历史数据
    candle_mgr.clear_all()
    exginfo_mgr.clear_all()

    # 1. 通过调用 handler.fetcher.get_exchange_info 获取所有交易的 symbol, 并根据 symbol_filter 过滤出我们想要的 symbol
    exginfo = fetcher.get_exchange_info()
    all_symbols: list = handler.symbol_filter(exginfo)

    cnt = 0
    last_begin_time = dict()

    # 2. 循环分批初始化每个 symbol 历史数据
    while all_symbols:
        # 2.1 获取权重和服务器时间，若使用权重到达临界点，sleep 到下一分钟
        server_time, weight = await fetcher.market_api.aioreq_time_and_weight()
        if weight > max_min_weight * 0.9:
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

            tasks.append(handler.fetch_and_save_history_candle(symbol, end_timestamp))

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

    server_time, weight = await handler.market_api.get_timestamp_and_weight()
    logging.info(f'Init history finished, Server time:, {server_time}, Used weight: {weight}')


async def main(argv):
    #从 argv 中获取根目录
    base_dir = argv[1]

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
                # while True:
                #     msg = await crawler.run_loop()
                #     if msg and msg_sender:
                #         msg['localtime'] = str(now_time())
                #         await msg_sender.send_message(json.dumps(msg, indent=1), 'error')
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


if __name__ == '__main__':
    asyncio.run(main(sys.argv))
