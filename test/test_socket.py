import asyncio
from datetime import datetime

from api.binance.binance_market_ws import (get_coin_futures_kline_socket, get_usdt_futures_kline_socket)
from bmac.filter_symbol import TradingUsdtFuturesFilter
from fetcher import BinanceFetcher
from util import create_aiohttp_session, get_logger

TRADE_TYPE_MAP = {
    'usdt_futures': get_usdt_futures_kline_socket,
    'coin_futures': get_coin_futures_kline_socket,
}


class CandleListener:

    def __init__(self, type_, symbols, time_interval, que):
        self.trade_type = type_
        self.symbols = set(symbols)
        self.time_interval = time_interval
        self.que: asyncio.Queue = que
        self.req_reconnect = False

    async def start_listen(self):
        socket_func = TRADE_TYPE_MAP[self.trade_type]
        while True:
            socket = socket_func(self.symbols, self.time_interval)
            async with socket as socket_conn:
                while True:
                    if self.req_reconnect:
                        self.req_reconnect = False
                        break
                    try:
                        res = await socket_conn.recv()
                    except asyncio.TimeoutError:
                        get_logger().error('Recv candle ws timeout, will reconnect')
                    if 'data' not in res:
                        continue
                    data = res['data']
                    if data.get('e', None) == 'kline' and 'k' in data:
                        candle = data['k']
                        if candle.get('x', False):
                            self.que.put_nowait(candle)

    def add_symbols(self, *symbols):
        for symbol in symbols:
            self.symbols.add(symbol)

    def remove_symbols(self, *symbols):
        for symbol in symbols:
            if symbol in self.symbols:
                self.symbols.remove(symbol)

    def req_reconnect(self):
        self.req_reconnect = True


async def print_candle(que):
    while True:
        try:
            data = await asyncio.wait_for(que.get(), timeout=60)
            print(data, datetime.now())
        except asyncio.TimeoutError:
            print('No candle data received in 60 seconds')


async def test_socket():
    trade_type = 'usdt_futures'
    n_groups = 8
    time_interval = '1m'
    sym_filter = TradingUsdtFuturesFilter()
    async with create_aiohttp_session(3) as session:
        fetcher = BinanceFetcher(trade_type, session)
        exginfo = await fetcher.get_exchange_info()
        symbols = sym_filter(exginfo)

    groups = [[] for i in range(n_groups)]
    for sym in symbols:
        group_id = hash(sym) % n_groups
        groups[group_id].append(sym)

    que = asyncio.Queue()
    for idx, grp in enumerate(groups):
        print('Group', idx, len(grp))
    listeners = [CandleListener(trade_type, syms_grp, time_interval, que) for syms_grp in groups]
    tasks = [l.start_listen() for l in listeners]
    tasks.append(print_candle(que))

    print('Num tasks', len(tasks))

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(test_socket())
