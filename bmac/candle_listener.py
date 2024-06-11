import asyncio
import logging
import pandas as pd
from api.binance.binance_market_ws import (get_coin_futures_kline_socket, get_usdt_futures_kline_socket)
from util import convert_interval_to_timedelta
from util.time import now_time


def convert_to_dataframe(x, interval_delta):

    columns = [
        'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
    ]
    candle_data = [
        pd.to_datetime(int(x['t']), unit='ms', utc=True),
        float(x['o']),
        float(x['h']),
        float(x['l']),
        float(x['c']),
        float(x['v']),
        float(x['q']),
        float(x['n']),
        float(x['V']),
        float(x['Q'])
    ]

    return pd.DataFrame(data=[candle_data], columns=columns, index=[candle_data[0] + interval_delta])


class CandleListener:
    TRADE_TYPE_MAP = {
        'usdt_futures': get_usdt_futures_kline_socket,
        'coin_futures': get_coin_futures_kline_socket,
    }

    def __init__(self, type_, symbols, time_interval, que):
        self.trade_type = type_
        self.symbols = set(symbols)
        self.time_interval = time_interval
        self.que: asyncio.Queue = que
        self.req_reconnect = False
        self.interval_delta = convert_interval_to_timedelta(time_interval)

    async def start_listen(self):
        if not self.symbols:
            return
        socket_func = self.TRADE_TYPE_MAP[self.trade_type]
        while True:
            socket = socket_func(self.symbols, self.time_interval)
            async with socket as socket_conn:
                while True:
                    if self.req_reconnect:
                        self.req_reconnect = False
                        break
                    try:
                        res = await socket_conn.recv()
                        self.handle_candle_data(res)
                    except asyncio.TimeoutError:
                        logging.error('Recv candle ws timeout, reconnecting')
                        break


    def handle_candle_data(self, res):
        if 'data' not in res:
            return
        data = res['data']

        if data.get('e', None) != 'kline' or 'k' not in data:
            return

        candle = data['k']
        is_closed = candle.get('x', False)
        
        if not is_closed:
            return
        
        df_candle = convert_to_dataframe(candle, self.interval_delta)
        self.que.put_nowait({
            'type': 'candle_data',
            'data': df_candle,
            'closed': is_closed,
            'run_time': df_candle.index[0],
            'symbol': data['s'],
            'recv_time': now_time()
        })

    def add_symbols(self, *symbols):
        for symbol in symbols:
            self.symbols.add(symbol)

    def remove_symbols(self, *symbols):
        for symbol in symbols:
            if symbol in self.symbols:
                self.symbols.remove(symbol)

    def reconnect(self):
        self.req_reconnect = True
