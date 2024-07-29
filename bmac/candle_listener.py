import asyncio

import pandas as pd

from api.binance import (get_coin_futures_multi_candlesticks_socket, get_spot_multi_candlesticks_socket,
                         get_usdt_futures_multi_candlesticks_socket)
from util import convert_interval_to_timedelta, get_logger
from util.time import now_time


def convert_to_dataframe(x, interval_delta):
    """
    解析 WS 返回的数据字典，返回 DataFrame
    """
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

    # 以 K 线结束时间为时间戳
    return pd.DataFrame(data=[candle_data], columns=columns, index=[candle_data[0] + interval_delta])


class CandleListener:

    # 交易类型到 ws 函数映射
    TRADE_TYPE_MAP = {
        'usdt_futures': get_usdt_futures_multi_candlesticks_socket,
        'coin_futures': get_coin_futures_multi_candlesticks_socket,
        'spot': get_spot_multi_candlesticks_socket
    }

    def __init__(self, type_, symbols, time_interval, que):
        # 交易类型
        self.trade_type = type_
        # 交易标的
        self.symbols = set(symbols)
        # K 线周期
        self.time_interval = time_interval
        self.interval_delta = convert_interval_to_timedelta(time_interval)
        # 消息队列
        self.que: asyncio.Queue = que
        # 重链接 flag
        self.req_reconnect = False

    async def start_listen(self):
        """
        WS 监听主函数
        """

        if not self.symbols:
            return

        socket_func = self.TRADE_TYPE_MAP[self.trade_type]
        while True:
            # 创建 WS
            socket = socket_func(self.symbols, self.time_interval)
            async with socket as socket_conn:
                # WS 连接成功后，获取并解析数据
                while True:
                    if self.req_reconnect:  # 如果需要重连，则退出重新连接
                        self.req_reconnect = False
                        break
                    try:
                        res = await socket_conn.recv()
                        self.handle_candle_data(res)
                    except asyncio.TimeoutError:  # 如果长时间未收到数据（默认60秒，正常情况K线每1-2秒推送一次），则退出重新连接
                        get_logger().error('Recv candle ws timeout, reconnecting')
                        break

    def handle_candle_data(self, res):
        """
        处理 WS 返回数据
        """

        # 防御性编程，如果币安出现错误未返回 data 字段，则抛弃
        if 'data' not in res:
            return

        # 取出 data 字段
        data = res['data']

        # 防御性编程，如果 data 中不包含 e 字段或 e 字段（数据类型）不为 kline 或 data 中没有 k 字段（K 线数据），则抛弃
        if data.get('e', None) != 'kline' or 'k' not in data:
            return

        # 取出 k 字段，即 K 线数据
        candle = data['k']

        # 判断 K 线是否闭合，如未闭合则抛弃
        is_closed = candle.get('x', False)
        if not is_closed:
            return

        # 将 K 线转换为 DataFrame
        df_candle = convert_to_dataframe(candle, self.interval_delta)

        # 将 K 线 DataFrame 放入通信队列
        self.que.put_nowait({
            'type': 'candle_data',
            'data': df_candle,
            'closed': is_closed,
            'run_time': df_candle.index[0],
            'symbol': data['s'],
            'time_interval': self.time_interval,
            'trade_type': self.trade_type,
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
