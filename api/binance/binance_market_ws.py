from typing import Optional
from .ws_basics import ReconnectingWebsocket

STREAM_URL = 'wss://stream.binance.com:9443/'
FSTREAM_URL = 'wss://fstream.binance.com/'
DSTREAM_URL = 'wss://dstream.binance.com/'
VSTREAM_URL = 'wss://vstream.binance.com/'


def _get_socket(path: str,
                stream_url: Optional[str] = None,
                prefix: str = 'ws/',
                is_binary: bool = False) -> ReconnectingWebsocket:
    conn = ReconnectingWebsocket(
        path=path,
        url=stream_url,
        prefix=prefix,
        is_binary=is_binary,
    )
    return conn


def get_usdt_futures_socket(path: str, prefix: str = 'stream?streams='):
    stream_url = FSTREAM_URL
    return _get_socket(path, stream_url, prefix)


def get_coin_futures_socket(path: str, prefix: str = 'stream?streams='):
    stream_url = DSTREAM_URL
    return _get_socket(path, stream_url, prefix)


def get_coin_futures_kline_socket(symbols, time_inteval):
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    path = '/'.join(channels)
    return get_coin_futures_socket(path)


def get_usdt_futures_kline_socket(symbols, time_inteval):
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    path = '/'.join(channels)
    return get_usdt_futures_socket(path)
