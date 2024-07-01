import os

from .candle_manager import CandleFileManager
from .filter_symbol import create_symbol_filter


class BmacHandler:

    def __init__(self, base_dir, cfg: dict):
        # 根目录
        self.base_dir = base_dir

        # 必要参数

        # K 线周期
        self.interval = cfg['interval']
        # 标的类型，可以是 'spot'/'usdt_spot', 'usdt_perp'/'usdt_swap', 'coin_perp'/'coin_swap'
        self.trade_type = cfg['trade_type']

        # 可选参数

        # 保留 K 线数量, 默认1500
        self.num_candles = cfg.get('num_candles', 1500)
        # 是否获取资金费率，默认否
        self.fetch_funding_rate = cfg.get('funding_rate', False)
        # http 超时时间，默认 5 秒
        self.http_timeout_sec = int(cfg.get('http_timeout_sec', 5))
        # K 线闭合超时时间，默认 15 秒
        self.candle_close_timeout_sec = int(cfg.get('candle_close_timeout_sec', 15))
        # symbol 白名单，如有则只获取白名单内的 symbol，默认无
        self.keep_symbols = cfg.get('keep_symbols', None)
        # K 线数据存储格式，默认 parquet，也可为 feather
        save_type = cfg.get('save_type', 'parquet')
        # 钉钉配置，默认无
        self.dingding = cfg.get('dingding', None)
        # rest fetcher 数量
        self.num_rest_fetchers = cfg.get('num_rest_fetchers', 8)
        # websocket listener 数量
        self.num_socket_listeners = cfg.get('num_socket_listeners', 8)

        # symbol_filter: 用于过滤 symbol 的仿函数
        self.symbol_filter = create_symbol_filter(self.trade_type, self.keep_symbols)

        # candle_mgr: 用于管理 K 线数据的 CandleFileManager
        candle_dir = os.path.join(base_dir, f'candle_{self.interval}')
        self.candle_mgr = CandleFileManager(candle_dir, save_type)

        # exginfo_mgr: 用于管理 exchange info(合约交易规则)的 CandleFileManager
        exginfo_dir = os.path.join(base_dir, f'exginfo_{self.interval}')
        self.exginfo_mgr = CandleFileManager(exginfo_dir, save_type)

        self.trade_type = normalize_trade_type(self.trade_type)


def normalize_trade_type(ty):
    if ty == 'usdt_spot':
        return 'spot'
    if ty in ('coin_perp', 'coin_swap'):
        return 'coin_futures'
    if ty in ('usdt_perp', 'usdt_swap'):
        return 'usdt_futures'
