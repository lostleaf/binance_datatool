import os

from util import get_logger

from .candle_manager import CandleFileManager
from .filter_symbol import TradingSpotFilter, TradingCoinFuturesFilter, TradingUsdtFuturesFilter

# 交割合约包含 CURRENT_QUARTER(当季合约), NEXT_QUARTER(次季合约)
DELIVERY_TYPES = ['CURRENT_QUARTER', 'NEXT_QUARTER']

TRADE_TYPE_MAP = {
    # spot
    'usdt_spot': (TradingSpotFilter(quote_asset='USDT', keep_stablecoins=False), 'spot'),
    'usdc_spot': (TradingSpotFilter(quote_asset='USDC', keep_stablecoins=False), 'spot'),
    'btc_spot': (TradingSpotFilter(quote_asset='BTC', keep_stablecoins=False), 'spot'),

    # usdt_futures
    'usdt_perp': (TradingUsdtFuturesFilter(quote_asset='USDT', types=['PERPETUAL']), 'usdt_futures'),
    'usdt_deli': (TradingUsdtFuturesFilter(quote_asset='USDT', types=DELIVERY_TYPES), 'usdt_futures'),
    'usdc_perp': (TradingUsdtFuturesFilter(quote_asset='USDC', types=['PERPETUAL']), 'usdt_futures'),

    # 仅包含 ETHBTC 永续合约，属于 U 本位合约
    'btc_perp': (TradingUsdtFuturesFilter(quote_asset='BTC', types=['PERPETUAL']), 'usdt_futures'),

    # 兼容 V1
    'usdt_swap': (TradingUsdtFuturesFilter(quote_asset='USDT', types=['PERPETUAL']), 'usdt_futures'),

    # coin_futures
    'coin_perp': (TradingCoinFuturesFilter(types=['PERPETUAL']), 'coin_futures'),
    'coin_deli': (TradingCoinFuturesFilter(types=DELIVERY_TYPES), 'coin_futures'),

    # 兼容 V1
    'coin_swap': (TradingCoinFuturesFilter(types=['PERPETUAL']), 'coin_futures'),
}

# 本地最多保留的 K 线数量
NUM_CANDLES_MAX_LIMIT = 10000


class BmacHandler:

    def __init__(self, base_dir, cfg: dict):
        # 根目录
        self.base_dir = base_dir

        # 必要参数

        # K 线周期
        self.interval = cfg['interval']
        # 标的类型
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

        if self.trade_type not in TRADE_TYPE_MAP:
            raise ValueError(f'Trade type {self.trade_type} currently not supported')

        if self.num_candles > NUM_CANDLES_MAX_LIMIT:
            raise ValueError(f'num_candles {self.num_candles} exceeds max limit of {NUM_CANDLES_MAX_LIMIT}')

        # symbol_filter: 用于过滤 symbol 的仿函数
        # api_trade_type: API 类型
        self.symbol_filter, self.api_trade_type = TRADE_TYPE_MAP[self.trade_type]

        # candle_mgr: 用于管理 K 线数据的 CandleFileManager
        candle_dir = os.path.join(base_dir, f'{self.trade_type}_{self.interval}')
        self.candle_mgr = CandleFileManager(candle_dir, save_type)

        # exginfo_mgr: 用于管理 exchange info(合约交易规则)的 CandleFileManager
        exginfo_dir = os.path.join(base_dir, f'exginfo_{self.interval}')
        self.exginfo_mgr = CandleFileManager(exginfo_dir, save_type)

        self.logger = get_logger('Bmac')

        if self.keep_symbols is not None:
            self.keep_symbols = set(self.keep_symbols)

        if self.api_trade_type == 'spot' and self.fetch_funding_rate:
            self.fetch_funding_rate = False
            self.logger.warning('Cannot fetch funding rate for spot, set fetch_funding_rate=False')