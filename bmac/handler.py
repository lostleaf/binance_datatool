import os

import aiohttp

from fetcher import BinanceFetcher
from msg_sender import DingDingSender

from .candle_manager import CandleFileManager
from .filter_symbol import create_symbol_filter


class BmacHandler:

    def __init__(self, base_dir, cfg: dict):
        '''
        interval: K线周期
        exginfo_mgr: 用于管理 exchange info(合约交易规则)的 CandleFileManager
        candle_mgr: 用于管理 K线的 CandleFeatherManager
        market_api: BinanceMarketApi 的子类，用于请求币本位或 USDT 本位合约公有 API
        symbol_filter: 用于过滤出 symbol

        初始化阶段，exginfo_mgr 和 candle_mgr，会清空历史数据并建立数据目录
        '''
        self.base_dir = base_dir
        self.interval = cfg['interval']
        self.http_timeout_sec = int(cfg['http_timeout_sec'])
        self.candle_close_timeout_sec = int(cfg['candle_close_timeout_sec'])
        self.trade_type = cfg['trade_type']
        self.fetch_funding_rate = cfg.get('funding_rate', False)
        self.num_candles = cfg['num_candles']

        self.keep_symbols = cfg.get('keep_symbols', None)
        self.symbol_filter = create_symbol_filter(self.trade_type, self.keep_symbols)

        save_type = cfg.get('save_type', 'parquet')
        candle_dir = os.path.join(base_dir, f'{self.trade_type}_{self.interval}')
        self.candle_mgr = CandleFileManager(candle_dir, save_type)
        exginfo_dir = os.path.join(base_dir, f'exginfo_{self.interval}')
        self.exginfo_mgr = CandleFileManager(exginfo_dir, save_type)

        self.dingding = cfg.get('dingding', None)
        self.senders = None
        self.fetcher: BinanceFetcher = None

    def init_conns(self, session: aiohttp.ClientSession):
        self.senders = dict()
        if self.dingding is not None:
            for channel_name, dcfg in self.dingding.items():
                access_token = dcfg['access_token']
                secret = dcfg['secret']
                self.senders[channel_name] = DingDingSender(session, secret, access_token)
        self.fetcher = BinanceFetcher(self.trade_type, session)
