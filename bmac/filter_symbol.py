from util import is_leverage_token, STABLECOINS

class TradingCoinPerpFilter:

    def __init__(self, keep_symbols=None):
        self.keep_symbols = set(keep_symbols) if keep_symbols else None

    @classmethod
    def is_trading_coin_swap(cls, x):
        '''
        筛选出所有币本位的，正在被交易的(TRADING)，永续合约（PERPETUAL）
        '''
        return x['quote_asset'] == 'USD' and x['status'] == 'TRADING' and x['contract_type'] == 'PERPETUAL'

    def __call__(self, syminfo: dict) -> list:
        symbols = [info['symbol'] for info in syminfo.values() if self.is_trading_coin_swap(info)]
        if self.keep_symbols is not None:  # 如有白名单，则只保留白名单内的
            symbols = [sym for sym in symbols if sym in self.keep_symbols]
        return symbols


class TradingUsdtSpotFilter:

    def __init__(self, keep_symbols=None):
        self.keep_symbols = set(keep_symbols) if keep_symbols else None

    @classmethod
    def is_trading_usdt_spot(cls, x):
        '''
        筛选出所有USDT本位的，正在被交易的(TRADING)，现货(Spot)
        '''
        if x['status'] != 'TRADING':
            return False
        if x['quote_asset'] == 'USDT':
            return False
        if is_leverage_token(x['symbol']):
            return False
        if x['symbol'] in STABLECOINS:
            return False
        return True

    def __call__(self, syminfo: dict) -> list:
        symbols = [info['symbol'] for info in syminfo.values() if self.is_trading_usdt_spot(info)]
        if self.keep_symbols is not None:  # 如有白名单，则只保留白名单内的
            symbols = [sym for sym in symbols if sym in self.keep_symbols]
        return symbols


class TradingUsdtPerpFilter:

    def __init__(self, keep_symbols=None):
        self.keep_symbols = set(keep_symbols) if keep_symbols else None

    @classmethod
    def is_trading_usdt_swap(cls, x):
        '''
        筛选出所有USDT本位的，正在被交易的(TRADING)，永续合约（PERPETUAL）
        '''
        return x['quote_asset'] == 'USDT' and x['status'] == 'TRADING' and x['contract_type'] == 'PERPETUAL'

    def __call__(self, syminfo: dict) -> list:
        symbols = [info['symbol'] for info in syminfo.values() if self.is_trading_usdt_swap(info)]
        if self.keep_symbols is not None:  # 如有白名单，则只保留白名单内的
            symbols = [sym for sym in symbols if sym in self.keep_symbols]
        return symbols


def create_symbol_filter(trade_type, keep_symbols):
    if trade_type == 'spot' or trade_type == 'usdt_spot':
        return TradingUsdtSpotFilter(keep_symbols)
    if trade_type == 'usdt_swap' or trade_type == 'usdt_perp':
        return TradingUsdtPerpFilter(keep_symbols)
    if trade_type == 'coin_swap' or trade_type == 'coin_perp':
        return TradingCoinPerpFilter(keep_symbols)
    raise ValueError(f'{trade_type} not supported')
