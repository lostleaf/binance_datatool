from abc import ABC, abstractmethod

from util import STABLECOINS


class BaseSymbolFilter(ABC):

    def __call__(self, syminfo: dict) -> list:
        symbols = [info['symbol'] for info in syminfo.values() if self.is_valid(info)]
        return symbols

    @abstractmethod
    def is_valid(self, x):
        pass


class TradingSpotFilter(BaseSymbolFilter):

    def __init__(self, quote_asset, keep_stablecoins):
        self.quote_asset = quote_asset
        self.keep_stablecoins = keep_stablecoins

    def is_valid(self, x):

        # Not valid if is not trading
        if x['status'] != 'TRADING':
            return False

        # Not valid if quote_asset mismatches
        if x['quote_asset'] != self.quote_asset:
            return False

        # Not valid if is stablecoin and stablecoins are not accepted
        if x['symbol'] in STABLECOINS and not self.keep_stablecoins:
            return False

        return True


class TradingUsdtFuturesFilter(BaseSymbolFilter):

    def __init__(self, quote_asset, types):
        self.quote_asset = quote_asset

        self.contract_types = types
        if isinstance(types, str):
            self.contract_types = {types}

    def is_valid(self, x):

        # Not valid if is not trading
        if x['status'] != 'TRADING':
            return False

        # Not valid if quote_asset mismatches
        if x['quote_asset'] != self.quote_asset:
            return False

        # Not valid if contract_type mismatches
        if x['contract_type'] not in self.contract_types:
            return False

        return True


class TradingCoinFuturesFilter(BaseSymbolFilter):
    # quote_asset of Coin margined Futures are always USD

    def __init__(self, types):
        self.contract_types = types
        if isinstance(types, str):
            self.contract_types = {types}

    def is_valid(self, x):

        # Not valid if is not trading
        if x['status'] != 'TRADING':
            return False

        # Not valid if contract_type mismatches
        if x['contract_type'] not in self.contract_types:
            return False

        return True
