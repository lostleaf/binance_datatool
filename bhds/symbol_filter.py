from abc import ABC, abstractmethod

from constant import ContractType

STABLECOINS = {
    'BKRW', 'USDC', 'USDP', 'TUSD', 'BUSD', 'FDUSD', 'DAI', 'EUR', 'GBP', 'USBP', 'SUSD', 'PAXG', 'AEUR', 'USDS',
    'USDSB'
}


class BaseSymbolFilter(ABC):

    def __call__(self, exginfo: dict) -> list[str]:
        symbols = [info['symbol'] for info in exginfo.values() if self.is_valid(info)]
        return symbols

    @abstractmethod
    def is_valid(self, x):
        pass


class SpotFilter(BaseSymbolFilter):

    def __init__(self, quote_asset, keep_stablecoins, keep_leverage_coins, status=None):
        self.quote_asset = quote_asset
        self.keep_stablecoins = keep_stablecoins
        self.keep_leverage_coins = keep_leverage_coins
        self.status = status

    def is_valid(self, x):

        # Not valid if status mismatch
        if self.status is not None and x['status'] != self.status:
            return False

        # Not valid if quote_asset mismatches
        if x['quote_asset'] != self.quote_asset:
            return False

        # Not valid if is stablecoin and stablecoins are not accepted
        if x['base_asset'] in STABLECOINS and not self.keep_stablecoins:
            return False

        # Not valid if is leverage coin and leverage coins are not accepted
        if x['is_leverage'] and not self.keep_leverage_coins:
            return False

        return True


class UmFuturesFilter(BaseSymbolFilter):

    def __init__(self, quote_asset, contract_type: ContractType, status=None):
        self.quote_asset = quote_asset
        self.contract_type = contract_type
        self.status = status

    def is_valid(self, x):

        # Not valid if status mismatch
        if self.status is not None and x['status'] != self.status:
            return False

        # Not valid if quote_asset mismatches
        if x['quote_asset'] != self.quote_asset:
            return False

        # Not valid if contract_type mismatches
        if x['contract_type'] != self.contract_type:
            return False

        return True


class CmFuturesFilter(BaseSymbolFilter):
    # quote_asset of Coin margined Futures are always USD

    def __init__(self, contract_type: ContractType, status=None):
        self.contract_type = contract_type
        self.status = status

    def is_valid(self, x):
        # Not valid if is not trading
        if self.status is not None and x['status'] != self.status:
            return False

        # Not valid if contract_type mismatches
        if x['contract_type'] != self.contract_type:
            return False

        return True


def create_symbol_filter(filter_name, params) -> BaseSymbolFilter:
    if filter_name not in globals():
        raise ValueError(f'{filter_name} is not supported')
    cls = globals()[filter_name]
    return cls(**params)
