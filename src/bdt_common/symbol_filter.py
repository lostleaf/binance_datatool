from abc import ABC, abstractmethod
from typing import Optional

from bdt_common.enums import ContractType
from bdt_common.infer_exginfo import infer_cm_futures_info, infer_spot_info, infer_um_futures_info


class BaseSymbolFilter(ABC):
    @classmethod
    @abstractmethod
    def infer_fn(cls, symbol: str) -> dict:
        pass

    @abstractmethod
    def is_valid(self, info: dict) -> bool:
        pass

    def filter(self, infos: list[str | dict]) -> list[str]:
        filtered_symbols = []
        for info in infos:
            if isinstance(info, str):
                info = self.infer_fn(info)
            if info is None:
                continue
            if self.is_valid(info):
                filtered_symbols.append(info["symbol"])
        return filtered_symbols

    def __call__(self, infos: list[str | dict]) -> list[str]:
        return self.filter(infos)


class SpotFilter(BaseSymbolFilter):
    @classmethod
    def infer_fn(cls, symbol: str) -> dict:
        return infer_spot_info(symbol)

    def __init__(self, quote: Optional[str], stable_pairs: bool, leverage_tokens: bool):
        self.quote = quote
        self.stable_pairs = stable_pairs
        self.leverage_tokens = leverage_tokens

    def is_valid(self, info: dict) -> bool:
        # Not valid if quote_asset mismatches
        if self.quote is not None and info["quote_asset"] != self.quote:
            return False

        # Not valid if is stable pair and stable pairs are not accepted
        if not self.stable_pairs and info["is_stable_pair"]:
            return False

        # Not valid if base asset is leverage coin and leverage coins are not accepted
        if not self.leverage_tokens and info["is_leverage"]:
            return False

        return True


class UmFuturesFilter(BaseSymbolFilter):
    @classmethod
    def infer_fn(cls, symbol: str) -> dict:
        return infer_um_futures_info(symbol)

    def __init__(self, quote: Optional[str], contract_type: Optional[ContractType], stable_pairs: bool):
        self.quote = quote
        self.contract_type = contract_type
        self.stable_pairs = stable_pairs

    def is_valid(self, info: dict) -> bool:
        # Not valid if quote_asset mismatches
        if self.quote is not None and info["quote_asset"] != self.quote:
            return False

        # Not valid if contract_type mismatches
        if self.contract_type is not None and info["contract_type"] != self.contract_type:
            return False

        # Not valid if is stable pair and stable pairs are not accepted
        if not self.stable_pairs and info["is_stable_pair"]:
            return False

        return True


class CmFuturesFilter(BaseSymbolFilter):
    @classmethod
    def infer_fn(cls, symbol: str) -> dict:
        return infer_cm_futures_info(symbol)

    def __init__(self, contract_type: Optional[ContractType]):
        self.contract_type = contract_type

    def is_valid(self, x):
        # Not valid if contract_type mismatches
        if self.contract_type is not None and x["contract_type"] != self.contract_type:
            return False

        return True


def create_symbol_filter_from_config(trade_type, filter_config: dict):
    """
    Create symbol filter based on trade type and filter configuration.

    Args:
        trade_type: TradeType enum value (spot, futures/um, futures/cm)
        filter_config: Dictionary containing filter configuration

    Returns:
        Appropriate symbol filter instance

    Raises:
        ValueError: If trade type is not supported for filtering
    """
    from bdt_common.enums import TradeType  # Import here to avoid circular imports

    quote = filter_config.get("quote")
    contract_type = filter_config.get("contract_type")
    contract_type = ContractType(contract_type) if contract_type else None
    stable_pairs = filter_config.get("stable_pairs", True)
    if trade_type == TradeType.spot:
        return SpotFilter(
            quote=quote,
            stable_pairs=stable_pairs,
            leverage_tokens=filter_config.get("leverage_tokens", False),
        )
    elif trade_type == TradeType.um_futures:
        return UmFuturesFilter(
            quote=quote,
            contract_type=contract_type,
            stable_pairs=stable_pairs,
        )
    elif trade_type == TradeType.cm_futures:
        return CmFuturesFilter(contract_type=contract_type)
    else:
        raise ValueError(f"Unsupported trade type for filtering: {trade_type}")
