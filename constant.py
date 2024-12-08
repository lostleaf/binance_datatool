from enum import Enum


class TradeType(str, Enum):
    spot = 'spot'
    um_futures = 'um_futures'
    cm_futures = 'cm_futures'


class ContractType(str, Enum):
    perpetual = 'PERPETUAL'
    delivery = 'DELIVERY'
