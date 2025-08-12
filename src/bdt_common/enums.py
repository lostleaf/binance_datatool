from enum import Enum


class TradeType(str, Enum):
    """
    Market segment on Binance.
    """

    # Spot market trades and data
    spot = "spot"
    # USDT-margined futures (perpetual or delivery)
    um_futures = "futures/um"
    # COIN-margined futures (perpetual or delivery)
    cm_futures = "futures/cm"


class ContractType(str, Enum):
    """
    Futures contract styles on Binance Futures.
    """

    # Perpetual contracts
    perpetual = "PERPETUAL"
    # Delivery/dated futures (expires on a fixed date)
    delivery = "DELIVERY"


class DataFrequency(str, Enum):
    """
    Partition frequency used by Binance's published historical datasets
    """

    # Data files partitioned by month (e.g., 2024-01)
    monthly = "monthly"

    # Data files partitioned by day (e.g., 2024-01-15)
    daily = "daily"


class DataType(str, Enum):
    """
    Type of dataset as named in Binance archives and APIs.
    """

    # Candlestick/Kline data 
    kline = "klines"
    # Funding rates for perpetual futures
    funding_rate = "fundingRate"
    # Aggregated trades stream
    agg_trade = "aggTrades"
    # Liquidation snapshot stream
    liquidation_snapshot = "liquidationSnapshot"
    # Metrics data
    metrics = "metrics"
