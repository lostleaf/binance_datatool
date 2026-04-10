"""Shared enums for Binance market archive paths."""

from enum import StrEnum


class TradeType(StrEnum):
    """Binance market segment."""

    spot = "spot"
    um = "um"
    cm = "cm"

    @property
    def s3_path(self) -> str:
        """Return the path component used in data.binance.vision."""
        if self is TradeType.spot:
            return "spot"
        return f"futures/{self.value}"


class DataFrequency(StrEnum):
    """Partition frequency on data.binance.vision."""

    daily = "daily"
    monthly = "monthly"


class DataType(StrEnum):
    """Dataset type on data.binance.vision."""

    klines = "klines"
    agg_trades = "aggTrades"
    trades = "trades"
    funding_rate = "fundingRate"
    book_depth = "bookDepth"
    book_ticker = "bookTicker"
    index_price_klines = "indexPriceKlines"
    mark_price_klines = "markPriceKlines"
    premium_index_klines = "premiumIndexKlines"
    metrics = "metrics"
    liquidation_snapshot = "liquidationSnapshot"
