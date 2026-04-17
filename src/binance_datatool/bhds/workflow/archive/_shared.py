"""Shared helpers for archive workflows."""

from __future__ import annotations

from typing import TYPE_CHECKING

from binance_datatool.common import TradeType, infer_cm_info, infer_spot_info, infer_um_info

if TYPE_CHECKING:
    from binance_datatool.common import DataType, SymbolInfo


def infer_symbol_info(trade_type: TradeType, symbol: str) -> SymbolInfo | None:
    """Infer typed symbol metadata for the requested market segment.

    Args:
        trade_type: Market segment being listed.
        symbol: Raw symbol from the archive directory listing.

    Returns:
        Parsed symbol metadata, or ``None`` when inference fails.
    """
    match trade_type:
        case TradeType.spot:
            return infer_spot_info(symbol)
        case TradeType.um:
            return infer_um_info(symbol)
        case TradeType.cm:
            return infer_cm_info(symbol)


def validate_interval(data_type: DataType, interval: str | None) -> None:
    """Validate whether the given interval matches the selected data type."""
    if data_type.has_interval_layer and interval is None:
        msg = "interval is required for kline-class data_type"
        raise ValueError(msg)
    if not data_type.has_interval_layer and interval is not None:
        msg = "interval is not applicable to non-kline data_type"
        raise ValueError(msg)
