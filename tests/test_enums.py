"""Tests for shared enum helpers."""

from binance_datatool.common import DataType


def test_data_type_has_interval_layer() -> None:
    """Only kline-class data types should require an interval layer."""
    true_members = {
        DataType.klines,
        DataType.index_price_klines,
        DataType.mark_price_klines,
        DataType.premium_index_klines,
    }

    for member in DataType:
        assert member.has_interval_layer is (member in true_members)
