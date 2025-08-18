#!/usr/bin/env python3
"""
Test how to use the new path builder to get AWS data paths without HTTP session.

This example shows how to use the path builder after separating it from the HTTP client.
"""

from pathlib import Path

from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import divider, logger
from bhds.aws.path_builder import AwsKlinePathBuilder, AwsPathBuilder
from bhds.tasks.common import get_bhds_home


def test_basic_path_builder():
    """Test the usage of basic path builder."""
    divider("Basic Path Builder Test", sep="-")

    # Create different types of path builders
    builders = [
        ("Spot Daily Kline", AwsPathBuilder(TradeType.spot, DataFrequency.daily, DataType.kline)),
        (
            "Futures Monthly Funding Rate",
            AwsPathBuilder(TradeType.um_futures, DataFrequency.monthly, DataType.funding_rate),
        ),
        ("Spot Monthly Aggregate Trade", AwsPathBuilder(TradeType.spot, DataFrequency.monthly, DataType.agg_trade)),
    ]

    for name, builder in builders:
        logger.info(f"{name}:")
        logger.debug(f"  Base directory: {builder.base_dir}")
        logger.debug(f"  BTCUSDT directory: {builder.get_symbol_dir('BTCUSDT')}")
        logger.debug(f"  ETHUSDT directory: {builder.get_symbol_dir('ETHUSDT')}")


def test_kline_path_builder():
    """Test the usage of kline path builder."""
    divider("Kline Path Builder Test", sep="-")

    # Create kline path builders with different time intervals
    intervals = ["1m", "5m", "1h", "1d"]

    for interval in intervals:
        builder = AwsKlinePathBuilder(trade_type=TradeType.spot, data_freq=DataFrequency.daily, time_interval=interval)

        logger.info(f"{interval} Kline data:")
        logger.debug(f"  Base directory: {builder.base_dir}")
        logger.debug(f"  BTCUSDT directory: {builder.get_symbol_dir('BTCUSDT')}")
        logger.debug(f"  ETHUSDT directory: {builder.get_symbol_dir('ETHUSDT')}")


def test_path_usage_in_practice():
    """Test how to use path builder in practical applications."""
    divider("Practical Application Test", sep="-")

    local_data_dir = get_bhds_home(None) / "aws_data"

    # Use path builder to get complete paths for specific data
    kline_builder = AwsKlinePathBuilder(trade_type=TradeType.spot, data_freq=DataFrequency.daily, time_interval="1m")

    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]

    logger.info("Local data directory structure:")
    for symbol in symbols:
        # Get relative path
        relative_path = kline_builder.get_symbol_dir(symbol)

        # Build complete local path
        full_path = local_data_dir / relative_path

        logger.debug(f"  {symbol}: {full_path}")


def main():
    """Main function."""
    divider("AWS Path Builder Usage Test")

    test_basic_path_builder()
    test_kline_path_builder()
    test_path_usage_in_practice()

    divider("All tests completed")


if __name__ == "__main__":
    main()
