#!/usr/bin/env python3
"""
Test how to use the new path builder to get AWS data paths without HTTP session.

This example shows how to use the path builder after separating it from the HTTP client.
"""

from bhds.aws.path_builder import AwsPathBuilder, AwsKlinePathBuilder
from bdt_common.enums import TradeType, DataFrequency, DataType


def test_basic_path_builder():
    """Test the usage of basic path builder."""
    print("=" * 60)
    print("Basic Path Builder Test")
    print("=" * 60)

    # Create different types of path builders
    builders = [
        ("Spot Daily Kline", AwsPathBuilder(TradeType.spot, DataFrequency.daily, DataType.kline)),
        ("Futures Monthly Funding Rate", AwsPathBuilder(TradeType.um_futures, DataFrequency.monthly, DataType.funding_rate)),
        ("Spot Monthly Aggregate Trade", AwsPathBuilder(TradeType.spot, DataFrequency.monthly, DataType.agg_trade)),
    ]

    for name, builder in builders:
        print(f"\n{name}:")
        print(f"  Base directory: {builder.base_dir}")
        print(f"  BTCUSDT directory: {builder.get_symbol_dir('BTCUSDT')}")
        print(f"  ETHUSDT directory: {builder.get_symbol_dir('ETHUSDT')}")


def test_kline_path_builder():
    """Test the usage of kline path builder."""
    print("\n" + "=" * 60)
    print("Kline Path Builder Test")
    print("=" * 60)

    # Create kline path builders with different time intervals
    intervals = ["1m", "5m", "1h", "1d"]

    for interval in intervals:
        builder = AwsKlinePathBuilder(trade_type=TradeType.spot, data_freq=DataFrequency.daily, time_interval=interval)

        print(f"\n{interval} Kline data:")
        print(f"  Base directory: {builder.base_dir}")
        print(f"  BTCUSDT directory: {builder.get_symbol_dir('BTCUSDT')}")
        print(f"  ETHUSDT directory: {builder.get_symbol_dir('ETHUSDT')}")


def test_path_usage_in_practice():
    """Test how to use path builder in practical applications."""
    print("\n" + "=" * 60)
    print("Practical Application Test")
    print("=" * 60)

    # Assume we have a local data directory
    from pathlib import Path

    local_data_dir = Path("/home/user/crypto_data/binance_data/aws_data/data")

    # Use path builder to get complete paths for specific data
    kline_builder = AwsKlinePathBuilder(trade_type=TradeType.spot, data_freq=DataFrequency.daily, time_interval="1m")

    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]

    print("\nLocal data directory structure:")
    for symbol in symbols:
        # Get relative path
        relative_path = kline_builder.get_symbol_dir(symbol)
        # Remove the leading 'data/' part
        clean_path = Path(*relative_path.parts[1:])
        # Build complete local path
        full_path = local_data_dir / clean_path

        print(f"  {symbol}: {full_path}")


def main():
    """Main function."""
    print("🚀 AWS Path Builder Usage Test")

    test_basic_path_builder()
    test_kline_path_builder()
    test_path_usage_in_practice()

    print("\n" + "=" * 60)
    print("✅ Test completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
