#!/usr/bin/env python3
"""
Test script for Holo1mKlineMerger

This script tests the holographic 1-minute kline data synthesis for:
- UM futures 1m klines with VWAP and funding rate

Test symbols: BTCUSDT, ETHUSDT
"""

import random
import tempfile
from pathlib import Path
import polars as pl

from bdt_common.enums import TradeType
from bhds.holo_kline.merger import Holo1mKlineMerger


def test_holo_merger_generate(symbol: str, trade_type: TradeType):
    """Test the Holo1mKlineMerger

    Args:
        symbol: Trading symbol to test
        trade_type: TradeType enum (spot, um_futures, cm_futures)
                   For futures types, include_funding=True
                   For spot type, include_funding=False
    """
    # Test configuration
    parsed_data_dir = Path.home() / "crypto_data" / "binance_data" / "parsed_data"

    print("=" * 60)
    print("Testing Holo1mKlineMerger")
    print("=" * 60)
    print(f"📊 Input directory: {parsed_data_dir}")
    print(f"📝 Test symbol: {symbol}")
    print(f"📝 Trade type: {trade_type}")

    include_vwap = True
    # Set include_funding based on trade_type
    include_funding = trade_type in [TradeType.um_futures, TradeType.cm_futures]

    print(f"\n📊 Testing: VWAP={include_vwap}, Funding={include_funding}")
    print("-" * 40)
    # Create temporary output directory
    with tempfile.TemporaryDirectory(prefix="holo_merger_test_") as temp_dir:
        temp_path = Path(temp_dir)
        print(f"📁 Temp directory: {temp_path}")
        print(f"📝 Note: Temp directory will be automatically cleaned up after test")

        try:
            # Create merger
            merger = Holo1mKlineMerger(
                trade_type=trade_type,
                base_dir=parsed_data_dir,
                include_vwap=include_vwap,
                include_funding=include_funding,
            )

            # Test generate method for each symbol
            output_file = temp_path / f"{symbol}.parquet"
            ldf = merger.generate(symbol, output_file)

            # Collect and check results
            ldf.collect()
            df = pl.read_parquet(output_file)
            print(f"\n  📈 {symbol}:")
            print(f"     Shape: {df.shape}")
            print(f"     Time range: {df['candle_begin_time'].min()} to {df['candle_begin_time'].max()}")
            print(f"     Time max diff: {df['candle_begin_time'].diff().abs().max()}")
            print(f"     Volume zero count: {df.filter(pl.col('volume') == 0).shape}")
            print(f"     Total records: {len(df)}")

            # Check for missing values
            null_counts = df.null_count()
            if null_counts.sum().sum_horizontal().item(0) > 0:
                print(f"     ⚠️  Null values found: {null_counts.to_dict()}")

            # Check VWAP and funding rate columns
            if include_vwap:
                print(f"     VWAP range: {df['vwap_1m'].min():.2f} - {df['vwap_1m'].max():.2f}")
            if include_funding:
                print(f"     Funding rate range: {df['funding_rate'].min():.6f} - {df['funding_rate'].max():.6f}")
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback

            traceback.print_exc()

        print(f"\n🧹 Temp directory will be cleaned up automatically")


def test_holo_merger_generate_all():
    """Test the Holo1mKlineMerger generate_all method for CM futures"""
    # Test configuration
    parsed_data_dir = Path.home() / "crypto_data" / "binance_data" / "parsed_data"
    trade_type = TradeType.cm_futures

    print("=" * 60)
    print("Testing Holo1mKlineMerger generate_all")
    print("=" * 60)
    print(f"📊 Input directory: {parsed_data_dir}")
    print(f"📝 Trade type: {trade_type}")

    include_vwap = True
    include_funding = True  # CM futures include funding

    print(f"\n📊 Testing: VWAP={include_vwap}, Funding={include_funding}")
    print("-" * 40)

    # Create temporary output directory
    with tempfile.TemporaryDirectory(prefix="holo_merger_all_test_") as temp_dir:
        temp_path = Path(temp_dir)
        print(f"📁 Temp directory: {temp_path}")
        print(f"📝 Note: Temp directory will be automatically cleaned up after test")

        try:
            # Create merger
            merger = Holo1mKlineMerger(
                trade_type=trade_type,
                base_dir=parsed_data_dir,
                include_vwap=include_vwap,
                include_funding=include_funding,
            )

            # Test generate_all method
            print(f"\n🔄 Running generate_all for all CM futures symbols...")
            results = merger.generate_all(temp_path)

            print(f"\n📊 Results Summary:")
            print(f"     Total symbols processed: {len(results)}")

            # Trigger actual file writing by collecting LazyFrames
            print(f"\n🔄 Collecting LazyFrames to trigger file writing...")
            collected_symbols = list()
            for symbol, ldf in results.items():
                try:
                    # Collect the LazyFrame to trigger file writing
                    ldf.collect()
                    output_file = temp_path / f"{symbol}.parquet"
                    if output_file.exists():
                        df = pl.read_parquet(output_file)
                        print(f"  ✅ {symbol}: Collected {len(df)} records")
                        collected_symbols.append(symbol)
                except Exception as e:
                    print(f"  ❌ {symbol}: Error collecting - {e}")

            # Check each result
            print(f"\n📊 Detailed Results:")
            random.shuffle(collected_symbols)

            for symbol in collected_symbols[:3]:
                output_file = temp_path / f"{symbol}.parquet"
                df = pl.read_parquet(output_file)

                # Check if DataFrame has data
                if len(df) == 0 or df.shape[1] == 0:
                    continue

                print(f"\n  📈 {symbol}:")
                print(f"     Shape: {df.shape}")

                # Check if required columns exist
                if "candle_begin_time" in df.columns:
                    print(f"     Time range: {df['candle_begin_time'].min()} to {df['candle_begin_time'].max()}")
                else:
                    print(f"     ⚠️  Missing candle_begin_time column")

                print(f"     Total records: {len(df)}")
                print(f"     Time max diff: {df['candle_begin_time'].diff().abs().max()}")
                print(f"     Volume zero count: {df.filter(pl.col('volume') == 0).shape}")

                # Check for missing values
                null_counts = df.null_count()
                if null_counts.sum().sum_horizontal().item(0) > 0:
                    print(f"     ⚠️  Null values found: {null_counts.to_dict()}")

                # Check VWAP and funding rate columns
                if include_vwap and "vwap_1m" in df.columns:
                    print(f"     VWAP range: {df['vwap_1m'].min():.2f} - {df['vwap_1m'].max():.2f}")
                if include_funding and "funding_rate" in df.columns:
                    print(f"     Funding rate range: {df['funding_rate'].min():.6f} - {df['funding_rate'].max():.6f}")

            print(f"\n📊 Samples Summary:")
            print(f"     Total symbols: {len(collected_symbols)}")

        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback

            traceback.print_exc()

        print(f"\n🧹 Temp directory will be cleaned up automatically")


def main():
    """Main function to run all tests"""
    # Test with different trade types
    print("\n" + "=" * 80)
    print("Testing UM Futures (include_funding=True)")
    print("=" * 80)
    test_holo_merger_generate("BTCUSDT", TradeType.um_futures)
    test_holo_merger_generate("ETHUSDT", TradeType.um_futures)

    print("\n" + "=" * 80)
    print("Testing CM Futures (include_funding=True)")
    print("=" * 80)
    test_holo_merger_generate("BTCUSD_PERP", TradeType.cm_futures)

    print("\n" + "=" * 80)
    print("Testing Spot (include_funding=False)")
    print("=" * 80)
    test_holo_merger_generate("BTCUSDT", TradeType.spot)
    test_holo_merger_generate("ETHUSDT", TradeType.spot)

    print("\n" + "=" * 80)
    print("Testing generate_all for CM Futures")
    print("=" * 80)
    test_holo_merger_generate_all()


if __name__ == "__main__":
    main()
