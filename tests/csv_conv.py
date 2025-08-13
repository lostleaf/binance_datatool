#!/usr/bin/env python3
"""
Test script for AwsCsvToParquetConverter

This script tests the CSV to Parquet conversion functionality for:
- Spot 1m klines (daily frequency)
- UM futures funding rates (monthly frequency)

Test symbols: BTCUSDT, ETHUSDT, BNBUSDT
"""

import tempfile
from pathlib import Path
import polars as pl

from bdt_common.enums import DataType, TradeType, DataFrequency
from bhds.aws.csv_conv import AwsCsvToParquetConverter
from bhds.aws.path_builder import AwsPathBuilder, AwsKlinePathBuilder
from bhds.aws.local import LocalAwsClient
from test_utils import print_directory_structure


def test_converter():
    """Test the AwsCsvToParquetConverter"""
    # Test configuration
    aws_data_dir = Path.home() / "crypto_data" / "binance_data" / "aws_data"
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    # Test cases: (data_type, trade_type, data_freq)
    test_cases = [
        (DataType.kline, TradeType.spot, DataFrequency.daily),
        (DataType.funding_rate, TradeType.um_futures, DataFrequency.monthly),
    ]

    print("=" * 60)
    print("Testing AwsCsvToParquetConverter")
    print("=" * 60)

    for data_type, trade_type, data_freq in test_cases:
        print(f"\n📊 Testing {data_type.value} - {trade_type.value} - {data_freq.value}")
        print("-" * 40)

        # Create temporary output directory (automatically cleaned up)
        with tempfile.TemporaryDirectory(prefix="parquet_test_") as temp_dir:
            temp_path = Path(temp_dir)
            print(f"📁 Temp directory: {temp_path}")
            print(f"📝 Note: Temp directory will be automatically cleaned up after test")
            print(f"📝 Note: Original CSV zip files will NOT be modified")

            try:
                # Create path builder and local AWS client
                if data_type == DataType.kline:
                    # For klines, use AwsKlinePathBuilder with time_interval
                    path_builder = AwsKlinePathBuilder(trade_type=trade_type, data_freq=data_freq, time_interval="1m")
                else:
                    # For other data types, use basic AwsPathBuilder
                    path_builder = AwsPathBuilder(trade_type=trade_type, data_freq=data_freq, data_type=data_type)

                # Create local AWS client
                local_aws_client = LocalAwsClient(base_dir=aws_data_dir, path_builder=path_builder)

                # Create processor
                processor = AwsCsvToParquetConverter(
                    local_aws_client=local_aws_client,
                    data_type=data_type,
                    output_base_dir=temp_path,
                    force_update=True,
                    verbose=True,
                )

                # Process symbols
                print(f"🚀 Processing symbols: {symbols}")
                results = processor.process_symbols(symbols)

                # Print results summary
                print("\n📈 Processing Results:")
                for symbol, result in results.items():
                    print(
                        f"  {symbol}: {result['processed_files']} processed, "
                        f"{result['skipped_files']} skipped, {result['failed_files']} failed"
                    )

                # Print directory structure (with limited files per directory)
                print("\n📂 Output Directory Structure (showing max 5 files per directory):")
                print_directory_structure(temp_path, max_depth=10)

                # Read and display sample data
                print("\n📄 Sample Data (last 5 rows):")
                parquet_files = list(temp_path.rglob("*.parquet"))

                if parquet_files:
                    # Show samples from first few files
                    for i, parquet_file in enumerate(parquet_files[:3]):
                        print(f"\n  📋 File: {parquet_file.relative_to(temp_path)}")
                        try:
                            df = pl.read_parquet(parquet_file)
                            print(f"     Shape: {df.shape}")
                            print(f"     Columns: {df.columns}")
                            print("     Last 5 rows:")
                            tail_df = df.tail(5)
                            print(tail_df)
                        except Exception as e:
                            print(f"     ❌ Error reading file: {e}")
                else:
                    print("  ⚠️  No parquet files found")

            except Exception as e:
                print(f"❌ Error processing {data_type.value} - {trade_type.value} - {data_freq.value}: {e}")
                import traceback

                traceback.print_exc()

            print(f"\n🧹 Temp directory {temp_path} will be cleaned up automatically")

    print("\n" + "=" * 60)
    print("✅ Test completed")
    print("=" * 60)


if __name__ == "__main__":
    test_converter()
