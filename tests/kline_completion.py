#!/usr/bin/env python3
"""
Test script for API Kline completion modules

Test symbols: LUNAUSDT, FTTUSDT
Test data: 1m spot klines in daily frequency
"""

import asyncio
import os
import tempfile
from pathlib import Path

import aiohttp
import polars as pl
from test_utils import print_directory_structure

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.network import create_aiohttp_session
from bdt_common.rest_api.fetcher import BinanceFetcher
from bhds.api.completion.kline import DailyKlineCompletion
from bhds.aws.csv_conv import AwsCsvToParquetConverter
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder


def prepare_kline_data(temp_dir: Path, symbols: list[str], interval: str = "1m"):
    """Prepare kline data by converting CSV to Parquet"""
    print("📊 Preparing kline data...")

    # AWS data directory
    aws_data_dir = Path.home() / "crypto_data" / "binance_data" / "aws_data"

    # Create path builder for spot 1m klines
    path_builder = AwsKlinePathBuilder(trade_type=TradeType.spot, data_freq=DataFrequency.daily, time_interval=interval)

    # Create local AWS client
    local_aws_client = LocalAwsClient(base_dir=aws_data_dir, path_builder=path_builder)

    # Create CSV to Parquet converter
    processor = AwsCsvToParquetConverter(
        local_aws_client=local_aws_client,
        data_type=DataType.kline,
        output_base_dir=temp_dir,
        force_update=True,
        verbose=True,
    )

    # Process symbols
    print(f"🚀 Converting CSV to Parquet for symbols: {symbols}")
    results = processor.process_symbols(symbols)

    # Print results summary
    print("\n📈 Conversion Results:")
    for symbol, result in results.items():
        print(
            f"  {symbol}: {result['processed_files']} processed, "
            f"{result['skipped_files']} skipped, {result['failed_files']} failed"
        )

    return temp_dir


async def test_kline_completion(data_dir: Path, symbols: list[str], interval: str = "1m"):
    """Test DailyKlineCompletion.get_missings method"""
    print("\n🔍 Testing DailyKlineCompletion.get_missings...")
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    print(f"HTTP_PROXY: {http_proxy}")

    # Create aiohttp session
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Create BinanceFetcher
        fetcher = BinanceFetcher(trade_type=TradeType.spot, session=session, http_proxy=http_proxy)

        # Create DailyKlineCompletion instance
        completion = DailyKlineCompletion(
            trade_type=TradeType.spot,
            interval=interval,
            base_dir=data_dir,
            fetcher=fetcher,
        )

        # Test get_missings method
        print(f"📋 Getting missing data tasks for symbols: {symbols}")
        tasks = completion.get_missings(symbols)

        print(f"\n📊 Found {len(tasks)} missing data tasks:")
        for i, (fetch_func, kwargs, output_path) in enumerate(tasks, 1):
            print(f"  Task {i}:")
            print(f"    Function: {fetch_func.__name__}")
            print(f"    Arguments: {kwargs}")
            print(f"    Output path: {output_path}")

            # Convert to Path object if needed
            output_path = Path(output_path)
            print(f"    Output path exists: {output_path.exists()}")

            # Check if parent directory exists
            parent_dir = output_path.parent
            print(f"    Parent directory: {parent_dir}")
            print(f"    Parent directory exists: {parent_dir.exists()}")

            if parent_dir.exists():
                # List files in parent directory
                files = list(parent_dir.glob("*.parquet"))
                print(f"    Existing parquet files in parent: {len(files)}")
                for file in files[:3]:  # Show first 3 files
                    print(f"      - {file.name}")
                if len(files) > 3:
                    print(f"      ... and {len(files) - 3} more files")
            print()
            break

        return tasks


async def test_complete_multiple_symbols(data_dir: Path, symbols: list[str], interval: str = "1m"):
    """Test the complete_multiple_symbols method"""
    print("🔧 Testing DailyKlineCompletion.complete_multiple_symbols...")
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    print(f"HTTP_PROXY: {http_proxy}")

    # Create aiohttp session
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        # Create fetcher
        fetcher = BinanceFetcher(trade_type=TradeType.spot, session=session, http_proxy=http_proxy)

        # Create completion instance
        completion = DailyKlineCompletion(
            trade_type=TradeType.spot,
            interval=interval,
            base_dir=data_dir,
            fetcher=fetcher,
        )

        print(f"📋 Completing data for symbols: {symbols}")
        missing_tasks = completion.get_missings(symbols)
        missing_files = [task[2] for task in missing_tasks]

        # Test complete_multiple_symbols method
        result = await completion.complete_multiple_symbols(symbols=symbols)

        print(f"\n📊 Completion result:")
        print(f"  Total tasks: {result['total']}")
        print(f"  Successful tasks: {result['success']}")
        if result["total"] > 0:
            print(f"  Success: {result['success']}/{result['total']}")
        else:
            print("  Success rate: N/A (no tasks found)")

        after_tasks = completion.get_missings(symbols)
        print(f"  Found {len(after_tasks)} missing tasks after run completion")
        # Check if data files were created/updated
        print(f"\n📁 Checking data files:")
        for file in missing_files[:3]:
            print(f"     - {file.name}")
            try:
                df = pl.read_parquet(file)
                print(f"       Shape: {df.shape}, Columns: {df.columns}")
                if len(df) > 0:
                    print(f"       Date:{df['candle_begin_time'].min()} ~ {df['candle_begin_time'].max()}")
            except Exception as e:
                print(f"       Error reading file: {e}")

        return result


async def test_api_kline_completion():
    """Main test function for API kline completion modules"""
    # Test configuration
    symbols = ["LUNAUSDT", "FTTUSDT"]
    interval = "1m"

    print("=" * 60)
    print("Testing API Kline Completion Modules")
    print("=" * 60)
    print(f"Test symbols: {symbols}")
    print(f"Test interval: {interval}")
    print(f"Test trade type: {TradeType.spot.value}")

    # Create temporary directory for test data
    with tempfile.TemporaryDirectory(prefix="api_kline_completion_test_") as temp_dir:
        temp_path = Path(temp_dir)
        print(f"📁 Temp directory: {temp_path}")
        print(f"📝 Note: Temp directory will be automatically cleaned up after test")

        try:
            # Step 1: Prepare kline data
            data_dir = prepare_kline_data(temp_path, symbols, interval)

            # Print directory structure
            print("\n📂 Data Directory Structure:")
            print_directory_structure(data_dir, max_depth=10)

            # Step 2: Test kline completion
            tasks = await test_kline_completion(data_dir, symbols, interval)

            print(f"\n✅ Successfully tested get_missings method")
            print(f"📊 Total tasks generated: {len(tasks)}")

            # Step 3: Test complete_multiple_symbols method
            print("\n" + "-" * 60)
            completion_result = await test_complete_multiple_symbols(data_dir, symbols, interval)

            print(f"\n✅ Successfully tested complete_multiple_symbols method")
            if completion_result["total"] > 0:
                print(f"📊 Summary: {completion_result['success']}/{completion_result['total']} tasks completed")
            else:
                print(f"📊 Summary: No missing data found - all data is up to date")

        except Exception as e:
            print(f"❌ Error during testing: {e}")
            import traceback

            traceback.print_exc()

        print(f"\n🧹 Temp directory {temp_path} will be cleaned up automatically")

    print("\n" + "=" * 60)
    print("✅ API Kline Completion Test completed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_api_kline_completion())
