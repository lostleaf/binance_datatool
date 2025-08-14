#!/usr/bin/env python3
"""
Test script for FundingRateDetector and DataExecutor

Test symbols: BTCUSDT, ETHUSDT, BNBUSDT
Tests both detection of missing funding rate data and execution of completion tasks
"""

import asyncio
import os
import tempfile
from pathlib import Path

import polars as pl

from test_utils import print_directory_structure

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.network import create_aiohttp_session
from bdt_common.rest_api.fetcher import BinanceFetcher
from bhds.api.completion.detector import FundingRateDetector
from bhds.api.completion.executor import DataExecutor
from bhds.aws.csv_conv import AwsCsvToParquetConverter
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsPathBuilder


def prepare_funding_data(temp_dir: Path, symbols: list[str]):
    """Prepare funding rate data by converting CSV to Parquet"""
    print("📊 Preparing funding rate data...")

    # AWS data directory
    aws_data_dir = Path.home() / "crypto_data" / "binance_data" / "aws_data"

    # Create path builder for funding rates
    path_builder = AwsPathBuilder(
        trade_type=TradeType.um_futures, data_freq=DataFrequency.monthly, data_type=DataType.funding_rate
    )

    # Create local AWS client
    local_aws_client = LocalAwsClient(base_dir=aws_data_dir, path_builder=path_builder)

    # Create CSV to Parquet converter
    processor = AwsCsvToParquetConverter(
        local_aws_client=local_aws_client,
        data_type=DataType.funding_rate,
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


def test_detection_only(data_dir: Path, symbols: list[str]):
    """Test FundingRateDetector.detect method (offline)"""
    print("🔍 Testing FundingRateDetector.detect...")

    # Create detector instance (no fetcher needed!)
    detector = FundingRateDetector(
        trade_type=TradeType.um_futures,
        base_dir=data_dir,
    )

    # Test detect method
    print(f"📋 Detecting missing data for symbols: {symbols}")
    tasks = detector.detect(symbols)

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

    return tasks


async def test_data_executor(data_dir: Path, symbols: list[str]):
    """Test DataExecutor with funding rate API calls"""
    print("⚙️ Testing DataExecutor for funding rates...")

    # Create detector and detect missing tasks
    detector = FundingRateDetector(
        trade_type=TradeType.um_futures,
        base_dir=data_dir,
    )

    tasks = detector.detect(symbols)
    print(f"\n📊 Found {len(tasks)} missing data tasks")

    if not tasks:
        print("✅ No missing data found, creating artificial task for testing")
        # Create a test task
        test_symbol = symbols[0]
        symbol_dir = Path(data_dir) / "parsed_data" / "fundingRate" / "um" / test_symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        save_path = symbol_dir / f"{test_symbol}-fundingRate-2023-01.parquet"
        tasks = [
            (
                BinanceFetcher.get_funding_rate_df,
                {"symbol": test_symbol, "start_time": "2023-01-01", "end_time": "2023-01-31"},
                save_path,
            )
        ]

    # Create executor and execute tasks
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    print(f"HTTP_PROXY: {http_proxy}")

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type=TradeType.um_futures, session=session, http_proxy=http_proxy)
        executor = DataExecutor(fetcher)

        print("\n🚀 Executing tasks...")
        result = await executor.execute(tasks, batch_size=2)

        print(f"\n📊 Execution result:")
        print(f"  Total tasks: {result['total']}")
        print(f"  Successful tasks: {result['success']}")
        print(
            f"  Success rate: {result['success']}/{result['total']} ({result['success']/result['total']*100:.1f}%)"
            if result["total"] > 0
            else "N/A"
        )

        # Check created files
        print(f"\n📁 Checking created files:")
        for _, _, save_path in tasks:
            if save_path.exists():
                print(f"  ✅ {save_path} created successfully")
                try:
                    df = pl.read_parquet(save_path)
                    print(f"     Shape: {df.shape}, Columns: {len(df.columns)}")
                    print(df.tail())
                except Exception as e:
                    print(f"     Error reading file: {e}")
            else:
                print(f"  ❌ {save_path} not found")

        return result


async def test_api_funding_detector():
    """Main test function for API funding detector and executor modules"""
    # Test configuration
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    print("=" * 80)
    print("Testing API Funding Detector and Executor Modules")
    print("=" * 80)
    print(f"Test symbols: {symbols}")
    print(f"Test trade type: {TradeType.um_futures.value}")
    print(f"Test data type: {DataType.funding_rate.value}")

    # Create temporary directory for test data
    with tempfile.TemporaryDirectory(prefix="api_funding_detector_test_") as temp_dir:
        temp_path = Path(temp_dir)
        print(f"📁 Temp directory: {temp_path}")
        print(f"📝 Note: Temp directory will be automatically cleaned up after test")

        try:
            # Step 1: Prepare funding rate data
            data_dir = prepare_funding_data(temp_path, symbols)

            # Print directory structure
            print("\n📂 Data Directory Structure:")
            print_directory_structure(data_dir, max_depth=10)

            # Step 2: Test detection only (offline)
            print("\n" + "-" * 60)
            print("TEST 1: Detection Only (Offline)")
            print("-" * 60)
            tasks = test_detection_only(data_dir, symbols)
            print(f"\n✅ Detection test completed - {len(tasks)} tasks found")

            # Step 3: Test execution only
            print("\n" + "-" * 60)
            print("TEST 2: Execution Only (Online)")
            print("-" * 60)
            exec_result = await test_data_executor(data_dir, symbols)
            print(f"\n✅ Execution test completed - {exec_result['success']}/{exec_result['total']} successful")

        except Exception as e:
            print(f"❌ Error during testing: {e}")
            import traceback

            traceback.print_exc()

        print(f"\n🧹 Temp directory {temp_path} will be cleaned up automatically")

    print("\n" + "=" * 80)
    print("✅ API Funding Detector and Executor Test completed")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_api_funding_detector())
