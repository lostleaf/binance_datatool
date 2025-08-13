#!/usr/bin/env python3
"""
Test script for API Funding Rates completion modules

Test symbols: BTCUSDT, ETHUSDT, BNBUSDT
"""

import asyncio
import os
import tempfile
from pathlib import Path

import aiohttp
import polars as pl
from test_utils import print_directory_structure

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import ContractType, DataFrequency, DataType, TradeType
from bdt_common.network import create_aiohttp_session
from bdt_common.rest_api.fetcher import BinanceFetcher
from bhds.api.completion.funding import RecentFundingCompletion
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


async def test_funding_completion(data_dir: Path, symbols: list[str]):
    """Test RecentFundingCompletion.get_missings method"""
    print("\n🔍 Testing RecentFundingCompletion.get_missings...")
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    print(f"HTTP_PROXY: {http_proxy}")
    # Create aiohttp session
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Create BinanceFetcher
        fetcher = BinanceFetcher(trade_type=TradeType.um_futures, session=session, http_proxy=http_proxy)

        # Create RecentFundingCompletion instance
        completion = RecentFundingCompletion(
            trade_type=TradeType.um_futures,
            base_dir=str(data_dir),
            fetcher=fetcher,
            contract_type=ContractType.perpetual,
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

        return tasks


async def test_complete_multiple_symbols(data_dir: Path, symbols: list[str]):
    """Test the complete_multiple_symbols method"""
    print("🔧 Testing RecentFundingCompletion.complete_multiple_symbols...")
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    print(f"HTTP_PROXY: {http_proxy}")
    # Create aiohttp session
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        # Create fetcher
        fetcher = BinanceFetcher(trade_type=TradeType.um_futures, session=session, http_proxy=http_proxy)

        # Create completion instance
        completion = RecentFundingCompletion(
            trade_type=TradeType.um_futures,
            base_dir=data_dir,
            fetcher=fetcher,
            contract_type=ContractType.perpetual,
        )

        print(f"📋 Completing data for symbols: {symbols}")

        # Test complete_multiple_symbols method
        result = await completion.complete_multiple_symbols(symbols=symbols)

        print(f"\n📊 Completion result:")
        print(f"  Total tasks: {result['total']}")
        print(f"  Successful tasks: {result['success']}")
        print(f"  Success rate: {result['success']}/{result['total']} ({result['success']/result['total']*100:.1f}%)")

        # Check if latest.parquet files were created
        print(f"\n📁 Checking created files:")
        for symbol in symbols:
            symbol_dir = completion.path_builder.get_symbol_dir(symbol)
            latest_file = data_dir / symbol_dir / "latest.parquet"
            if latest_file.exists():
                print(f"  ✅ {latest_file} created successfully")
                # Check file size
                file_size = latest_file.stat().st_size
                print(f"     File size: {file_size} bytes")
                df = pl.read_parquet(latest_file)
                print(df.shape)
                print(pl.concat([df.head(3), df.tail(3)]))
            else:
                print(f"  ❌ {latest_file} not found")

        return result


async def test_api_completion():
    """Main test function for API completion modules"""
    # Test configuration
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    print("=" * 60)
    print("Testing API Completion Modules")
    print("=" * 60)

    # Create temporary directory for test data
    with tempfile.TemporaryDirectory(prefix="api_completion_test_") as temp_dir:
        temp_path = Path(temp_dir)
        print(f"📁 Temp directory: {temp_path}")
        print(f"📝 Note: Temp directory will be automatically cleaned up after test")

        try:
            # Step 1: Prepare funding rate data
            data_dir = prepare_funding_data(temp_path, symbols)

            # Print directory structure
            print("\n📂 Data Directory Structure:")
            print_directory_structure(data_dir, max_depth=10)

            # Step 2: Test funding completion
            tasks = await test_funding_completion(data_dir, symbols)

            print(f"\n✅ Successfully tested get_missings method")
            print(f"📊 Total tasks generated: {len(tasks)}")

            # Step 3: Test complete_multiple_symbols method
            print("\n" + "-" * 60)
            completion_result = await test_complete_multiple_symbols(data_dir, symbols)

            print(f"\n✅ Successfully tested complete_multiple_symbols method")
            print(f"📊 Summary: {completion_result['success']}/{completion_result['total']} tasks completed")

        except Exception as e:
            print(f"❌ Error during testing: {e}")
            import traceback

            traceback.print_exc()

        print(f"\n🧹 Temp directory {temp_path} will be cleaned up automatically")

    print("\n" + "=" * 60)
    print("✅ API Completion Test completed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_api_completion())
