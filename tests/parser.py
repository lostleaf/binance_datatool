#!/usr/bin/env python3
"""
Test script for the unified AWS parser module.

This script tests the parser functionality with actual BTCUSDT data files
from the AWS data directory. It verifies both klines and funding rate parsing.
"""

import os
from pathlib import Path
import random

from bhds.aws.parser import create_aws_parser
from bhds.aws.path_builder import AwsKlinePathBuilder, AwsPathBuilder
from bhds.aws.checksum import AwsDataFileManager
from bdt_common.enums import TradeType, DataFrequency, DataType


def get_aws_data_dir():
    """Get the AWS data directory path."""
    # Check if CRYPTO_BASE_DIR is set, otherwise use default
    crypto_base = os.environ.get("CRYPTO_BASE_DIR", str(Path.home() / "crypto_data"))
    return Path(crypto_base) / "binance_data" / "aws_data"


def test_klines_parser():
    """Test the klines parser with actual BTCUSDT data."""
    print("=" * 80)
    print("Testing Klines Parser")
    print("=" * 80)

    aws_data_dir = get_aws_data_dir()
    
    # Use AwsKlinePathBuilder to get symbol directory without HTTP session
    path_builder = AwsKlinePathBuilder(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        time_interval="1m"
    )
    symbol_dir = path_builder.get_symbol_dir("BTCUSDT")
    # symbol_dir returns 'data/spot/daily/klines/BTCUSDT/1m'
    klines_dir = aws_data_dir / symbol_dir

    if not klines_dir.exists():
        print(f"❌ Klines directory not found: {klines_dir}")
        return

    # Use AwsDataFileManager to manage files
    file_manager = AwsDataFileManager(klines_dir)
    verified_files, unverified_files = file_manager.get_files()
    
    # Prefer verified files
    if verified_files:
        random.shuffle(verified_files)
        klines_files = verified_files[:3]
    else:
        print("❌ No klines files found")
        return
    
    print(f"Found {len(verified_files)} verified files and {len(unverified_files)} unverified files")
    print(f"Using {len(klines_files)} files for testing")

    parser = create_aws_parser(DataType.kline)

    print(f"Found {len(klines_files)} klines files to test:")
    for file in klines_files:
        print(f"  - {file.name}")
    print()

    for file in klines_files:
        try:
            print(f"Parsing: {file.name}")
            df = parser.read_csv_from_zip(file)

            print(f"✅ Successfully parsed {len(df)} rows")
            print("Schema:")
            print(df.schema)
            print("\nLast 5 rows:")
            print(df.tail(5))
            print("\n" + "-" * 60 + "\n")

        except Exception as e:
            print(f"❌ Error parsing {file.name}: {e}")


def test_funding_parser():
    """Test the funding rate parser with actual BTCUSDT data."""
    print("=" * 80)
    print("Testing Funding Rate Parser")
    print("=" * 80)

    aws_data_dir = get_aws_data_dir()
    
    # Use AwsPathBuilder to get funding rate directory
    path_builder = AwsPathBuilder(
        trade_type=TradeType.um_futures,
        data_freq=DataFrequency.monthly,
        data_type=DataType.funding_rate
    )
    symbol_dir = path_builder.get_symbol_dir("BTCUSDT")
    funding_dir = aws_data_dir / symbol_dir

    if not funding_dir.exists():
        print(f"❌ Funding directory not found: {funding_dir}")
        return

    # Use AwsDataFileManager to manage files
    file_manager = AwsDataFileManager(funding_dir)
    verified_files, unverified_files = file_manager.get_files()
    
    # Prefer verified files, fallback to unverified if no verified files available
    if verified_files:
        random.shuffle(verified_files)
        funding_files = verified_files[:3]
    else:
        print("❌ No funding files found")
        return

    parser = create_aws_parser(DataType.funding_rate)

    print(f"Found {len(funding_files)} funding files to test:")
    for file in funding_files:
        print(f"  - {file.name}")
    print()

    for file in funding_files:
        try:
            print(f"Parsing: {file.name}")
            df = parser.read_csv_from_zip(file)

            print(f"✅ Successfully parsed {len(df)} rows")
            print("Schema:")
            print(df.schema)
            print("\nLast 5 rows:")
            print(df.tail(5))
            print("\n" + "-" * 60 + "\n")

        except Exception as e:
            print(f"❌ Error parsing {file.name}: {e}")


def test_parser_creation():
    """Test parser creation function."""
    print("=" * 80)
    print("Testing Parser Creation")
    print("=" * 80)

    try:
        kline_parser = create_aws_parser(DataType.kline)
        print("✅ Successfully created klines parser")
        print(f"   Type: {type(kline_parser).__name__}")

        funding_parser = create_aws_parser(DataType.funding_rate)
        print("✅ Successfully created funding parser")
        print(f"   Type: {type(funding_parser).__name__}")

        # Test error handling
        try:
            create_aws_parser(DataType.agg_trade)  # Use an unsupported DataType
            print("❌ Should have raised ValueError for invalid type")
        except ValueError as e:
            print(f"✅ Correctly handled invalid type: {e}")

    except Exception as e:
        print(f"❌ Error creating parsers: {e}")


def main():
    """Run all tests."""
    print("🧪 Testing AWS Parser Module")
    print("=" * 80)

    aws_data_dir = get_aws_data_dir()
    print(f"AWS Data Directory: {aws_data_dir}")
    print(f"Directory exists: {aws_data_dir.exists()}")
    print()

    if not aws_data_dir.exists():
        print("❌ AWS data directory not found. Please ensure data is downloaded.")
        print("   You can use the AWS downloader to fetch the data first.")
        return

    test_parser_creation()
    test_klines_parser()
    test_funding_parser()

    print("=" * 80)
    print("✅ All tests completed!")


if __name__ == "__main__":
    main()
