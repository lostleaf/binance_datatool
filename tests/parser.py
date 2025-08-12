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


def get_aws_data_dir():
    """Get the AWS data directory path."""
    # Check if CRYPTO_BASE_DIR is set, otherwise use default
    crypto_base = os.environ.get("CRYPTO_BASE_DIR", str(Path.home() / "crypto_data"))
    return Path(crypto_base) / "binance_data" / "aws_data" / "data"


def test_klines_parser():
    """Test the klines parser with actual BTCUSDT data."""
    print("=" * 80)
    print("Testing Klines Parser")
    print("=" * 80)

    aws_data_dir = get_aws_data_dir()
    klines_dir = aws_data_dir / "spot" / "daily" / "klines" / "BTCUSDT" / "1m"

    if not klines_dir.exists():
        print(f"❌ Klines directory not found: {klines_dir}")
        return

    # Get the first 3 available klines files
    klines_files = list(klines_dir.glob("*.zip"))
    random.shuffle(klines_files)
    klines_files = klines_files[:3]

    if not klines_files:
        print("❌ No klines files found")
        return

    parser = create_aws_parser("klines")

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
    funding_dir = aws_data_dir / "futures" / "um" / "monthly" / "fundingRate" / "BTCUSDT"

    if not funding_dir.exists():
        print(f"❌ Funding directory not found: {funding_dir}")
        return

    # Get the first 3 available funding files
    funding_files = list(funding_dir.glob("*.zip"))
    random.shuffle(funding_files)
    funding_files = funding_files[:3]

    if not funding_files:
        print("❌ No funding files found")
        return

    parser = create_aws_parser("funding")

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
        kline_parser = create_aws_parser("klines")
        print("✅ Successfully created klines parser")
        print(f"   Type: {type(kline_parser).__name__}")

        funding_parser = create_aws_parser("funding")
        print("✅ Successfully created funding parser")
        print(f"   Type: {type(funding_parser).__name__}")

        # Test error handling
        try:
            create_aws_parser("invalid_type")
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
