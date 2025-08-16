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
from bhds.aws.local import AwsDataFileManager
from bdt_common.enums import TradeType, DataFrequency, DataType
from bdt_common.log_kit import logger, divider


def get_aws_data_dir():
    """Get the AWS data directory path."""
    # Check if CRYPTO_BASE_DIR is set, otherwise use default
    crypto_base = os.environ.get("CRYPTO_BASE_DIR", str(Path.home() / "crypto_data"))
    return Path(crypto_base) / "binance_data" / "aws_data"


def test_klines_parser():
    """Test the klines parser with actual BTCUSDT data."""
    divider("Testing Klines Parser")

    aws_data_dir = get_aws_data_dir()

    # Use AwsKlinePathBuilder to get symbol directory without HTTP session
    path_builder = AwsKlinePathBuilder(trade_type=TradeType.spot, data_freq=DataFrequency.daily, time_interval="1m")
    symbol_dir = path_builder.get_symbol_dir("BTCUSDT")
    # symbol_dir returns 'data/spot/daily/klines/BTCUSDT/1m'
    klines_dir = aws_data_dir / symbol_dir

    if not klines_dir.exists():
        logger.error(f"Klines directory not found: {klines_dir}")
        return

    # Use AwsDataFileManager to manage files
    file_manager = AwsDataFileManager(klines_dir)
    verified_files, unverified_files = file_manager.get_files()

    # Prefer verified files
    if verified_files:
        random.shuffle(verified_files)
        klines_files = verified_files[:3]
    else:
        logger.error("No klines files found")
        return

    logger.info(
        f"Verified: {len(verified_files)}, Unverified: {len(unverified_files)}, Testing: {len(klines_files)} files"
    )

    parser = create_aws_parser(DataType.kline)

    logger.info(f"Found {len(klines_files)} klines files to test:")
    for file in klines_files:
        logger.debug(f"  - {file.name}")

    for file in klines_files:
        try:
            logger.debug(f"Parsing: {file.name}")
            df = parser.read_csv_from_zip(file)

            logger.ok(f"Successfully parsed {len(df)} rows")
            logger.debug("Schema:")
            logger.debug(df.schema)
            logger.debug("\nLast 5 rows:")
            logger.debug(df.tail(5))
            divider("", sep="-")

        except Exception as e:
            logger.exception(f"Error parsing {file.name}: {e}")


def test_funding_parser():
    """Test the funding rate parser with actual BTCUSDT data."""
    divider("Testing Funding Rate Parser")

    aws_data_dir = get_aws_data_dir()

    # Use AwsPathBuilder to get funding rate directory
    path_builder = AwsPathBuilder(
        trade_type=TradeType.um_futures, data_freq=DataFrequency.monthly, data_type=DataType.funding_rate
    )
    symbol_dir = path_builder.get_symbol_dir("BTCUSDT")
    funding_dir = aws_data_dir / symbol_dir

    if not funding_dir.exists():
        logger.error(f"Funding directory not found: {funding_dir}")
        return

    # Use AwsDataFileManager to manage files
    file_manager = AwsDataFileManager(funding_dir)
    verified_files, unverified_files = file_manager.get_files()

    # Prefer verified files, fallback to unverified if no verified files available
    if verified_files:
        random.shuffle(verified_files)
        funding_files = verified_files[:3]
    else:
        logger.error("No funding files found")
        return

    parser = create_aws_parser(DataType.funding_rate)

    logger.info(f"Found {len(funding_files)} funding files to test:")
    for file in funding_files:
        logger.debug(f"  - {file.name}")

    for file in funding_files:
        try:
            logger.debug(f"Parsing: {file.name}")
            df = parser.read_csv_from_zip(file)

            logger.ok(f"Successfully parsed {len(df)} rows")
            logger.debug("Schema:")
            logger.debug(df.schema)
            logger.debug("\nLast 5 rows:")
            logger.debug(df.tail(5))
            divider("", sep="-")

        except Exception as e:
            logger.exception(f"Error parsing {file.name}: {e}")


def test_parser_creation():
    """Test parser creation function."""
    divider("Testing Parser Creation")

    try:
        kline_parser = create_aws_parser(DataType.kline)
        logger.ok(f"Successfully created klines parser, Type: {type(kline_parser).__name__}")

        funding_parser = create_aws_parser(DataType.funding_rate)
        logger.ok(f"Successfully created funding parser, Type: {type(funding_parser).__name__}")

        # Test error handling
        try:
            create_aws_parser(DataType.agg_trade)  # Use an unsupported DataType
            logger.error("Should have raised ValueError for invalid type")
        except ValueError as e:
            logger.ok(f"Correctly handled invalid type: {e}")

    except Exception as e:
        logger.exception(f"Error creating parsers: {e}")


def main():
    """Run all tests."""
    aws_data_dir = get_aws_data_dir()
    logger.info(f"AWS Data Directory: {aws_data_dir}, Directory exists: {aws_data_dir.exists()}")

    if not aws_data_dir.exists():
        logger.error("AWS data directory not found. Please ensure data is downloaded.")
        logger.error("   You can use the AWS downloader to fetch the data first.")
        return

    test_parser_creation()
    test_klines_parser()
    test_funding_parser()


if __name__ == "__main__":
    main()
