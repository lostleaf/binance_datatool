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
from bdt_common.log_kit import logger, divider
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

    divider("Testing CSV to Parquet Conversion")

    for data_type, trade_type, data_freq in test_cases:
        # Create temporary output directory (automatically cleaned up)
        with tempfile.TemporaryDirectory(prefix="parquet_test_") as temp_dir:
            temp_path = Path(temp_dir)
            divider(
                f"Testing {data_type.value} - {trade_type.value} - {data_freq.value}", sep="-"
            )

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
                logger.debug(f"Processing symbols: {symbols}")
                results = processor.process_symbols(symbols)

                # Print results summary
                for symbol, result in results.items():
                    logger.ok(
                        f"{symbol}: {result['processed_files']} processed, "
                        f"{result['skipped_files']} skipped, {result['failed_files']} failed"
                    )

                # Print directory structure (with limited files per directory)
                logger.info("Output Directory Structure (showing max 5 files per directory):")
                print_directory_structure(temp_path, max_depth=10)

                # Read and display sample data
                logger.info("Sample Data (last 5 rows):")
                parquet_files = list(temp_path.rglob("*.parquet"))

                if parquet_files:
                    # Show samples from first few files
                    for i, parquet_file in enumerate(parquet_files[:3]):
                        logger.debug(f"File: {parquet_file.relative_to(temp_path)}")
                        try:
                            df = pl.read_parquet(parquet_file)
                            logger.debug(f"Shape: {df.shape}, Columns: {df.columns}, Last 5 rows:")
                            logger.debug(str(df.tail(5)))
                        except Exception as e:
                            logger.error(f"     Error reading file: {e}")
                else:
                    logger.warning("  No parquet files found")

            except Exception as e:
                logger.exception(f"Error processing {data_type.value} - {trade_type.value} - {data_freq.value}: {e}")

            logger.info(f"Temp directory {temp_path} will be cleaned up automatically")

    divider("All tests completed")


if __name__ == "__main__":
    test_converter()
