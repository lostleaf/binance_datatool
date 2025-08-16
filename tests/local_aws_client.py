#!/usr/bin/env python3
"""
Test for LocalAwsClient functionality.

Tests the LocalAwsClient class which manages downloaded Binance historical data files,
including symbol listing, file verification status tracking, and summary statistics.
"""

import os
from pathlib import Path

from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import logger, divider
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsPathBuilder, AwsKlinePathBuilder


def get_data_directory() -> Path:
    """Get the default data directory path."""
    default_base = os.path.join(os.path.expanduser("~"), "crypto_data")
    base_dir = Path(os.getenv("CRYPTO_BASE_DIR", default_base))
    data_dir = base_dir / "binance_data" / "aws_data"
    return data_dir


def test_local_client(base_dir: Path, trade_type: TradeType, data_freq: DataFrequency, data_type: DataType, title: str):
    """Test LocalAwsClient with specified parameters."""
    divider(title, sep="-")

    path_builder = AwsPathBuilder(
        trade_type=trade_type,
        data_freq=data_freq,
        data_type=data_type,
    )

    client = LocalAwsClient(
        base_dir=base_dir,
        path_builder=path_builder,
    )

    # List symbols
    symbols = client.list_symbols()
    logger.info(f"symbols count: {len(symbols)}")

    if not symbols:
        logger.warning("No symbols found in local directory")
        return

    # Show first few symbols
    sample_symbols = symbols[:10] if len(symbols) > 10 else symbols
    logger.debug(f"sample symbols: {sample_symbols}")

    # Check for common symbols
    target_symbols = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}
    available_targets = target_symbols.intersection(symbols)
    logger.info(f"targets in symbols: {available_targets}")

    if not available_targets:
        # Use first few symbols if targets not available
        available_targets = set(symbols[:3])
        logger.info(f"using first 3 symbols instead: {available_targets}")

    # Test file listing and status for target symbols
    for sym in available_targets:
        files = client.list_data_files(sym)
        logger.debug(f"{sym} files count: {len(files)}")

        if files:
            logger.debug(f"{sym} first file: {files[0]}")
            logger.debug(f"{sym} last file: {files[-1]}")

            # Test file status
            status = client.get_symbol_file_status(sym)
            verified_count = len(status["verified"])
            unverified_count = len(status["unverified"])
            logger.info(f"{sym} verified files: {verified_count}, unverified files: {unverified_count}")

    # Test batch operations
    if available_targets:
        logger.info("Batch Operations:")
        batch_status = client.batch_get_symbol_file_status(list(available_targets))
        for sym, status in batch_status.items():
            verified_count = len(status["verified"])
            unverified_count = len(status["unverified"])
            logger.debug(f"Batch - {sym}: {verified_count} verified, {unverified_count} unverified")

    # Test summary
    summary = client.get_summary()
    logger.info(
        f"Total symbols: {summary['total_symbols']}, Total files: {summary['total_files']}, "
        f"Verified files: {summary['verified_files']}, Unverified files: {summary['unverified_files']}"
    )

    logger.ok("Test completed successfully")


def test_local_kline_client(
    base_dir: Path, trade_type: TradeType, data_freq: DataFrequency, time_interval: str, title: str
):
    """Test LocalAwsClient with kline-specific path builder."""
    divider(title, sep="-")

    path_builder = AwsKlinePathBuilder(
        trade_type=trade_type,
        data_freq=data_freq,
        time_interval=time_interval,
    )

    client = LocalAwsClient(
        base_dir=base_dir,
        path_builder=path_builder,
    )

    # List symbols
    symbols = client.list_symbols()
    logger.info(f"symbols count: {len(symbols)}")

    if not symbols:
        logger.warning("No symbols found in local directory")
        return

    # Show first few symbols
    sample_symbols = symbols[:10] if len(symbols) > 10 else symbols
    logger.debug(f"sample symbols: {sample_symbols}")

    # Check for common symbols
    target_symbols = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}
    available_targets = target_symbols.intersection(symbols)
    logger.info(f"targets in symbols: {available_targets}")

    if not available_targets:
        # Use first few symbols if targets not available
        available_targets = set(symbols[:3])
        logger.info(f"using first 3 symbols instead: {available_targets}")

    # Test file listing and status for target symbols
    for sym in available_targets:
        files = client.list_data_files(sym)
        logger.debug(f"{sym} files count: {len(files)}")

        if files:
            logger.debug(f"{sym} first file: {files[0]}")
            logger.debug(f"{sym} last file: {files[-1]}")

            # Test file status
            status = client.get_symbol_file_status(sym)
            verified_count = len(status["verified"])
            unverified_count = len(status["unverified"])
            logger.info(f"{sym} verified files: {verified_count}, unverified files: {unverified_count}")

    # Test all symbols status (limited output)
    logger.info("All Symbols Status (first 5):")
    all_status = client.get_all_symbols_status()
    for i, (sym, status) in enumerate(all_status.items()):
        if i >= 5:  # Limit output
            break
        verified_count = len(status["verified"])
        unverified_count = len(status["unverified"])
        logger.debug(f"{sym}: {verified_count} verified, {unverified_count} unverified")

    # Test summary
    summary = client.get_summary()
    logger.info(
        f"Total symbols: {summary['total_symbols']}, Total files: {summary['total_files']}, "
        f"Verified files: {summary['verified_files']}, Unverified files: {summary['unverified_files']}"
    )

    logger.ok("Test completed successfully")


def main():
    """Run all LocalAwsClient tests."""
    divider("Testing LocalAwsClient Module")

    base_dir = get_data_directory()
    logger.info(f"Base directory: {base_dir}, Directory exists: {base_dir.exists()}")

    if not base_dir.exists():
        logger.error("Base data directory not found. Please ensure data is downloaded.")
        logger.error("You can use the AWS downloader to fetch the data first.")
        return

    # Test different configurations
    test_configs = [
        {
            "trade_type": TradeType.um_futures,
            "data_freq": DataFrequency.monthly,
            "data_type": DataType.funding_rate,
            "title": "LocalAwsClient test - UM futures monthly fundingRate",
        }
    ]

    # Test kline configurations
    kline_configs = [
        {
            "trade_type": TradeType.um_futures,
            "data_freq": DataFrequency.daily,
            "time_interval": "1m",
            "title": "LocalAwsKlineClient test - UM futures daily 1m klines",
        },
        {
            "trade_type": TradeType.spot,
            "data_freq": DataFrequency.daily,
            "time_interval": "1m",
            "title": "LocalAwsKlineClient test - Spot daily 1m klines",
        },
    ]

    # Run standard tests
    for config in test_configs:
        try:
            test_local_client(base_dir, config["trade_type"], config["data_freq"], config["data_type"], config["title"])
        except Exception as e:
            logger.exception(f"{config['title']} failed: {e}")

    # Run kline tests
    for config in kline_configs:
        try:
            test_local_kline_client(
                base_dir, config["trade_type"], config["data_freq"], config["time_interval"], config["title"]
            )
        except Exception as e:
            logger.exception(f"{config['title']} failed: {e}")

    divider("All tests completed")


if __name__ == "__main__":
    main()
