#!/usr/bin/env python3
"""
Parse AWS Data Task

Parses downloaded AWS cryptocurrency market data from CSV to optimized Parquet format
with optional API completion for missing historical data. Supports both klines and
funding rates data types with configurable symbol filtering.
"""
import os
from pathlib import Path
from typing import List

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import divider, logger
from bdt_common.network import create_aiohttp_session
from bdt_common.rest_api.fetcher import BinanceFetcher
from bhds.tasks.common import create_symbol_filter_from_config
from bhds.api.completion.detector import create_detector
from bhds.api.completion.executor import DataExecutor
from bhds.aws.csv_conv import AwsCsvToParquetConverter
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import create_path_builder
from bhds.tasks.common import get_data_directory, load_config


class ParseAwsDataTask:
    """
    Task for parsing AWS downloaded data from CSV to Parquet with optional API completion.

    Supports:
    - klines: Candlestick data (OHLCV)
    - fundingRate: Funding rates for perpetual futures

    Features:
    - CSV to Parquet conversion
    - Optional API completion for missing data
    - Configurable symbol filtering
    - Custom directory support
    - HTTP proxy support
    """

    def __init__(self, config: str | dict | Path):
        if isinstance(config, str) or isinstance(config, Path):
            self.config = load_config(str(config))
            logger.info(f"Loaded configuration from: {config}")
        else:
            self.config = config

        # Get top-level params
        self.aws_data_dir = get_data_directory(self.config.get("aws_data_dir"), "aws_data")
        self.output_dir = get_data_directory(self.config.get("output_dir"), "parsed_data")
        self.http_proxy = self.config.get("http_proxy") or os.getenv("HTTP_PROXY") or os.getenv("http_proxy")

        if "trade_type" not in self.config:
            raise KeyError("Missing 'trade_type' in config")
        self.trade_type = TradeType(self.config["trade_type"])

        if "data_type" not in self.config:
            raise KeyError("Missing 'data_type' in config")
        self.data_type = DataType(self.config["data_type"])
        self.data_freq = DataFrequency(self.config["data_freq"])

        logger.info(
            f"Trade type: {self.trade_type.value}, "
            f"Data type: {self.data_type.value}, "
            f"Data frequency: {self.data_freq.value}"
        )
        logger.info(f"AWS data directory: {self.aws_data_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"HTTP proxy: {self.http_proxy}")

        self.enable_completion = self.config.get("enable_completion", False)
        self.force_update = self.config.get("force_update", False)

        # Validate frequency constraints
        self._validate_frequency_constraints()

    def _validate_frequency_constraints(self):
        """Validate frequency constraints for API completion."""
        if not self.enable_completion:
            return

        if self.data_type == DataType.kline and self.data_freq == DataFrequency.monthly:
            logger.warning(
                "Monthly klines do not support API completion, " "disabling completion for this configuration"
            )
            self.enable_completion = False

        if self.data_type == DataType.funding_rate and self.data_freq != DataFrequency.monthly:
            logger.warning(
                "Funding rates only support monthly frequency for API completion, "
                "disabling completion for this configuration"
            )
            self.enable_completion = False

    def _apply_symbol_filter(self, all_symbols: List[str]) -> List[str]:
        """Apply symbol filtering based on config."""
        symbols = self.config.get("symbols")
        if symbols:
            valid = sorted(set(symbols).intersection(set(all_symbols)))
            logger.info(f"Using {len(valid)} user-specified symbols")
            return valid

        # Fallback to filter logic
        filter_cfg = self.config.get("symbol_filter")
        if filter_cfg is None or not filter_cfg:
            logger.info("No symbol filtering applied, using all symbols")
            return all_symbols

        symbol_filter = create_symbol_filter_from_config(self.trade_type, filter_cfg)
        filtered_symbols = symbol_filter(all_symbols)
        logger.info(f"Found {len(all_symbols)} total symbols, filtered to {len(filtered_symbols)} symbols")
        return filtered_symbols

    def _convert_csv_to_parquet(self) -> List[str]:
        """Convert AWS CSV files to optimized Parquet format."""
        logger.debug("Converting AWS CSV files to Parquet...")

        # Create local AWS client for source data
        path_builder = create_path_builder(
            trade_type=self.trade_type,
            data_freq=self.data_freq,
            data_type=self.data_type,
            time_interval=self.config.get("time_interval"),
        )
        local_aws_client = LocalAwsClient(base_dir=self.aws_data_dir, path_builder=path_builder)

        # Get available symbols
        all_symbols = local_aws_client.list_symbols()
        if not all_symbols:
            logger.warning("No symbols found in AWS data directory")
            return []

        target_symbols = self._apply_symbol_filter(all_symbols)
        if not target_symbols:
            logger.warning("No symbols to process after filtering")
            return []

        # Create CSV to Parquet converter
        converter = AwsCsvToParquetConverter(
            local_aws_client=local_aws_client,
            data_type=self.data_type,
            output_base_dir=self.output_dir,
            force_update=self.force_update,
            verbose=True,
        )

        # Process symbols
        logger.debug(f"Processing {len(target_symbols)} symbols for CSV→Parquet conversion")
        results = converter.process_symbols(target_symbols)

        # Log results
        total_processed = sum(r["processed_files"] for r in results.values())
        total_skipped = sum(r["skipped_files"] for r in results.values())
        total_failed = sum(r["failed_files"] for r in results.values())

        logger.ok(
            f"CSV→Parquet conversion complete: "
            f"{total_processed} processed, {total_skipped} skipped, {total_failed} failed"
        )

        return target_symbols

    async def _detect_and_complete_missing_data(self, symbols: List[str]):
        """Detect missing data and complete via API if enabled."""
        if not self.enable_completion:
            logger.debug("API completion disabled, skipping missing data detection")
            return

        logger.debug("Detecting missing data for API completion...")

        # Create appropriate detector using factory method
        try:
            detector = create_detector(
                data_type=self.data_type,
                trade_type=self.trade_type,
                base_dir=str(self.output_dir),
                interval=self.config.get("time_interval"),
            )
        except ValueError as e:
            logger.warning(str(e))
            return

        # Detect missing data
        tasks = detector.detect(symbols)
        if not tasks:
            logger.ok("No missing data detected")
            return

        logger.info(f"Detected {len(tasks)} missing data tasks for API completion")

        # Execute completion tasks
        async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
            fetcher = BinanceFetcher(trade_type=self.trade_type, session=session, http_proxy=self.http_proxy)
            executor = DataExecutor(fetcher)

            logger.debug("Executing API completion tasks...")
            result = await executor.execute(tasks)

            logger.ok(f"API completion completed: " f"{result['success']}/{result['total']} successful tasks")

    async def run(self):
        """Execute the complete parse task."""
        divider(f"BHDS: Start Parse AWS {self.data_type.value}", with_timestamp=True)

        try:
            # Step 1: Convert CSV to Parquet
            processed_symbols = self._convert_csv_to_parquet()

            if not processed_symbols:
                logger.warning("No symbols processed, skipping API completion")
                return

            # Step 2: Optional API completion
            if self.enable_completion:
                await self._detect_and_complete_missing_data(processed_symbols)
            else:
                logger.info("API completion disabled - task complete")

        except Exception as e:
            logger.exception(f"Error during parse task: {e}")
            raise

        divider(f"BHDS: Parse AWS {self.data_type.value} Completed", with_timestamp=True)
