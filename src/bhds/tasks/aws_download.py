#!/usr/bin/env python3
"""
AWS Download Task

Downloads cryptocurrency data from Binance AWS historical data archives 
using configuration from YAML files. Supports both spot and futures markets
with configurable symbol filtering and data verification.
"""
import os
from itertools import chain
from pathlib import Path
from typing import Optional

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import divider, logger
from bdt_common.network import create_aiohttp_session
from bhds.tasks.common import create_symbol_filter_from_config
from bhds.aws.checksum import ChecksumVerifier
from bhds.aws.local import LocalAwsClient
from bhds.aws.client import AwsClient
from bhds.aws.path_builder import create_path_builder
from bhds.aws.downloader import AwsDownloader
from bhds.tasks.common import load_config, get_data_directory


def create_aws_client_from_config(
    trade_type: TradeType, data_type: DataType, aws_cfg: dict, session, http_proxy: Optional[str]
) -> AwsClient:
    """Instantiate proper AwsClient based on config."""

    data_freq = DataFrequency(aws_cfg["data_freq"])  # e.g. "daily", "monthly"
    time_interval = aws_cfg.get("time_interval") if data_type == DataType.kline else None

    path_builder = create_path_builder(
        trade_type=trade_type, data_freq=data_freq, data_type=data_type, time_interval=time_interval
    )

    return AwsClient(
        path_builder=path_builder,
        session=session,
        http_proxy=http_proxy,
    )


class AwsDownloadTask:
    def __init__(self, config: str | dict | Path):
        if isinstance(config, str) or isinstance(config, Path):
            self.config = load_config(config)
            logger.info(f"Loaded configuration from: {config}")
        else:
            self.config = config

        # Get top-level params
        self.data_dir = get_data_directory(self.config.get("data_dir"), "aws_data")
        self.http_proxy = self.config.get("http_proxy") or os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        logger.info(f"Data directory: {self.data_dir}, HTTP proxy: {self.http_proxy}")

        if "trade_type" not in self.config:
            raise KeyError("Missing 'trade_type' in config")
        self.trade_type = TradeType(self.config["trade_type"])  # e.g. "spot", "futures/um", "futures/cm"

        if "data_type" not in self.config:
            raise KeyError("Missing 'data_type' in config")
        self.data_type = DataType(self.config["data_type"])  # e.g. "klines"

        self.verification_config: dict = self.config.get("verification")

    def _apply_symbol_filter(self, all_symbols: list[str]) -> list[str]:
        filter_cfg = self.config.get("symbol_filter")

        if filter_cfg is None or not filter_cfg:
            logger.info("No symbol filtering applied, using all symbols")
            return all_symbols

        symbol_filter = create_symbol_filter_from_config(self.trade_type, filter_cfg)
        filtered_symbols = symbol_filter(all_symbols)
        return filtered_symbols

    def _get_target_symbols(self, all_symbols: list[str]) -> list[str]:
        """Return the final symbols to process.
        If config['symbols'] is provided and non-empty, use it; otherwise apply filter.
        """
        symbols = self.config.get("symbols")
        if symbols:
            valid = sorted(set(symbols).intersection(set(all_symbols)))
            logger.info(f"Using {len(valid)} user-specified symbols")
            return valid

        # Fallback to filter logic
        filtered = self._apply_symbol_filter(all_symbols)
        logger.info(f"Found {len(all_symbols)} total symbols, Filtered to {len(filtered)} symbols")
        return filtered

    async def _download_files(self, client: AwsClient, symbols: list[str]) -> None:
        """Download files for symbols."""
        logger.debug(f"\U0001F4CA Processing symbols: {symbols[:5]}{'...' if len(symbols) > 5 else ''}")

        files_map = await client.batch_list_data_files(symbols)
        all_files = sorted(chain.from_iterable(files_map.values()))
        if not all_files:
            logger.warning("No files found")
            return

        logger.debug(f"\U0001F4E5 Total files found: {len(all_files)}, downloading missings...")
        downloader = AwsDownloader(local_dir=self.data_dir, http_proxy=self.http_proxy, verbose=True)
        downloader.aws_download(all_files)

    def _verify_files(self, client: AwsClient, symbols: list[str]) -> None:
        """Verify checksums for downloaded files."""
        verifier = ChecksumVerifier(delete_mismatch=self.verification_config.get("delete_mismatch", False))

        # Use LocalAwsClient to get all unverified files
        local_client = LocalAwsClient(self.data_dir, client.path_builder)
        all_symbols_status = local_client.get_all_symbols_status()

        all_unverified_files = []
        for symbol_status in all_symbols_status.values():
            all_unverified_files.extend(symbol_status["unverified"])

        if all_unverified_files:
            logger.debug(f"\U0001F50D Verifying {len(all_unverified_files)} files")
            results = verifier.verify_files(all_unverified_files)
            logger.ok(f"Verification complete: {results['success']} success, {results['failed']} failed")
        else:
            logger.ok("All files already verified")

    async def run(self):
        divider("BHDS: Start Binance AWS Download", with_timestamp=True)
        async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
            client = create_aws_client_from_config(
                self.trade_type, self.data_type, self.config["aws_client"], session, self.http_proxy
            )

            logger.debug("\U0001F50D Fetching available symbols...")
            all_symbols = await client.list_symbols()

            target_symbols = self._get_target_symbols(all_symbols)

            if not target_symbols:
                logger.warning("\u26A0\uFE0F  No symbols to process after filtering")
                return

            await self._download_files(client, target_symbols)
            if self.verification_config:
                self._verify_files(client, target_symbols)

        divider("BHDS: Binance AWS Download Completed", with_timestamp=True)
