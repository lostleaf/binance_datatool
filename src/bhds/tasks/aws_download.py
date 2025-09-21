#!/usr/bin/env python3
"""
BHDS AWS Download Task.

This module implements the download workflow to fetch historical market data from Binance's official AWS data center.
It loads a YAML config, resolves symbols, downloads missing files, and optionally verifies checksums.
"""
import os
from itertools import chain
from pathlib import Path

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import divider, logger
from bdt_common.network import create_aiohttp_session
from bhds.aws.checksum import ChecksumVerifier
from bhds.aws.client import AwsClient, create_aws_client_from_config
from bhds.aws.downloader import AwsDownloader
from bhds.aws.local import LocalAwsClient
from bhds.tasks.common import create_symbol_filter_from_config, get_bhds_home, load_config


class AwsDownloadTask:
    """
    Coordinates downloading historical Binance data from the official AWS data center for the public bhds application.

    The task loads config, filters target symbols, downloads missing archives, and optionally verifies checksums.
    """
    def __init__(self, config_path: str | Path):
        """Initialize the download task from a YAML configuration.

        Args:
            config_path: Path to the YAML config file.

        Raises:
            KeyError: If required fields (trade_type, data_type, data_freq) are missing.
        """
        self.config = load_config(config_path)
        logger.info(f"Loaded configuration from: {config_path}")

        # Get top-level params
        bhds_home = get_bhds_home(self.config.get("bhds_home"))
        self.aws_data_dir = bhds_home / "aws_data"
        self.http_proxy = self.config.get("http_proxy") or os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        self.use_proxy_for_aria2c = self.config.get("use_proxy_for_aria2c", False)
        logger.info(
            f"Data directory: {self.aws_data_dir}, HTTP proxy: {self.http_proxy}, "
            f"Use proxy for aria2c: {self.use_proxy_for_aria2c}"
        )

        if "trade_type" not in self.config:
            raise KeyError("Missing 'trade_type' in config")
        self.trade_type = TradeType(self.config["trade_type"])  # e.g. "spot", "futures/um", "futures/cm"

        if "data_type" not in self.config:
            raise KeyError("Missing 'data_type' in config")
        self.data_type = DataType(self.config["data_type"])  # e.g. "klines"

        if "data_freq" not in self.config:
            raise KeyError("Missing 'data_freq' in config")
        self.data_freq = DataFrequency(self.config["data_freq"])  # e.g. "daily", "monthly"

        self.verification_config: dict = self.config.get("checksum_verification")

    def _apply_symbol_filter(self, all_symbols: list[str]) -> list[str]:
        """Apply the configured symbol filter to the full symbol list.

        Args:
            all_symbols: All available symbols returned by the AWS client.

        Returns:
            The filtered list of symbols. If no filter is configured, returns the input list unchanged.
        """
        filter_cfg = self.config.get("symbol_filter")

        if filter_cfg is None or not filter_cfg:
            logger.info("No symbol filtering applied, using all symbols")
            return all_symbols

        symbol_filter = create_symbol_filter_from_config(self.trade_type, filter_cfg)
        filtered_symbols = symbol_filter(all_symbols)
        return filtered_symbols

    def _get_target_symbols(self, all_symbols: list[str]) -> list[str]:
        """Determine the final set of symbols to process.

        If config['symbols'] is provided and non-empty, intersect it with the available symbols.
        Otherwise, apply the configured symbol filter.

        Args:
            all_symbols: All available symbols returned by the AWS client.

        Returns:
            A sorted list of target symbols to be processed.
        """
        symbols = self.config.get("symbols")
        if symbols:
            valid = sorted(set(symbols).intersection(set(all_symbols)))
            logger.info(f"Using {len(valid)} user-specified symbols")
            return valid

        # Fallback to configured symbol filter
        filtered = self._apply_symbol_filter(all_symbols)
        logger.info(f"Found {len(all_symbols)} total symbols, Filtered to {len(filtered)} symbols")
        return filtered

    async def _download_files(self, client: AwsClient, symbols: list[str]) -> None:
        """Download data files for the given symbols.

        Lists files via the AWS client, then downloads only the missing files into aws_data_dir.
        Optionally routes aria2c traffic through an HTTP proxy when enabled.

        Args:
            client: AWS data client used to list and build file paths.
            symbols: Symbols to download.
        """
        logger.debug(f"📊 Processing symbols: {symbols[:5]}{'...' if len(symbols) > 5 else ''}")

        files_map = await client.batch_list_data_files(symbols)
        all_files = sorted(chain.from_iterable(files_map.values()))
        if not all_files:
            logger.warning("No files found")
            return

        logger.debug(f"📥 Total files found: {len(all_files)}, downloading missings...")
        # Only pass http_proxy to AwsDownloader if use_proxy_for_aria2c is True
        proxy_for_downloader = self.http_proxy if self.use_proxy_for_aria2c else None
        downloader = AwsDownloader(local_dir=self.aws_data_dir, http_proxy=proxy_for_downloader, verbose=True)
        downloader.aws_download(all_files)

    def _verify_files(self, client: AwsClient) -> None:
        """Verify checksums for downloaded files and optionally delete mismatches.

        Args:
            client: AWS data client (used for path building when scanning local files).
        """
        divider("BHDS: Verifying Downloaded Files", sep="-")

        verifier = ChecksumVerifier(delete_mismatch=self.verification_config.get("delete_mismatch", False))

        # Use LocalAwsClient to get all unverified files
        local_client = LocalAwsClient(self.aws_data_dir, client.path_builder)
        all_symbols_status = local_client.get_all_symbols_status()

        all_unverified_files = []
        for symbol_status in all_symbols_status.values():
            all_unverified_files.extend(symbol_status["unverified"])

        if all_unverified_files:
            logger.debug(f"🔍 Verifying {len(all_unverified_files)} files")
            results = verifier.verify_files(all_unverified_files)
            logger.ok(f"Verification complete: {results['success']} success, {results['failed']} failed")
        else:
            logger.ok("All files already verified")

    async def run(self):
        """Execute the end-to-end download workflow.

        Steps:
            1. Create HTTP session.
            2. Create AWS client from config.
            3. List available symbols and compute targets.
            4. Download missing files.
            5. Optionally verify checksums.
        """
        divider("BHDS: Start Binance AWS Download", with_timestamp=True)
        async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
            client = create_aws_client_from_config(
                self.trade_type,
                self.data_type,
                self.data_freq,
                self.config.get("time_interval"),
                session,
                self.http_proxy,
            )

            logger.debug("🔍 Fetching available symbols...")
            all_symbols = await client.list_symbols()

            target_symbols = self._get_target_symbols(all_symbols)

            if not target_symbols:
                logger.warning("⚠️ No symbols to process after filtering")
                return

            await self._download_files(client, target_symbols)
            if self.verification_config:
                self._verify_files(client)

        divider("BHDS: Binance AWS Download Completed", with_timestamp=True)
