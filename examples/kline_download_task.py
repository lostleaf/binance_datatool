#!/usr/bin/env python3
"""
K-line Download Task Example

Demonstrates downloading spot 1m kline data with USDT symbols (excluding stable/leverage tokens)
using AWS client, downloader, and checksum verification.
"""
import asyncio
import os
import sys
from itertools import chain
from pathlib import Path

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, TradeType
from bdt_common.network import create_aiohttp_session
from bdt_common.symbol_filter import SpotFilter
from bhds.aws.checksum import AwsDataFileManager, ChecksumVerifier
from bhds.aws.client import AwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder
from bhds.aws.downloader import AwsDownloader


async def download_and_verify_klines(client: AwsClient, data_dir: Path, symbols, http_proxy):
    """Download kline data for given symbols and verify checksums."""
    data_dir = Path(data_dir)

    downloader = AwsDownloader(local_dir=data_dir, http_proxy=http_proxy, verbose=True)
    verifier = ChecksumVerifier(delete_mismatch=False)

    print(f"\n📊 Processing {len(symbols)} symbols: {symbols}")

    # Batch list all data files
    files_map = await client.batch_list_data_files(symbols)

    # Collect all files to download
    all_files = sorted(chain.from_iterable(files_map.values()))
    if not all_files:
        print("⚠️  No files found")
        return

    print(f"📥 Total files to download: {len(all_files)}")

    # Download all files at once
    downloader.aws_download(all_files)

    # Verify all symbols
    all_unverified_files = []
    for symbol in symbols:
        symbol_dir = data_dir / str(client.get_symbol_dir(symbol))
        if symbol_dir.exists():
            manager = AwsDataFileManager(symbol_dir)
            all_unverified_files.extend(manager.get_unverified_files())

    verifier.verify_files(all_unverified_files)


def create_aws_client(session, http_proxy):
    path_builder = AwsKlinePathBuilder(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        time_interval="1m",
    )
    return AwsClient(
        path_builder=path_builder,
        session=session,
        http_proxy=http_proxy,
    )


async def main(data_dir: str):
    """Main download task."""
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        client = create_aws_client(session, http_proxy)

        # Get all symbols and filter USDT (no stable/leverage)
        all_symbols = await client.list_symbols()

        usdt_symbols = SpotFilter(quote="USDT", stable_pairs=False, leverage_tokens=False)(all_symbols)
        await download_and_verify_klines(client, Path(data_dir), usdt_symbols[:2], http_proxy)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
