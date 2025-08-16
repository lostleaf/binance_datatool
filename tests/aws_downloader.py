import asyncio
import os
import random
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, TradeType
from bdt_common.log_kit import logger, divider
from bdt_common.network import create_aiohttp_session
from bhds.aws.client import AwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder
from bhds.aws.downloader import AwsDownloader


async def collect_1m_kline_files(
    session, http_proxy: str | None, trade_type: TradeType, max_symbols: int, files_per_symbol: int
):
    path_builder = AwsKlinePathBuilder(
        trade_type=trade_type,
        data_freq=DataFrequency.daily,
        time_interval="1m",
    )
    client = AwsClient(
        path_builder=path_builder,
        session=session,
        http_proxy=http_proxy,
    )

    # List symbols and pick target ones
    symbols = await client.list_symbols()
    target_candidates = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}
    targets = list(target_candidates.intersection(symbols))
    if not targets:
        raise ValueError(f"No symbols of {target_candidates} found")
    else:
        targets = targets[:max_symbols]
    logger.info(f"Total symbols: {len(symbols)}, Selected: {targets}")

    files_map = await client.batch_list_data_files(targets)

    # Collect a few .zip files per symbol
    aws_files: list[Path] = []
    for sym in targets:
        files = files_map.get(sym, [])
        zip_files = [p for p in files if p.name.endswith(".zip")]
        if not zip_files:
            logger.warning(f"No .zip files found for {sym}, skipping.")
            continue
        aws_files.extend(zip_files[:files_per_symbol])

    logger.info(f"Collected {len(aws_files)} aws files to download")
    for p in aws_files:
        logger.debug(f"  - {p}")

    return aws_files


async def test_aws_downloader_1m_klines():
    divider("Testing AWS Downloader")

    # Check aria2c availability early
    if shutil.which("aria2c") is None:
        logger.error("aria2c not found in PATH. Please install aria2 to run this test.")
        return

    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        # Collect a small set of 1m kline files from UM futures
        try:
            aws_files = await collect_1m_kline_files(
                session,
                http_proxy,
                trade_type=TradeType.um_futures,
                max_symbols=3,
                files_per_symbol=2,
            )
        except Exception as e:
            logger.exception(f"Failed to list files from AWS: {e}")
            return

        if not aws_files:
            logger.warning("No files to download. Exiting test.")
            return

        divider("Step 1: Downloading files", sep="-")

        with TemporaryDirectory(prefix="bhds_dl_") as tmpdir:
            local_dir = Path(tmpdir)
            logger.info(f"Temporary download dir: {local_dir}")

            # Ensure parent directories exist (aria2 usually creates them, but we prepare them to be safe)
            for p in aws_files:
                (local_dir / p).parent.mkdir(parents=True, exist_ok=True)

            downloader = AwsDownloader(local_dir=local_dir, http_proxy=http_proxy, verbose=True)
            try:
                downloader.aws_download(aws_files, max_tries=2)
            except Exception as e:
                logger.exception(f"Downloader raised exception: {e}")
                return

            divider("Step 2: Verifying downloads", sep="-")

            # Verify all files exist locally
            missing = []
            for p in aws_files:
                local_file = local_dir / p
                if not local_file.exists():
                    missing.append(str(local_file))
            if missing:
                logger.error("Some files are missing after download:")
                for m in missing:
                    logger.error(f"  MISSING: {m}")
            else:
                logger.ok("All files downloaded successfully.")

            # Print details of successfully downloaded files
            logger.info("Downloaded files detail:")
            for p in aws_files:
                local_file = local_dir / p
                if local_file.exists():
                    try:
                        size = local_file.stat().st_size
                    except Exception:
                        size = -1
                    logger.debug(f"{p} -> {local_file} (size={size // 1024}KB)")

            # TemporaryDirectory context will clean up automatically
            logger.info("Cleaning up temporary directory.")

    divider("All tests completed")


if __name__ == "__main__":
    asyncio.run(test_aws_downloader_1m_klines())
