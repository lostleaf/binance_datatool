import asyncio
import os
import random
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, TradeType
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
    print(f"Total symbols: {len(symbols)}")
    target_candidates = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}
    targets = list(target_candidates.intersection(symbols))
    if not targets:
        raise ValueError(f"No symbols of {target_candidates} found")
    else:
        targets = targets[:max_symbols]
    print(f"Selected symbols: {targets}")

    files_map = await client.batch_list_data_files(targets)

    # Collect a few .zip files per symbol
    aws_files: list[Path] = []
    for sym in targets:
        files = files_map.get(sym, [])
        zip_files = [p for p in files if p.name.endswith(".zip")]
        if not zip_files:
            print(f"No .zip files found for {sym}, skipping.")
            continue
        aws_files.extend(zip_files[:files_per_symbol])

    print(f"Collected {len(aws_files)} aws files to download")
    for p in aws_files:
        print(f"  - {p}")

    return aws_files


async def test_aws_downloader_1m_klines():
    # Check aria2c availability early
    if shutil.which("aria2c") is None:
        print("aria2c not found in PATH. Please install aria2 to run this test.")
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
            print(f"Failed to list files from AWS: {e}")
            return

        if not aws_files:
            print("No files to download. Exiting test.")
            return

        with TemporaryDirectory(prefix="bhds_dl_") as tmpdir:
            local_dir = Path(tmpdir)
            print(f"Temporary download dir: {local_dir}")

            # Ensure parent directories exist (aria2 usually creates them, but we prepare them to be safe)
            for p in aws_files:
                (local_dir / p).parent.mkdir(parents=True, exist_ok=True)

            downloader = AwsDownloader(local_dir=local_dir, http_proxy=http_proxy, verbose=True)
            try:
                downloader.aws_download(aws_files, max_tries=2)
            except Exception as e:
                print(f"Downloader raised exception: {e}")
                return

            # Verify all files exist locally
            missing = []
            for p in aws_files:
                local_file = local_dir / p
                if not local_file.exists():
                    missing.append(str(local_file))
            if missing:
                print("Some files are missing after download:")
                for m in missing:
                    print(f"  MISSING: {m}")
            else:
                print("All files downloaded successfully.")

            # Print details of successfully downloaded files
            print("Downloaded files detail:")
            for p in aws_files:
                local_file = local_dir / p
                if local_file.exists():
                    try:
                        size = local_file.stat().st_size
                    except Exception:
                        size = -1
                    print(f"  OK: {p} -> {local_file} (size={size} bytes)")

            # TemporaryDirectory context will clean up automatically
            print("Cleaning up temporary directory.")


async def main():
    await test_aws_downloader_1m_klines()


if __name__ == "__main__":
    asyncio.run(main())
