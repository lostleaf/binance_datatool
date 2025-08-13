#!/usr/bin/env python3
"""
Comprehensive test for checksum functionality including verification and file management.

Tests:
1. AWS file discovery and download
2. SHA256 checksum calculation and verification
3. ChecksumVerifier class functionality
4. AwsDataFileManager for tracking verification status
"""

import asyncio
import os
import random
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, TradeType
from bdt_common.network import create_aiohttp_session
from bhds.aws.checksum import ChecksumVerifier, calc_checksum, read_checksum
from bhds.aws.local import AwsDataFileManager
from bhds.aws.client import AwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder
from bhds.aws.downloader import AwsDownloader


async def collect_target_files(
    http_proxy: str | None, trade_type: TradeType, symbols: list[str], files_per_symbol: int
) -> list[Path]:
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:

        """Collect target AWS files for testing."""
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

        # List symbols and validate targets
        available_symbols = await client.list_symbols()
        print(f"Total available symbols: {len(available_symbols)}")

        valid_targets = [s for s in symbols if s in available_symbols]
        if not valid_targets:
            raise ValueError(f"None of {symbols} found in available symbols")

        print(f"Valid target symbols: {valid_targets}")

        # Collect files for each target symbol
        files_map = await client.batch_list_data_files(valid_targets)

    aws_files: list[Path] = []

    for symbol in valid_targets:
        files = files_map.get(symbol, [])
        zip_files = [p for p in files if p.name.endswith(".zip")]
        random.shuffle(zip_files)
        if not zip_files:
            print(f"No .zip files found for {symbol}, skipping.")
            continue

        selected = zip_files[:files_per_symbol]
        for selected_file in selected:
            aws_files.append(selected_file)
            aws_files.append(selected_file.parent / f"{selected_file.name}.CHECKSUM")

        print(f"Selected {len(selected)} files for {symbol}: {[f.name for f in selected]}")

    return aws_files


async def test_checksum_functionality():
    """Main test function for checksum functionality."""
    print("=" * 80)
    print("Starting comprehensive checksum test")
    print("=" * 80)

    # Check prerequisites
    if shutil.which("aria2c") is None:
        print("❌ aria2c not found in PATH. Please install aria2 to run this test.")
        return

    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")

    print("\n🔍 Step 1: Discovering AWS files...")
    try:
        target_symbols = ["BTCUSDT", "ETHUSDT"]
        aws_files = await collect_target_files(
            http_proxy=http_proxy,
            trade_type=TradeType.um_futures,
            symbols=target_symbols,
            files_per_symbol=2,
        )
    except Exception as e:
        print(f"❌ Failed to discover AWS files: {e}")
        return

    if not aws_files:
        print("❌ No files to test. Exiting.")
        return

    print(f"✅ Found {len(aws_files)} files to test")

    # Step 2: Download files to temporary directory
    print("\n📥 Step 2: Downloading files...")
    with TemporaryDirectory(prefix="bhds_checksum_test_") as tmpdir:
        local_dir = Path(tmpdir)
        print(f"Temporary directory: {local_dir}")

        # Ensure parent directories exist
        for aws_path in aws_files:
            (local_dir / aws_path).parent.mkdir(parents=True, exist_ok=True)

        downloader = AwsDownloader(local_dir=local_dir, http_proxy=http_proxy, verbose=True)
        try:
            downloader.aws_download(aws_files, max_tries=2)
        except Exception as e:
            print(f"❌ Download failed: {e}")
            return

        # Step 3: Test checksum calculation and verification
        print("\n🔐 Step 3: Testing checksum functionality...")

        # Get downloaded files
        downloaded_files = [local_dir / aws_path for aws_path in aws_files]

        # Test individual file checksum calculation
        print("\n📊 Individual checksum calculations:")
        for local_file in downloaded_files:
            if local_file.exists() and local_file.name.endswith(".zip"):
                checksum_file = local_file.with_suffix(local_file.suffix + ".CHECKSUM")

                if checksum_file.exists():
                    try:
                        calculated = calc_checksum(local_file)
                        expected = read_checksum(checksum_file)
                        match = calculated == expected

                        print(f"  📁 {local_file.name}")
                        print(f"     Calculated: {calculated}")
                        print(f"     Expected:   {expected}")
                        print(f"     Status:     {'✅ MATCH' if match else '❌ MISMATCH'}")
                    except Exception as e:
                        print(f"  ❌ Error processing {local_file.name}: {e}")
                else:
                    print(f"  ⚠️  No checksum file: {checksum_file.name}")

        # Step 4: Test AwsDataFileManager
        print("\n📁 Step 4: Testing AwsDataFileManager...")

        # Collect all unique parent directories
        target_dirs = set()
        for aws_path in aws_files:
            target_dir = local_dir / aws_path.parent
            if target_dir.exists():
                target_dirs.add(target_dir)
        
        if not target_dirs:
            print("❌ No valid directories found for testing AwsDataFileManager")
            return
            
        print(f"\n  Found {len(target_dirs)} unique directories to test:")
        for target_dir in sorted(target_dirs):
            print(f"    📂 {target_dir}")

        # Process each directory
        all_unverified_files = []
        for target_dir in sorted(target_dirs):
            print(f"\n  Testing directory: {target_dir}")
            
            manager = AwsDataFileManager(target_dir)
            verified_files, unverified_files = manager.get_files()

            print(f"    Initial verified files: {len(verified_files)}")
            for vf in verified_files:
                print(f"      ✅ {vf.name}")

            print(f"    Initial unverified files: {len(unverified_files)}")
            for uvf in unverified_files:
                print(f"      ⚪ {uvf.name}")
                
            # Collect all unverified files for batch processing
            all_unverified_files.extend(unverified_files)

        # Step 5: Test ChecksumVerifier on all unverified files
        print("\n🔍 Step 5: Testing ChecksumVerifier...")
        verifier = ChecksumVerifier(delete_mismatch=False, n_jobs=2)

        # Test single file verification on first unverified file
        if all_unverified_files:
            print("\n  Single file verification:")
            first_unverified = all_unverified_files[0]
            try:
                success = verifier.verify_file(first_unverified)
                print(f"    {first_unverified.name}: {'✅ Verified' if success else '❌ Failed'}")
            except Exception as e:
                print(f"    {first_unverified.name}: ❌ Error - {e}")

        # Test batch verification on all unverified files
        print("\n  Batch verification:")
        if all_unverified_files:
            results = verifier.verify_files(all_unverified_files)
            print(f"    Total files: {results['total_files']}")
            print(f"    Successful: {results['success']}")
            print(f"    Failed: {results['failed']}")

            if results["errors"]:
                print("    Errors:")
                for file_path, error in results["errors"].items():
                    print(f"      {file_path.name}: {error}")

        # Step 6: Final verification status for all directories
        print("\n📋 Step 6: Final verification status...")
        for target_dir in sorted(target_dirs):
            print(f"\n  Final status for: {target_dir}")
            
            # Refresh the manager to get updated status
            manager = AwsDataFileManager(target_dir)
            final_verified_files, final_unverified_files = manager.get_files()

            print(f"    Final verified files: {len(final_verified_files)}")
            for vf in final_verified_files:
                print(f"      ✅ {vf.name}")

            print(f"    Final unverified files: {len(final_unverified_files)}")
            for uvf in final_unverified_files:
                print(f"      ⚪ {uvf.name}")

        print("\n" + "=" * 80)
        print("✅ Checksum test completed successfully!")
        print("=" * 80)


async def main():
    """Entry point."""
    try:
        await test_checksum_functionality()
    except KeyboardInterrupt:
        print("\n❌ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
