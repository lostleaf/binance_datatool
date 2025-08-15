#!/usr/bin/env python3
"""
Test script for HoloKlineSplitter

This script tests kline data splitting based on detected gaps for:
- Spot 1m LUNAUSDT klines with gap-based splitting
"""

import tempfile
from pathlib import Path
import polars as pl

from bdt_common.enums import TradeType
from bhds.holo_kline.merger import Holo1mKlineMerger
from bhds.holo_kline.gap_detector import HoloKlineGapDetector
from bhds.holo_kline.splitter import HoloKlineSplitter


def test_splitter():
    """Test kline splitting for spot 1m LUNAUSDT klines"""
    # Test configuration
    parsed_data_dir = Path.home() / "crypto_data" / "binance_data" / "parsed_data"
    symbol = "LUNAUSDT"
    trade_type = TradeType.spot

    print("=" * 60)
    print("Testing HoloKlineSplitter")
    print("=" * 60)
    print(f"Symbol: {symbol}")
    print(f"Trade type: {trade_type}")

    include_vwap = True
    include_funding = False  # Spot markets do not have funding rates

    print(f"VWAP: {include_vwap}, Funding: {include_funding}")
    print("-" * 40)

    # Create temporary output directory
    with tempfile.TemporaryDirectory(prefix="splitter_test_") as temp_dir:
        temp_path = Path(temp_dir)
        print(f"Temp directory: {temp_path}")

        try:
            # Step 1: Generate holographic kline data
            print(f"\nGenerating kline data for {symbol}...")
            merger = Holo1mKlineMerger(
                trade_type=trade_type,
                base_dir=parsed_data_dir,
                include_vwap=include_vwap,
                include_funding=include_funding,
            )

            kline_file = temp_path / f"{symbol}.parquet"
            ldf = merger.generate(symbol, kline_file)
            ldf.collect()  # Write to file

            # Read generated data
            df = pl.read_parquet(kline_file)
            print(f"Generated: {len(df)} rows, {df['candle_begin_time'].min()} to {df['candle_begin_time'].max()}")

            # Step 2: Detect gaps
            print(f"\nDetecting gaps...")
            detector = HoloKlineGapDetector(min_days=1, min_price_chg=0.1)

            gaps_ldf = detector.detect(kline_file)
            gaps_df = gaps_ldf.collect()

            print(f"Gaps detected: {len(gaps_df)}")

            # Print gap details
            if len(gaps_df) > 0:
                print(f"\n=== Gap Details ===")
                for idx, gap in enumerate(gaps_df.iter_rows(named=True), 1):
                    print(
                        f"Gap #{idx}: {gap['prev_begin_time']} -> {gap['candle_begin_time']}"
                        f" (duration: {gap['time_diff']})"
                    )

            # Step 3: Split kline data (memory API)
            print(f"\n=== Memory API Test ===")
            splitter = HoloKlineSplitter(prefix="SP")

            segments = splitter.split(df, gaps_df, symbol)

            if segments is None:
                print("No valid splits found")
                return

            for seg_symbol, seg_df in segments.items():
                start_time = seg_df["candle_begin_time"].min()
                end_time = seg_df["candle_begin_time"].max()
                print(f"{seg_symbol}: {len(seg_df)} rows, {start_time} to {end_time}")

            # Step 4: Test file API
            print(f"\n=== File API Test ===")
            output_files = splitter.split_file(kline_file, gaps_df)

            # Read each generated file and print info
            for file_path in output_files:
                seg_df = pl.read_parquet(file_path)
                start_time = seg_df["candle_begin_time"].min()
                end_time = seg_df["candle_begin_time"].max()
                print(f"{file_path.name}: {len(seg_df)} rows, {start_time} to {end_time}")

        except Exception as e:
            print(f"Error: {e}")
            import traceback

            traceback.print_exc()

        print(f"\nTemp directory will be cleaned up automatically")


if __name__ == "__main__":
    test_splitter()
