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
from bdt_common.log_kit import divider, logger
from bhds.holo_kline.gap_detector import HoloKlineGapDetector
from bhds.holo_kline.merger import Holo1mKlineMerger
from bhds.holo_kline.splitter import HoloKlineSplitter
from bhds.tasks.common import get_bhds_home


def test_splitter():
    """Test kline splitting for spot 1m LUNAUSDT klines"""
    # Test configuration
    parsed_data_dir = get_bhds_home(None) / "parsed_data"
    symbol = "LUNAUSDT"
    trade_type = TradeType.spot
    include_vwap = True
    include_funding = False  # Spot markets do not have funding rates

    divider("Testing HoloKlineSplitter")
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Trade type: {trade_type}")
    logger.info(f"VWAP: {include_vwap}, Funding: {include_funding}")

    # Create temporary output directory
    with tempfile.TemporaryDirectory(prefix="splitter_test_") as temp_dir:
        temp_path = Path(temp_dir)
        logger.info(f"Temp directory: {temp_path}")

        try:
            # Step 1: Generate holographic kline data
            divider(f"Generating kline data for {symbol}", sep="-")
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
            logger.ok(
                f"Generated: {len(df)} rows, "
                f"Date range: {df['candle_begin_time'].min()} to {df['candle_begin_time'].max()}"
            )

            # Step 2: Detect gaps
            divider("Detecting gaps", sep="-")
            detector = HoloKlineGapDetector(min_days=1, min_price_chg=0.1)

            gaps_ldf = detector.detect(kline_file)
            gaps_df = gaps_ldf.collect()

            logger.ok(f"Gaps detected: {len(gaps_df)}")

            # Print gap details
            if len(gaps_df) > 0:
                logger.info("Gap Details:")
                for idx, gap in enumerate(gaps_df.iter_rows(named=True), 1):
                    logger.debug(
                        f"Gap #{idx}: {gap['prev_begin_time']} -> {gap['candle_begin_time']}"
                        f" (duration: {gap['time_diff']})"
                    )

            # Step 3: Split kline data (memory API)
            divider("Memory API Test", sep="-")
            splitter = HoloKlineSplitter(prefix="SP")

            segments = splitter.split(df, gaps_df, symbol)

            if segments is None:
                logger.warning("No valid splits found")
                return

            for seg_symbol, seg_df in segments.items():
                start_time = seg_df["candle_begin_time"].min()
                end_time = seg_df["candle_begin_time"].max()
                logger.ok(f"{seg_symbol}: {len(seg_df)} rows, {start_time} to {end_time}")

            # Step 4: Test file API
            divider("File API Test", sep="-")
            output_files = splitter.split_file(kline_file, gaps_df)

            # Read each generated file and print info
            for file_path in output_files:
                seg_df = pl.read_parquet(file_path)
                start_time = seg_df["candle_begin_time"].min()
                end_time = seg_df["candle_begin_time"].max()
                logger.ok(f"{file_path.name}: {len(seg_df)} rows, {start_time} to {end_time}")

        except Exception as e:
            logger.exception(f"Error: {e}")

        logger.debug("Temp directory will be cleaned up automatically")

    divider("All tests completed")


if __name__ == "__main__":
    test_splitter()
