#!/usr/bin/env python3
"""
Test script for HoloKlineGapDetector

This script tests gap detection in holographic 1-minute kline data for:
- Spot 1m LUNAUSDT klines with gap analysis
"""

import tempfile
from pathlib import Path

import polars as pl

from bdt_common.enums import TradeType
from bdt_common.log_kit import divider, logger
from bhds.holo_kline.gap_detector import HoloKlineGapDetector
from bhds.holo_kline.merger import Holo1mKlineMerger
from bhds.tasks.common import get_bhds_home


def test_gap_detection():
    """Test gap detection for spot 1m LUNAUSDT klines"""
    # Test configuration
    parsed_data_dir = get_bhds_home(None) / "parsed_data"
    symbol = "LUNAUSDT"
    trade_type = TradeType.spot

    divider("Testing HoloKlineGapDetector")

    include_vwap = True
    include_funding = False  # Spot markets do not have funding rates

    logger.info(f"TradeType: {trade_type}, Symbol: {symbol}, VWAP={include_vwap}, Funding={include_funding}")
    logger.info(f"Input directory: {parsed_data_dir}, ")

    # Create temporary output directory
    with tempfile.TemporaryDirectory(prefix="gap_detector_test_") as temp_dir:
        temp_path = Path(temp_dir)
        logger.debug(f"Temp directory: {temp_path}, will be automatically cleaned up after test")

        try:
            # Step 1: Generate holographic kline data
            divider(f"Step 1: Generating holographic kline data {symbol}", sep="-")
            merger = Holo1mKlineMerger(
                trade_type=trade_type,
                base_dir=parsed_data_dir,
                include_vwap=include_vwap,
                include_funding=include_funding,
            )

            output_file = temp_path / f"{symbol}.parquet"
            ldf = merger.generate(symbol, output_file)

            # Collect the LazyFrame to trigger file writing
            ldf.collect()

            # Read and analyze the generated kline data
            df = pl.read_parquet(output_file)
            logger.info("Generated kline data:")
            logger.debug(f"     Shape: {df.shape}")
            logger.debug(f"     Time range: {df['candle_begin_time'].min()} to {df['candle_begin_time'].max()}")
            logger.debug(f"     Total records: {len(df)}")

            # Step 2: Detect gaps in the generated data
            divider(f"Step 2: Detecting gaps in kline data {symbol}", sep="-")

            # Configure gap detector parameters
            min_days = 1
            min_price_chg = 0.1  # 10% price change threshold

            detector = HoloKlineGapDetector(min_days=min_days, min_price_chg=min_price_chg)

            # Detect gaps
            gaps_ldf = detector.detect(output_file)
            gaps_df = gaps_ldf.collect()

            logger.info("Gap Detection Results:")
            logger.debug(f"     Min days: {min_days}")
            logger.debug(f"     Min price change: {min_price_chg * 100}%")
            logger.ok(f"Total gaps detected: {len(gaps_df)}")

            if len(gaps_df) > 0:
                logger.info("Gap Details:")

                # Sort gaps by duration (longest first)
                gaps_df = gaps_df.sort("time_diff", descending=True)

                for idx, gap in enumerate(gaps_df.iter_rows(named=True), 1):
                    logger.debug(f"Gap #{idx}:")
                    logger.debug(f"     Start time: {gap['prev_begin_time']}")
                    logger.debug(f"     End time: {gap['candle_begin_time']}")
                    logger.debug(f"     Duration: {gap['time_diff']}")
                    logger.debug(f"     Price change: {gap['price_change']:.2%}")
                    logger.debug(f"     Previous close: {gap['prev_close']}")
                    logger.debug(f"     Next open: {gap['open']}")

                # Summary statistics
                logger.info("Gap Statistics:")
                logger.debug(f"     Average gap duration: {gaps_df['time_diff'].mean()}")
                logger.debug(f"     Longest gap: {gaps_df['time_diff'].max()}")
                logger.debug(f"     Largest price change: {gaps_df['price_change'].abs().max():.2%}")
                logger.debug(
                    f"     Price change: {gaps_df['price_change'].min():.2%} to {gaps_df['price_change'].max():.2%}"
                )
            else:
                logger.ok("No gaps detected with current thresholds")

        except Exception as e:
            logger.exception(f"Error during gap detection: {e}")

        logger.debug("Temp directory will be cleaned up automatically")

    divider("All tests completed")


if __name__ == "__main__":
    test_gap_detection()
