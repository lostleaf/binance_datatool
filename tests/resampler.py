#!/usr/bin/env python3
"""
Test script for HoloKlineResampler

This script tests kline data resampling functionality for:
- UM Futures 1m BTCUSDT klines resampled to higher intervals
"""

import tempfile
from pathlib import Path

import polars as pl

from bdt_common.enums import TradeType
from bdt_common.log_kit import logger, divider
from bhds.holo_kline.merger import Holo1mKlineMerger
from bhds.holo_kline.resampler import HoloKlineResampler


def test_resampler():
    """Test kline resampling for UM Futures 1m BTCUSDT klines"""
    # Test configuration
    parsed_data_dir = Path.home() / "crypto_data" / "binance_data" / "parsed_data"
    symbol = "BTCUSDT"
    trade_type = TradeType.um_futures

    divider("Testing HoloKlineResampler")
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Trade type: {trade_type}")

    include_vwap = True
    include_funding = True

    logger.debug(f"VWAP: {include_vwap}, Funding: {include_funding}")

    # Create temporary output directory
    with tempfile.TemporaryDirectory(prefix="resampler_test_") as temp_dir:
        temp_path = Path(temp_dir)
        logger.debug(f"Temp directory: {temp_path}")

        try:
            # Step 1: Generate holographic kline data
            logger.info("Generating 1m kline data...")
            merger = Holo1mKlineMerger(
                trade_type=trade_type,
                base_dir=parsed_data_dir,
                include_vwap=include_vwap,
                include_funding=include_funding,
            )

            kline_file = temp_path / f"{symbol}_1m.parquet"
            ldf = merger.generate(symbol, kline_file)
            ldf.collect()  # Write to file

            # Read generated data
            df = pl.read_parquet(kline_file)
            logger.info(
                f"Generated 1m klines: {len(df)} rows, "
                f"Date range: {df['candle_begin_time'].min()} to {df['candle_begin_time'].max()}"
            )

            # Step 2: Test resample to 1h
            divider("Test 1: Resample to 1h", sep="-")
            resampler_1h = HoloKlineResampler("1h")

            resampled_1h = resampler_1h.resample(pl.scan_parquet(kline_file))
            resampled_1h_df = resampled_1h.collect()

            logger.ok(f"1h resampled: {len(resampled_1h_df)} rows")
            if len(resampled_1h_df) > 0:
                min_cbt = resampled_1h_df["candle_begin_time"].min()
                max_cbt = resampled_1h_df["candle_begin_time"].max()
                logger.debug(f"Shape: {resampled_1h_df.shape}, Date range: {min_cbt} to {max_cbt}")
                df_sample = pl.concat([resampled_1h_df.head(2), resampled_1h_df.tail(2)])
                logger.debug(f"Sample data:\n{df_sample}")

            # Step 3: Test resample with 30m offset
            divider("Test 2: Resample to 1h with 30m offset", sep="-")
            resampled_30m_offset = resampler_1h.resample(pl.scan_parquet(kline_file), offset="30m")
            resampled_30m_offset_df = resampled_30m_offset.collect()

            logger.ok(f"1h with 30m offset: {len(resampled_30m_offset_df)} rows")
            if len(resampled_30m_offset_df) > 0:
                min_cbt = resampled_30m_offset_df["candle_begin_time"].min()
                max_cbt = resampled_30m_offset_df["candle_begin_time"].max()
                logger.debug(f"Shape: {resampled_30m_offset_df.shape}, Date range: {min_cbt} to {max_cbt}")
                df_sample = pl.concat([resampled_30m_offset_df.head(2), resampled_30m_offset_df.tail(2)])
                logger.debug(f"Sample data:\n{df_sample}")

            # Step 4: Test resample_offsets with 5m base_offset
            divider("Test 3: Resample_offsets with 5m base_offset", sep="-")
            offset_results = resampler_1h.resample_offsets(pl.scan_parquet(kline_file), base_offset="5m")

            logger.ok(f"Generated {len(offset_results)} offset variations")
            infos = []
            for offset_str, offset_ldf in offset_results.items():
                offset_df = offset_ldf.collect()
                if len(offset_df) > 0:
                    min_cbt = offset_df["candle_begin_time"].min()
                    max_cbt = offset_df["candle_begin_time"].max()
                    max_cet = offset_df["candle_end_time"].max()
                    infos.append(
                        {
                            "offset": offset_str,
                            "rows": len(offset_df),
                            "min_begin_time": min_cbt,
                            "max_begin_time": max_cbt,
                            "max_end_time": max_cet,
                        }
                    )
            logger.debug(pl.DataFrame(infos))

            divider("All tests completed")

        except Exception as e:
            logger.error(f"Error: {e}")
            import traceback

            traceback.print_exc()

        logger.debug("Temp directory will be cleaned up automatically")


if __name__ == "__main__":
    test_resampler()
