#!/usr/bin/env python3
"""
Test script for Holo1mKlineMerger

This script tests the holographic 1-minute kline data synthesis for:
- UM futures 1m klines with VWAP and funding rate

Test symbols: BTCUSDT, ETHUSDT
"""

import random
import tempfile
from pathlib import Path

import polars as pl

from bdt_common.enums import TradeType
from bdt_common.log_kit import divider, logger
from bdt_common.polars_utils import execute_polars_batch
from bhds.holo_kline.merger import Holo1mKlineMerger


def test_holo_merger_generate(symbol: str, trade_type: TradeType):
    """Test the Holo1mKlineMerger

    Args:
        symbol: Trading symbol to test
        trade_type: TradeType enum (spot, um_futures, cm_futures)
                   For futures types, include_funding=True
                   For spot type, include_funding=False
    """
    # Test configuration
    parsed_data_dir = Path.home() / "crypto_data" / "binance_data" / "parsed_data"

    include_vwap = True
    # Set include_funding based on trade_type
    include_funding = trade_type in [TradeType.um_futures, TradeType.cm_futures]

    divider(f"TradeType: {trade_type}, symbol: {symbol}, VWAP: {include_vwap}, Funding: {include_funding}", sep="-")
    logger.info(f"Input directory: {parsed_data_dir}")

    # Create temporary output directory
    with tempfile.TemporaryDirectory(prefix="holo_merger_test_") as temp_dir:
        temp_path = Path(temp_dir)
        logger.debug(f"Temp directory: {temp_path}, will be automatically cleaned up after test")

        try:
            # Create merger
            merger = Holo1mKlineMerger(
                trade_type=trade_type,
                base_dir=parsed_data_dir,
                include_vwap=include_vwap,
                include_funding=include_funding,
            )

            # Test generate method for each symbol
            output_file = temp_path / f"{symbol}.parquet"
            ldf = merger.generate(symbol, output_file)

            # Collect and check results
            ldf.collect()
            df = pl.read_parquet(output_file)
            logger.debug(f"{symbol}:")
            logger.debug(f"     Shape: {df.shape}")
            logger.debug(f"     Time range: {df['candle_begin_time'].min()} to {df['candle_begin_time'].max()}")
            logger.debug(f"     Time max diff: {df['candle_begin_time'].diff().abs().max()}")
            logger.debug(f"     Volume zero count: {df.filter(pl.col('volume') == 0).shape}")
            logger.debug(f"     Total records: {len(df)}")

            # Check for missing values
            null_counts = df.null_count()
            if null_counts.sum().sum_horizontal().item(0) > 0:
                logger.warning(f"     Null values found: {null_counts.to_dict()}")

            # Check VWAP and funding rate columns
            if include_vwap:
                logger.debug(f"     VWAP range: {df['vwap_1m'].min():.2f} - {df['vwap_1m'].max():.2f}")
            if include_funding:
                logger.debug(f"     Funding rate range: {df['funding_rate'].min()} - {df['funding_rate'].max()}")
        except Exception as e:
            logger.exception(f"Error: {e}")


def test_holo_merger_generate_all():
    """Test the Holo1mKlineMerger generate_all method for CM futures"""
    # Test configuration
    parsed_data_dir = Path.home() / "crypto_data" / "binance_data" / "parsed_data"
    trade_type = TradeType.cm_futures

    include_vwap = True
    include_funding = True  # CM futures include funding

    logger.info(f"TradeType: {trade_type}, VWAP: {include_vwap}, Funding:{include_funding}")
    logger.info(f"Input directory: {parsed_data_dir}")

    # Create temporary output directory
    with tempfile.TemporaryDirectory(prefix="holo_merger_all_test_") as temp_dir:
        temp_path = Path(temp_dir)
        logger.debug(f"Temp directory: {temp_path}")

        try:
            # Create merger
            merger = Holo1mKlineMerger(
                trade_type=trade_type,
                base_dir=parsed_data_dir,
                include_vwap=include_vwap,
                include_funding=include_funding,
            )

            # Test generate_all method
            logger.info("Running generate_all for all CM futures symbols...")
            lazy_frames = merger.generate_all(temp_path)

            logger.info("Results Summary:")
            logger.debug(f"     Total symbols processed: {len(lazy_frames)}")

            # Trigger actual file writing by collecting LazyFrames
            logger.info("Collecting LazyFrames to trigger file writing...")
            execute_polars_batch(lazy_frames, "Collecting kline data")
            generated_files = list(temp_path.glob("*.parquet"))
            collected_symbols = list()
            for output_file in generated_files:
                try:
                    # Collect the LazyFrame to trigger file writing
                    symbol = output_file.stem
                    df = pl.read_parquet(output_file)
                    logger.ok(f"{symbol}: Collected {len(df)} records")
                    collected_symbols.append(symbol)
                except Exception as e:
                    logger.error(f"{symbol}: Error collecting - {e}")

            # Check each result
            logger.info("Detailed Results:")
            random.shuffle(collected_symbols)

            for symbol in collected_symbols[:3]:
                output_file = temp_path / f"{symbol}.parquet"
                df = pl.read_parquet(output_file)

                # Check if DataFrame has data
                if len(df) == 0 or df.shape[1] == 0:
                    continue

                logger.debug(f"{symbol}:")
                logger.debug(f"     Shape: {df.shape}")

                # Check if required columns exist
                if "candle_begin_time" in df.columns:
                    logger.debug(f"     Time range: {df['candle_begin_time'].min()} to {df['candle_begin_time'].max()}")
                else:
                    logger.warning(f"     Missing candle_begin_time column")

                logger.debug(f"     Total records: {len(df)}")
                logger.debug(f"     Time max diff: {df['candle_begin_time'].diff().abs().max()}")
                logger.debug(f"     Volume zero count: {df.filter(pl.col('volume') == 0).shape}")

                # Check for missing values
                null_counts = df.null_count()
                if null_counts.sum().sum_horizontal().item(0) > 0:
                    logger.warning(f"     Null values found: {null_counts.to_dict()}")

                # Check VWAP and funding rate columns
                if include_vwap and "vwap_1m" in df.columns:
                    logger.debug(f"     VWAP range: {df['vwap_1m'].min():.2f} - {df['vwap_1m'].max():.2f}")
                if include_funding and "funding_rate" in df.columns:
                    logger.debug(
                        f"     Funding rate range: {df['funding_rate'].min():.6f} - {df['funding_rate'].max():.6f}"
                    )

            logger.info("Samples Summary:")
            logger.debug(f"     Total symbols: {len(collected_symbols)}")

        except Exception as e:
            logger.exception(f"Error: {e}")


def main():
    """Main function to run all tests"""
    # Test with different trade types
    divider("Testing UM Futures (include_funding=True)")
    test_holo_merger_generate("BTCUSDT", TradeType.um_futures)
    test_holo_merger_generate("ETHUSDT", TradeType.um_futures)

    divider("Testing CM Futures (include_funding=True)")
    test_holo_merger_generate("BTCUSD_PERP", TradeType.cm_futures)

    divider("Testing Spot (include_funding=False)")
    test_holo_merger_generate("BTCUSDT", TradeType.spot)
    test_holo_merger_generate("ETHUSDT", TradeType.spot)

    divider("Testing generate_all for CM Futures")
    test_holo_merger_generate_all()

    divider("All tests completed")


if __name__ == "__main__":
    main()
