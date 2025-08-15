"""
Holo1mKlineMerger - Holographic 1-minute kline data synthesizer

Reads official complete 1-minute kline data from parsed AWS data, optionally adds
VWAP and funding rate fields, and fills missing kline gaps to ensure time series continuity.
"""

from pathlib import Path
from typing import Dict

import polars as pl

from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import logger
from bhds.aws.path_builder import AwsKlinePathBuilder, AwsPathBuilder


class Holo1mKlineMerger:
    """Holo_1m_kline generator - holographic 1-minute kline data synthesizer"""

    def __init__(
        self,
        trade_type: TradeType,
        base_dir: Path,
        include_vwap: bool,
        include_funding: bool,
    ):
        """
        Initialize Holo1mKlineMerger

        Args:
            trade_type: Trading type (spot, um_futures, cm_futures)
            base_dir: parsed_data root directory
            include_vwap: Whether to include VWAP
            include_funding: Whether to include funding rate

        Raises:
            ValueError: If spot type includes funding rate
        """
        if trade_type == TradeType.spot and include_funding:
            raise ValueError("Spot kline cannot include funding rates")

        self.trade_type = trade_type
        self.base_dir = base_dir
        self.include_vwap = include_vwap
        self.include_funding = include_funding

        # Initialize path builders
        self.kline_builder = AwsKlinePathBuilder(
            trade_type=trade_type, data_freq=DataFrequency.daily, time_interval="1m"
        )

        self.funding_builder = AwsPathBuilder(
            trade_type=trade_type, data_freq=DataFrequency.monthly, data_type=DataType.funding_rate
        )

    def generate(self, symbol: str, output_path: Path) -> pl.LazyFrame:
        """
        Generate holo_1m_kline for a single symbol and save

        Args:
            symbol: Trading pair symbol (e.g. "BTCUSDT")
            output_path: Output file path

        Returns:
            pl.LazyFrame: Processed LazyFrame
        """
        # Use path_builder to build correct paths
        kline_dir = self.base_dir / self.kline_builder.get_symbol_dir(symbol)

        if not kline_dir.exists():
            raise FileNotFoundError(f"Kline directory not found: {kline_dir}")

        # Read and deduplicate 1-minute kline data, Filter out zero volume klines
        ldf = (
            pl.scan_parquet(kline_dir)
            .filter(pl.col("volume") > 0)
            .unique("candle_begin_time")
            .sort("candle_begin_time")
        )

        # Add VWAP (optional, clipped to [low, high])
        if self.include_vwap:
            vwap_expr = (
                pl.when(pl.col("volume") > 0)
                .then((pl.col("quote_volume") / pl.col("volume")).clip(pl.col("low"), pl.col("high")))
                .otherwise(pl.col("open"))
                .alias("vwap_1m")
            )
            ldf = ldf.with_columns(vwap_expr)

        # Add funding rate (optional, only for futures)
        if self.include_funding and self.trade_type != TradeType.spot:
            funding_dir = self.base_dir / self.funding_builder.get_symbol_dir(symbol)
            if funding_dir.exists():
                funding_ldf = pl.scan_parquet(funding_dir).unique("candle_begin_time")
                ldf = ldf.join(
                    funding_ldf.select(["candle_begin_time", "funding_rate"]), on="candle_begin_time", how="left"
                ).with_columns(pl.col("funding_rate").fill_null(0))
            else:
                logger.warning(f"Funding directory not found for {symbol}: {funding_dir}")
                self.include_funding = False

        # Fill kline gaps (ensure time continuity)
        ldf = self._fill_kline_gaps(ldf)

        # Save result (maintain LazyFrame)
        return ldf.sink_parquet(output_path, lazy=True)

    def generate_all(self, output_dir: Path) -> Dict[str, pl.LazyFrame]:
        """
        Generate holo_1m_kline for all symbols in batch

        Args:
            output_dir: Output directory

        Returns:
            Dict[str, pl.LazyFrame]: Symbol to LazyFrame mapping
        """
        # Get all symbols from kline directory
        kline_base_dir = self.base_dir / self.kline_builder.base_dir
        symbols = [d.name for d in kline_base_dir.iterdir() if d.is_dir()]

        results = {}
        for symbol in symbols:
            output_path = output_dir / f"{symbol}.parquet"
            ldf = self.generate(symbol, output_path)
            results[symbol] = ldf

        return results

    def _fill_kline_gaps(self, ldf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Fill missing data in 1-minute kline time series

        Based on legacy fill_kline_gaps implementation, adapted for 1-minute interval
        """
        # Get time range
        bounds = ldf.select(
            pl.col("candle_begin_time").min().alias("min_time"),
            pl.col("candle_begin_time").max().alias("max_time"),
        )

        # Generate complete 1-minute calendar
        calendar = bounds.select(
            pl.datetime_range(
                pl.col("min_time").first(),
                pl.col("max_time").first(),
                interval="1m",
                time_zone="UTC",
                eager=False,
            ).alias("candle_begin_time")
        )

        # Left join with original data
        result_ldf = calendar.join(ldf, on="candle_begin_time", how="left", maintain_order="left")

        # Fill missing data
        result_ldf = result_ldf.with_columns(pl.col("close").fill_null(strategy="forward"))
        result_ldf = result_ldf.with_columns(
            pl.col("open").fill_null(pl.col("close")),
            pl.col("high").fill_null(pl.col("close")),
            pl.col("low").fill_null(pl.col("close")),
        )

        # Fill volume fields
        result_ldf = result_ldf.with_columns(
            pl.col("volume").fill_null(0),
            pl.col("quote_volume").fill_null(0),
            pl.col("trade_num").fill_null(0),
            pl.col("taker_buy_base_asset_volume").fill_null(0),
            pl.col("taker_buy_quote_asset_volume").fill_null(0),
        )

        # Fill enhanced fields
        if self.include_vwap:
            result_ldf = result_ldf.with_columns(pl.col("vwap_1m").fill_null(pl.col("open")))

        if self.include_funding:
            result_ldf = result_ldf.with_columns(pl.col("funding_rate").fill_null(0))

        return result_ldf
