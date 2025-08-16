from datetime import timedelta
from typing import Dict

import polars as pl

from bdt_common.time import convert_interval_to_timedelta


class HoloKlineResampler:
    """
    Resampler for converting 1m kline data to higher time frames.

    This class provides functionality to resample 1-minute kline data to higher
    time frames with configurable offsets, supporting both spot and futures data.
    """

    def __init__(self, resample_interval: str | timedelta):
        """
        Initialize the resampler.

        Args:
            resample_interval: Target resampling interval (e.g., "5m", "1h", "1d")
        """
        if isinstance(resample_interval, str):
            self.resample_interval = convert_interval_to_timedelta(resample_interval)
        elif isinstance(resample_interval, timedelta):
            self.resample_interval = resample_interval

    def resample(
        self,
        ldf: pl.LazyFrame,
        offset: str | timedelta = "0m",
        schema: Dict[str, pl.DataType] | None = None,
    ) -> pl.LazyFrame:
        """
        Resample 1m kline data to the configured interval.

        Args:
            lf: Input 1m kline LazyFrame
            offset: Time offset for resampling, supports string or timedelta
            schema: Schema of the input DataFrame (optional for performance)

        Returns:
            Resampled LazyFrame
        """
        if schema is None:
            schema = ldf.collect_schema()

        # Convert time intervals to timedelta
        time_interval = convert_interval_to_timedelta("1m")

        if isinstance(offset, str):
            offset = convert_interval_to_timedelta(offset)

        # Add candle end time column
        ldf = ldf.with_columns(
            (pl.col("candle_begin_time") + time_interval).alias("candle_end_time"),
        )

        # Define aggregation rules
        agg = [
            pl.col("candle_begin_time").first().alias("candle_begin_time_real"),
            pl.col("candle_end_time").last(),
            pl.col("open").first(),
            pl.col("high").max(),
            pl.col("low").min(),
            pl.col("close").last(),
            pl.col("volume").sum(),
            pl.col("quote_volume").sum(),
            pl.col("trade_num").sum(),
            pl.col("taker_buy_base_asset_volume").sum(),
            pl.col("taker_buy_quote_asset_volume").sum(),
        ]

        # Handle vwap_1m column (corrected from vwap1m)
        if "vwap_1m" in schema:
            agg.append(pl.col("vwap_1m").first().alias("vwap_1m_open"))

        # Handle funding_rate column
        if "funding_rate" in schema:
            has_funding_cond = pl.col("funding_rate").abs() > 1e-6
            agg.extend(
                [
                    pl.col("funding_rate").filter(has_funding_cond).first().alias("funding_rate"),
                    pl.col("open").filter(has_funding_cond).first().alias("funding_price"),
                    pl.col("candle_begin_time").filter(has_funding_cond).first().alias("funding_time"),
                ]
            )

        # Group by dynamic time windows with offset
        ldf = ldf.group_by_dynamic(
            "candle_begin_time",
            every=self.resample_interval,
            offset=offset,
        ).agg(agg)

        # Filter out incomplete intervals
        duration_match = (pl.col("candle_end_time") - pl.col("candle_begin_time")) == self.resample_interval
        not_last_row = pl.col("_idx") != pl.col("_idx").max()
        ldf = ldf.with_row_index(name="_idx").filter(not_last_row | duration_match).drop("_idx")

        # Drop temporary columns
        ldf = ldf.drop("candle_begin_time")
        ldf = ldf.rename({"candle_begin_time_real": "candle_begin_time"})

        return ldf

    def resample_offsets(
        self,
        ldf: pl.LazyFrame,
        base_offset: str,
        schema: Dict[str, pl.DataType] | None = None,
    ) -> Dict[str, pl.LazyFrame]:
        """
        Generate resampled data for multiple offsets based on base_offset.

        Args:
            lf: Input 1m kline LazyFrame
            base_offset: Base offset for generating multiple resampling points
            schema: Schema of the input DataFrame (optional for performance)

        Returns:
            Dictionary mapping offset strings to resampled LazyFrames

        Raises:
            ValueError: If base_offset is "0m"
        """
        if base_offset == "0m":
            raise ValueError("base_offset cannot be '0m', use resample() method instead")

        if schema is None:
            schema = ldf.collect_schema()

        base_delta = convert_interval_to_timedelta(base_offset)

        # Calculate number of offsets
        num_offsets = self.resample_interval // base_delta

        # Generate offset strings and resampled frames
        results = {}
        base_num = int(base_offset[:-1])
        base_unit = base_offset[-1]

        for i in range(num_offsets):
            offset_str = f"{i * base_num}{base_unit}"
            results[offset_str] = self.resample(ldf, offset=offset_str, schema=schema)

        return results
