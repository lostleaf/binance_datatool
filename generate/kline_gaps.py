from typing import Optional
import polars as pl


from datetime import timedelta

from util.time import convert_interval_to_timedelta


def scan_gaps(df: pl.DataFrame, min_days: int, min_price_chg: float) -> pl.DataFrame:
    """
    Scan for gaps in kline data that meet certain criteria.

    Args:
        df: Input DataFrame containing kline data
        min_days: Minimum number of days for a gap to be considered
        min_price_chg: Minimum price change ratio threshold for a gap to be considered

    Returns:
        DataFrame containing identified gaps with columns:
        - prev_begin_time: Start time of gap
        - candle_begin_time: End time of gap
        - prev_close: Close price before gap
        - open: Open price after gap
        - time_diff: Time duration of gap
        - price_change: Price change ratio over gap
    """
    ldf = df.lazy()

    ldf = ldf.with_columns(
        pl.col("candle_begin_time").diff().alias("time_diff"),
        (pl.col("open") / pl.col("close").shift() - 1).alias("price_change"),
        pl.col("candle_begin_time").shift().alias("prev_begin_time"),
        pl.col("close").shift().alias("prev_close"),
    )

    min_delta = timedelta(days=min_days)
    df_gap = ldf.filter((pl.col("time_diff") > min_delta) & (pl.col("price_change").abs() > min_price_chg))
    df_gap = df_gap.select("prev_begin_time", "candle_begin_time", "prev_close", "open", "time_diff", "price_change")

    return df_gap.collect()


def split_by_gaps(df: pl.DataFrame, df_gap: pl.DataFrame, symbol: str) -> Optional[dict[str, pl.DataFrame]]:
    """
    Split a DataFrame into segments based on gaps.

    Args:
        df: Original DataFrame to split
        df_gap: DataFrame containing gap information
        symbol: Trading pair symbol

    Returns:
        Dictionary mapping split symbol names to DataFrames, or None if no valid splits
    """
    # No gaps found, return original df
    if df_gap.is_empty():
        return {symbol: df}

    # Get gap start times
    gap_times = df_gap.get_column("candle_begin_time").to_list()

    # Split df at gap points
    dfs = []
    for i, gap_time in enumerate(gap_times):
        if i == 0:
            # First segment - from start to first gap
            split_df = df.filter(pl.col("candle_begin_time") < gap_time)
        else:
            # Middle segments - from previous gap to current gap
            prev_gap_time = gap_times[i - 1]
            split_df = df.filter(pl.col("candle_begin_time").is_between(prev_gap_time, gap_time, closed="left"))

        # Skip empty df
        if split_df.is_empty():
            continue

        dfs.append(split_df)

    # Add final segment after last gap
    final_df = df.filter(pl.col("candle_begin_time") >= gap_times[-1])
    if not final_df.is_empty():
        dfs.append(final_df)

    if not dfs:
        return None

    # Generate dict with split symbols using list comprehension
    result = {(f"SP{i}_{symbol}" if i < len(dfs) - 1 else symbol): df for i, df in enumerate(dfs)}

    return result


def fill_kline_gaps(df: pl.DataFrame, time_interval: str) -> pl.DataFrame:
    """
    Fill gaps between klines by adding rows with 0 volume and previous close price.

    Args:
        df: DataFrame containing kline data
        time_interval: Kline interval string (e.g. "1m", "5m", "1h", etc)

    Returns:
        DataFrame with gaps filled
    """
    if df.is_empty():
        return df

    # Create complete time series
    complete_times = pl.datetime_range(
        df["candle_begin_time"].min(),
        df["candle_begin_time"].max(),
        interval=convert_interval_to_timedelta(time_interval),
        time_zone="UTC",
        time_unit=df["candle_begin_time"].dtype.time_unit,
        eager=True,
    )

    # Create template df with all timestamps
    template_df = pl.LazyFrame({"candle_begin_time": complete_times})

    # Join with original data
    ldf = template_df.join(df.lazy(), on="candle_begin_time", how="left")

    # Fill prices with previous close
    ldf = ldf.with_columns(pl.col("close").fill_null(strategy="forward"))
    ldf = ldf.with_columns(
        pl.col("open").fill_null(pl.col("close")),
        pl.col("high").fill_null(pl.col("close")),
        pl.col("low").fill_null(pl.col("close")),
    )

    # Fill Vwaps with open
    if "avg_price_1m" in df.columns:
        ldf = ldf.with_columns(pl.col("avg_price_1m").fill_null(pl.col("open")))
        ldf = ldf.with_columns(pl.col("avg_price_1m").clip(pl.col("low"), pl.col("high")))
    
    if 'funding_rate' in df.columns:
        ldf = ldf.with_columns(pl.col("funding_rate").fill_null(0))

    # Fill volumes with 0
    ldf = ldf.with_columns(
        pl.col("volume").fill_null(0),
        pl.col("quote_volume").fill_null(0),
        pl.col("trade_num").fill_null(0),
        pl.col("taker_buy_base_asset_volume").fill_null(0),
        pl.col("taker_buy_quote_asset_volume").fill_null(0),
    )

    return ldf.collect()