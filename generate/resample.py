import polars as pl

from util.time import convert_interval_to_timedelta


def polars_calc_resample(df: pl.DataFrame, time_interval: str, resample_interval: str, offset_str: str) -> pl.DataFrame:
    """
    Resample a Polars kline DataFrame to a higher time frame with an offset.
    For example, resample 5-minute klines to hourly klines with a 5-minute offset.
    """
    # Convert the time intervals and offset from string to timedelta
    time_interval = convert_interval_to_timedelta(time_interval)
    resample_interval = convert_interval_to_timedelta(resample_interval)
    offset = convert_interval_to_timedelta(offset_str)

    # Create a lazy DataFrame for efficient computation
    ldf = df.lazy()

    # Add a new column for the end time of each kline
    ldf = ldf.with_columns((pl.col("candle_begin_time") + time_interval).alias("candle_end_time"))

    # Group the data by the start time of the klines, resampling to the specified interval with the given offset
    ldf = ldf.group_by_dynamic("candle_begin_time", every=resample_interval, offset=offset).agg(
        # Aggregate the data for each group
        pl.col("candle_begin_time").first().alias("candle_begin_time_real"),  # Real start time of the resampled kline
        pl.col("candle_end_time").last(),  # End time of the resampled kline
        pl.col("open").first(),  # Opening price of the resampled kline
        pl.col("high").max(),  # Highest price during the resampled period
        pl.col("low").min(),  # Lowest price during the resampled period
        pl.col("close").last(),  # Closing price of the resampled kline
        pl.col("volume").sum(),  # Total volume during the resampled period
        pl.col("quote_volume").sum(),  # Total quote volume during the resampled period
        pl.col("trade_num").sum(),  # Total number of trades during the resampled period
        pl.col("taker_buy_base_asset_volume").sum(),  # Total taker buy base asset volume during the resampled period
        pl.col("taker_buy_quote_asset_volume").sum(),  # Total taker buy quote asset volume during the resampled period
        pl.col("avg_price_1m").first(),  # Average price over the first minute of the resampled period
    )

    # Filter out klines that are shorter than the specified resample interval
    ldf = ldf.filter((pl.col("candle_end_time") - pl.col("candle_begin_time_real")) == resample_interval)

    # Drop the temporary columns used for calculations
    ldf = ldf.drop(["candle_begin_time_real", "candle_end_time"])

    # Collect the results into a DataFrame and return
    return ldf.collect()
