import polars as pl

from config.config import BINANCE_DATA_DIR, TradeType
from util.time import convert_interval_to_timedelta
from datetime import timedelta

def polars_calc_resample(df: pl.DataFrame, time_interval: str, resample_interval: str, offset: str | timedelta) -> pl.DataFrame:
    """
    Resample a Polars kline DataFrame to a higher time frame with an offset.
    For example, resample 5-minute klines to hourly klines with a 5-minute offset.

    Args:
        df: Polars kline DataFrame
        time_interval: Time interval of the klines
        resample_interval: Time interval to resample to
        offset_str: Offset to apply to the resampled klines

    Returns:
        Polars kline DataFrame
    """
    # Convert the time intervals and offset from string to timedelta
    time_interval = convert_interval_to_timedelta(time_interval)
    resample_interval = convert_interval_to_timedelta(resample_interval)

    if isinstance(offset, str):
        offset = convert_interval_to_timedelta(offset)

    # Create a lazy DataFrame for efficient computation
    ldf = df.lazy()

    # Add a new column for the end time of each kline
    ldf = ldf.with_columns((pl.col("candle_begin_time") + time_interval).alias("candle_end_time"))

    # Aggregation rules
    agg = [
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
    ]

    if "avg_price_1m" in df.columns:
        # Average price over the first minute of the resampled period
        agg.append(pl.col("avg_price_1m").first())

    if 'funding_rate' in df.columns:
        # Funding rate at the start of the resampled period
        agg.append(pl.col('funding_rate').first())

    # Group the data by the start time of the klines, resampling to the specified interval with the given offset
    ldf = ldf.group_by_dynamic("candle_begin_time", every=resample_interval, offset=offset).agg(agg)

    # Filter out klines that are shorter than the specified resample interval
    ldf = ldf.filter((pl.col("candle_end_time") - pl.col("candle_begin_time_real")) == resample_interval)

    # Drop the temporary columns used for calculations
    ldf = ldf.drop(["candle_begin_time_real", "candle_end_time"])

    # Collect the results into a DataFrame and return
    return ldf.collect()


def resample_kline(trade_type: TradeType, symbol: str, resample_interval: str, base_offset: str):
    """
    Resample a kline DataFrame to a higher time frame with an offset.
    """
    time_interval = "1m"
    results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value

    spot_1m_kline_dir = results_dir / "klines" / time_interval
    kline_file = spot_1m_kline_dir / f"{symbol}.pqt"
    if not kline_file.exists():
        return

    # Calculate base offset interval
    base_delta = convert_interval_to_timedelta(base_offset)
    resample_delta = convert_interval_to_timedelta(resample_interval)

    # Ensure base offset is smaller than resample interval
    if base_delta >= resample_delta:
        return

    if base_offset == '0m':
        # If base offset is 0m, there is only one possible offset
        num_offsets = 1 
    else:
        # Calculate number of possible offsets
        num_offsets = resample_delta // base_delta

    df = pl.read_parquet(kline_file)

    # Generate resampled data for each offset
    for i in range(num_offsets):
        offset_str = f'{i * int(base_offset[:-1])}{base_offset[-1]}'
        
        # Create output directory for this offset
        resampled_kline_dir = results_dir / "resampled_klines" / resample_interval / offset_str
        resampled_kline_dir.mkdir(parents=True, exist_ok=True)

        # Read and resample data
        df_resampled = polars_calc_resample(df, time_interval, resample_interval, offset_str)
        df_resampled.write_parquet(resampled_kline_dir / f"{symbol}.pqt")
