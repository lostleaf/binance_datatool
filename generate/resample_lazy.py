import shutil
import time
from datetime import timedelta

import polars as pl
from tqdm import tqdm

from config.config import BINANCE_DATA_DIR, TradeType
from generate.util import list_results_kline_symbols
from util.log_kit import divider, logger
from util.time import convert_interval_to_timedelta


def polars_calc_resample_lazy(
    lf: pl.LazyFrame,
    time_interval: str,
    resample_interval: str,
    offset: str | timedelta,
    schema: dict[str, pl.DataType],
) -> pl.LazyFrame:
    """
    Resample a Polars kline LazyFrame to a higher time frame with an offset.
    Returns a LazyFrame for delayed execution.

    Args:
        lf: Polars kline LazyFrame
        time_interval: Time interval of the klines
        resample_interval: Time interval to resample to
        offset: Offset to apply to the resampled klines
        schema: Schema of the input kline DataFrame

    Returns:
        Polars kline LazyFrame
    """
    # Convert the time intervals and offset from string to timedelta
    time_interval = convert_interval_to_timedelta(time_interval)
    resample_interval = convert_interval_to_timedelta(resample_interval)

    if isinstance(offset, str):
        offset = convert_interval_to_timedelta(offset)

    # Add a new column for the end time of each kline
    ldf = lf.with_columns(
        (pl.col("candle_begin_time") + time_interval).alias("candle_end_time"),
    )

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

    # Check for avg_price_1m column
    if "avg_price_1m" in schema:
        # Average price over the first minute of the resampled period
        agg.append(pl.col("avg_price_1m").first())

    if "funding_rate" in schema:
        # Only consider funding rates with absolute value greater than 0.01 bps
        has_funding_cond = pl.col("funding_rate").abs() > 1e-6

        # Get the first valid funding rate and its corresponding price and time
        agg.extend(
            [
                pl.col("funding_rate").filter(has_funding_cond).first().alias("funding_rate"),
                pl.col("open").filter(has_funding_cond).first().alias("funding_price"),
                pl.col("candle_begin_time").filter(has_funding_cond).first().alias("funding_time"),
            ]
        )

    # Group the data by the start time of the klines, resampling to the specified interval with the given offset
    ldf = ldf.group_by_dynamic("candle_begin_time", every=resample_interval, offset=offset).agg(agg)

    # Filter out klines that are shorter than the specified resample interval
    ldf = ldf.filter((pl.col("candle_end_time") - pl.col("candle_begin_time_real")) == resample_interval)

    # Drop the temporary columns used for calculations
    ldf = ldf.drop(["candle_begin_time_real", "candle_end_time"])

    return ldf


def process_symbols_batch(
    trade_type: TradeType, symbols: list[str], resample_interval: str, base_offset: str
) -> list[pl.LazyFrame]:
    """
    Create LazyFrame processing pipeline for all symbols and sink to parquet files.

    Args:
        trade_type: Trade type (spot, um_futures, cm_futures)
        symbols: List of symbol names
        resample_interval: Time interval to resample to
        base_offset: Base offset for resampling

    Returns:
        List of LazyFrames that will be executed by sink_parquet
    """
    time_interval = "1m"
    results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value
    resampled_dir = results_dir / "resampled_klines" / resample_interval

    # Calculate offset intervals
    base_delta = convert_interval_to_timedelta(base_offset)
    resample_delta = convert_interval_to_timedelta(resample_interval)

    if base_offset == "0m":
        num_offsets = 1
    else:
        num_offsets = resample_delta // base_delta

    lazy_frames = []

    for symbol in symbols:
        kline_file = results_dir / "klines" / time_interval / f"{symbol}.parquet"
        if not kline_file.exists():
            continue

        # Scan file as LazyFrame
        lf = pl.scan_parquet(kline_file)
        schema = pl.read_parquet_schema(kline_file)

        # Create LazyFrame for each offset and sink to file
        for i in range(num_offsets):
            offset_str = f"{i * int(base_offset[:-1])}{base_offset[-1]}"

            # Create output directory for this offset
            offset_dir = resampled_dir / offset_str
            offset_dir.mkdir(parents=True, exist_ok=True)

            # Create resampled LazyFrame with offset
            resampled_lf = polars_calc_resample_lazy(lf, time_interval, resample_interval, offset_str, schema)

            # Sink to parquet file (lazy execution)
            output_file = offset_dir / f"{symbol}.parquet"
            resampled_lf = resampled_lf.sink_parquet(output_file, lazy=True)

            # Add the sink operation to lazy frames list
            lazy_frames.append(resampled_lf)

    return lazy_frames


def resample_kline_type(trade_type: TradeType, resample_interval: str, base_offset: str):
    """
    Resample kline data for all symbols of a given trade type using LazyFrame batch processing.

    Args:
        trade_type: Trade type (spot, um_futures, cm_futures)
        resample_interval: Time interval to resample to
        base_offset: Base offset for resampling
    """
    divider(f"Resample kline {trade_type.value} {resample_interval} {base_offset}")
    symbols = list_results_kline_symbols(trade_type, "1m")

    resampled_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "resampled_klines" / resample_interval
    logger.info(f"Resampled kline directory: {resampled_dir}")

    if resampled_dir.exists():
        logger.warning(f"Resampled kline directory exists, removing it")
        shutil.rmtree(resampled_dir)

    start_time = time.perf_counter()

    # Create and execute all LazyFrames with sink_parquet
    lazy_frames = process_symbols_batch(trade_type, symbols, resample_interval, base_offset)

    time_elapsed = time.perf_counter() - start_time
    logger.ok(f"Finished creating resampled tasks in {time_elapsed:.2f}s")

    if not lazy_frames:
        logger.warning("No valid data to process")
        return

    logger.info(f"Executing {len(lazy_frames)} resample tasks")
    start_time = time.perf_counter()

    # Execute all sink operations
    batch_size = 288
    with tqdm(total=len(lazy_frames), desc="Resample klines", unit="task") as pbar:
        for i in range(0, len(lazy_frames), batch_size):
            batch = lazy_frames[i:i+batch_size]
            pl.collect_all(batch)
            pbar.update(len(batch))

    time_elapsed = (time.perf_counter() - start_time) / 60
    logger.ok(f"Finished in {time_elapsed:.2f}mins")


def resample_kline(trade_type: TradeType, symbol: str, resample_interval: str, base_offset: str):
    """
    Resample a kline DataFrame to a higher time frame with an offset.
    This is an internal function for single symbol processing.

    Args:
        trade_type: Trade type (spot, um_futures, cm_futures)
        symbol: Symbol name
        resample_interval: Time interval to resample to
        base_offset: Base offset for resampling
    """
    divider(f"Resample kline {trade_type.value} {symbol} {resample_interval} {base_offset}")
    time_start = time.perf_counter()

    # Directly use process_symbols_batch for single symbol
    lazy_frames = process_symbols_batch(trade_type, [symbol], resample_interval, base_offset)

    if lazy_frames:
        # Execute sink operations for this single symbol
        pl.collect_all(lazy_frames)

    logger.ok(f"Finished in {time.perf_counter() - time_start:.2f}s")
