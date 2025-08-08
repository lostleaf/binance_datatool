import multiprocessing as mp
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
from functools import partial

import polars as pl
from tqdm import tqdm

from config.config import BINANCE_DATA_DIR, N_JOBS, TradeType
from generate.util import list_results_kline_symbols
from util.concurrent import mp_env_init
from util.log_kit import divider, logger
from util.time import convert_interval_to_timedelta


def polars_calc_resample(
    df: pl.DataFrame, time_interval: str, resample_interval: str, offset: str | timedelta
) -> pl.DataFrame:
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

    if "funding_rate" in df.columns:
        # Only consider funding rates with absolute value greater than 0.01 bps
        has_funding_cond = pl.col("funding_rate").abs() > 1e-6

        # Get the first valid funding rate and its corresponding price and time
        agg.extend([
            pl.col("funding_rate").filter(has_funding_cond).first().alias("funding_rate"),
            pl.col("open").filter(has_funding_cond).first().alias("funding_price"),
            pl.col("candle_begin_time").filter(has_funding_cond).first().alias("funding_time")
        ])

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

    kline_file = results_dir / "klines" / time_interval / f"{symbol}.parquet"
    if not kline_file.exists():
        return

    # Calculate base offset interval
    base_delta = convert_interval_to_timedelta(base_offset)
    resample_delta = convert_interval_to_timedelta(resample_interval)

    # Ensure base offset is smaller than resample interval
    if base_delta >= resample_delta:
        return

    if base_offset == "0m":
        # If base offset is 0m, there is only one possible offset
        num_offsets = 1
    else:
        # Calculate number of possible offsets
        num_offsets = resample_delta // base_delta

    df = pl.read_parquet(kline_file)

    # Generate resampled data for each offset
    for i in range(num_offsets):
        offset_str = f"{i * int(base_offset[:-1])}{base_offset[-1]}"

        # Create output directory for this offset
        resampled_offset_dir = results_dir / "resampled_klines" / resample_interval / offset_str
        resampled_offset_dir.mkdir(parents=True, exist_ok=True)

        # Read and resample data
        df_resampled = polars_calc_resample(df, time_interval, resample_interval, offset_str)
        df_resampled.write_parquet(resampled_offset_dir / f"{symbol}.parquet")

    return symbol


def resample_kline_type(trade_type: TradeType, resample_interval: str, base_offset: str):
    """
    Resample kline data for all symbols of a given trade type.
    """
    divider(f"Resample kline {trade_type.value} {resample_interval} {base_offset}")
    symbols = list_results_kline_symbols(trade_type, "1m")

    resampled_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "resampled_klines" / resample_interval
    logger.info(f"Resampled kline directory: {resampled_dir}")
    if resampled_dir.exists():
        logger.warning(f"Resampled kline directory exists, removing it")
        shutil.rmtree(resampled_dir)

    start_time = time.perf_counter()

    run_func = partial(
        resample_kline,
        trade_type=trade_type,
        resample_interval=resample_interval,
        base_offset=base_offset,
    )

    with ProcessPoolExecutor(max_workers=N_JOBS, mp_context=mp.get_context("spawn"), initializer=mp_env_init) as exe:
        tasks = [exe.submit(run_func, symbol=symbol) for symbol in symbols]
        with tqdm(total=len(tasks), desc="Resample klines", unit="task") as pbar:
            for task in as_completed(tasks):
                symbol = task.result()
                pbar.set_postfix_str(symbol)
                pbar.update(1)

    time_elapsed = (time.perf_counter() - start_time) / 60
    logger.ok(f"Finished in {time_elapsed:.2f}mins")
