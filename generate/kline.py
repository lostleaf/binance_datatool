from datetime import timedelta
from functools import partial
import time
from typing import Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

import polars as pl
from tqdm import tqdm

from aws.kline.parse import TSManager
from aws.kline.util import local_list_kline_symbols
from config import BINANCE_DATA_DIR, TradeType
import config
from util.concurrent import mp_env_init
from util.log_kit import divider, logger
from util.time import convert_interval_to_timedelta


def merge_klines(trade_type: TradeType, symbol: str, time_interval: str, exclude_empty: bool) -> Optional[pl.DataFrame]:
    """
    Merge K-line data from AWS parsed data and API downloaded data

    Args:
        trade_type: Trading type (e.g. SPOT, UM, CM)
        symbol: Trading pair symbol (e.g. BTCUSDT)
        time_interval: K-line interval (e.g. 1m, 1h)
        exclude_empty: Whether to exclude K-lines with zero volume

    Returns:
        Merged DataFrame containing data from both sources, or None if no AWS data found
    """
    # Get AWS parsed data directory
    parsed_symbol_kline_dir = BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "klines" / symbol / time_interval

    # Get AWS parsed data
    ts_mgr = TSManager(parsed_symbol_kline_dir)
    aws_df = ts_mgr.read_all()
    if aws_df is None or aws_df.is_empty():
        return None

    if exclude_empty:
        aws_df = aws_df.filter(pl.col("volume") > 0)

    # Get API data directory
    api_kline_dir = BINANCE_DATA_DIR / "api_data" / trade_type.value / "klines" / symbol / time_interval
    # Read all API data files
    api_files = list(api_kline_dir.glob("*.pqt"))

    if not api_files:
        return aws_df

    # Read and concatenate all API data
    api_df = pl.read_parquet(api_files, columns=aws_df.columns)

    if exclude_empty:
        api_df = api_df.filter(pl.col("volume") > 0)

    # Merge the dataframes, keeping all rows from both sources
    merged_df = pl.concat([aws_df, api_df])

    # Remove duplicates and sort by timestamp
    merged_df = merged_df.unique(subset=["candle_begin_time"], keep="last").sort("candle_begin_time")

    return merged_df


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
            split_df = df.filter(pl.col("candle_begin_time").between(prev_gap_time, gap_time, closed="left"))

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

    # Fill volumes with 0
    ldf = ldf.with_columns(
        pl.col("volume").fill_null(0),
        pl.col("quote_volume").fill_null(0),
        pl.col("trade_num").fill_null(0),
        pl.col("taker_buy_base_asset_volume").fill_null(0),
        pl.col("taker_buy_quote_asset_volume").fill_null(0),
    )

    return ldf.collect()


def merge_and_split_gaps(
    trade_type: TradeType,
    time_interval: str,
    symbol: str,
    split_gaps: bool,
    min_days: int,
    min_price_chg: float,
    with_vwap: bool,
):
    """
    Merge AWS and API kline data for a single symbol and scan for gaps.
    Scan for gaps in kline data where:
    1. df_gap: time gap > min_days AND absolute price change > min_price_chg
    2. df_gap2: time gap > min_days*2 regardless of price change

    Then split data by gaps and fill missing klines in each segment.

    Args:
        trade_type: Type of trading (spot/futures)
        time_interval: Kline time interval
        symbol: Trading pair symbol
        split_gaps: Whether to split data by gaps
        min_days: Minimum gap days threshold
        min_price_chg: Minimum price change ratio threshold
        exclude_empty: Whether to exclude klines with 0 volume
        with_vwap: Whether to calculate vwap
    Returns:
        Dictionary mapping split symbol names to DataFrames with filled gaps
    """
    df = merge_klines(trade_type, symbol, time_interval, True)

    if df is None or df.is_empty():
        return

    if with_vwap:
        df = df.with_columns((pl.col("quote_volume") / pl.col("volume")).alias(f"avg_price_{time_interval}"))

    splited_dfs = {symbol: df}
    if split_gaps:
        df_gap = pl.concat([scan_gaps(df, min_days, min_price_chg), scan_gaps(df, min_days * 2, 0)]).unique(
            "candle_begin_time", keep="last"
        )
        splited_dfs = split_by_gaps(df, df_gap, symbol)

    if not splited_dfs:
        return

    results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "klines" / time_interval

    # Make sure results directory exists
    results_dir.mkdir(parents=True, exist_ok=True)

    for symbol, df in splited_dfs.items():
        df = fill_kline_gaps(df, time_interval)
        df.write_parquet(results_dir / f"{symbol}.pqt")


def merge_and_split_gaps_type_all(
    trade_type: TradeType,
    time_interval: str,
    split_gaps: bool,
    min_days: int,
    min_price_chg: float,
    with_vwap: bool,
):
    divider(f"Merge and split gaps for {trade_type.value} {time_interval}")

    msg = f"split_gaps={split_gaps}"
    if split_gaps:
        msg += f" (min_days={min_days}, min_price_chg={min_price_chg})"
    msg += f"; with_vwap={with_vwap}"
    logger.info(msg)

    symbols = local_list_kline_symbols(trade_type, time_interval)

    if not symbols:
        logger.warning(f"No symbols found for {trade_type.value} {time_interval}")
        return

    logger.info(f"num_symbols={len(symbols)} ({symbols[0]} -- {symbols[-1]})")

    start_time = time.perf_counter()

    run_func = partial(
        merge_and_split_gaps,
        trade_type=trade_type,
        time_interval=time_interval,
        split_gaps=split_gaps,
        min_days=min_days,
        min_price_chg=min_price_chg,
        with_vwap=with_vwap,
    )

    with ProcessPoolExecutor(
        max_workers=config.N_JOBS, mp_context=mp.get_context("spawn"), initializer=mp_env_init
    ) as exe:
        tasks = [exe.submit(run_func, symbol=symbol) for symbol in symbols]
        with tqdm(total=len(tasks), desc="Processing tasks", unit="task") as pbar:
            for task in as_completed(tasks):
                task.result()
                pbar.update(1)
    time_elapsed = (time.perf_counter() - start_time) / 60
    logger.ok(f"Finished in {time_elapsed:.2f}mins")
