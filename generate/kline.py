from functools import partial
import shutil
import time
from typing import Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

import polars as pl
from tqdm import tqdm

from aws.kline.util import local_list_kline_symbols
from config import BINANCE_DATA_DIR, TradeType
import config
from generate.merge import merge_klines, merge_funding_rates
from generate.kline_gaps import fill_kline_gaps, scan_gaps, split_by_gaps
from util.concurrent import mp_env_init
from util.log_kit import divider, logger


def gen_kline(
    trade_type: TradeType,
    time_interval: str,
    symbol: str,
    split_gaps: bool,
    min_days: int,
    min_price_chg: float,
    with_vwap: bool,
    with_funding: bool,
):
    """
    Generate complete kline data for a single symbol by merging AWS and API data.

    This function processes kline data through several stages:
    1. Merges kline data from AWS and API sources
    2. Optionally adds VWAP (Volume Weighted Average Price) calculations
    3. Optionally includes funding rates for perpetual futures
    4. Identifies and splits data by significant gaps if requested
    5. Fills missing kline data in each segment
    6. Saves processed data to parquet files

    Gap detection criteria:
    - Primary gaps: time gap > min_days AND absolute price change > min_price_chg
    - Secondary gaps: time gap > min_days*2 regardless of price change

    Args:
        trade_type: Type of trading (spot, um_futures, or cm_futures)
        time_interval: Kline time interval (e.g., '1m', '5m', '1h', '1d')
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        split_gaps: Whether to split data by detected gaps into separate segments
        min_days: Minimum gap duration threshold in days
        min_price_chg: Minimum price change ratio threshold for gap detection
        with_vwap: Whether to calculate and include VWAP (Volume Weighted Average Price)
        with_funding: Whether to include funding rates (only applies to perpetual futures)

    Returns:
        str: The processed symbol name, or None if no data was processed
    """

    # Merge kline data from AWS and API sources
    # This combines historical data from AWS with recent data from Binance API
    ldf_kline = merge_klines(trade_type, symbol, time_interval, exclude_empty=True)

    # Check if any data was retrieved
    if ldf_kline is None:
        return

    # Calculate VWAP if requested
    # VWAP is calculated as quote_volume divided by volume for each kline
    if with_vwap:
        ldf_kline = ldf_kline.with_columns((pl.col("quote_volume") / pl.col("volume")).alias(f"vwap{time_interval}"))

    # Add funding rates for perpetual futures if requested
    # Funding rates are only applicable to futures contracts
    if trade_type in (TradeType.um_futures, TradeType.cm_futures) and with_funding:
        ldf_funding = merge_funding_rates(trade_type, symbol)
        if ldf_funding is not None:
            # Join funding rates with kline data on candle_begin_time
            # Fill null values with 0 for periods without funding rate data
            ldf_kline = ldf_kline.join(ldf_funding, on="candle_begin_time", how="left").fill_null(0)

    # Define the output directory path for processed results
    results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "klines" / time_interval

    # Ensure the results directory exists, create if necessary
    results_dir.mkdir(parents=True, exist_ok=True)

    # Process gaps if splitting is enabled
    if split_gaps:
        # Scan for primary gaps based on both time and price change criteria
        ldf_gap1 = scan_gaps(ldf_kline, min_days, min_price_chg)

        # Scan for secondary gaps based only on extended time criteria
        ldf_gap2 = scan_gaps(ldf_kline, min_days * 2, 0)

        # Combine gap detections and remove duplicates
        ldf_gap = pl.concat([ldf_gap1, ldf_gap2]).unique("candle_begin_time", keep="last")

        # Collect lazy frames to eager DataFrames for gap splitting
        df_kline, df_gap = pl.collect_all([ldf_kline, ldf_gap])

        # Split the data into segments based on detected gaps
        split_dfs = split_by_gaps(df_kline, df_gap, symbol)
    else:
        # If no gap splitting, use the entire dataset as a single segment
        split_dfs = {symbol: ldf_kline.collect()}

    # Skip processing if no data segments were created
    if not split_dfs:
        return

    # Process each data segment
    for symbol, df in split_dfs.items():
        min_time = df["candle_begin_time"].min()
        max_time = df["candle_begin_time"].max()

        # Fill any missing klines in the segment to ensure continuous data
        ldf = fill_kline_gaps(df.lazy(), time_interval, min_time, max_time, with_vwap, with_funding)
        ldf.sink_parquet(results_dir / f"{symbol}.parquet")

    # Return the processed symbol name
    return symbol


def gen_kline_type(
    trade_type: TradeType,
    time_interval: str,
    split_gaps: bool,
    min_days: int,
    min_price_chg: float,
    with_vwap: bool,
    with_funding_rates: bool,
):
    divider(f"BHDS Merge klines for {trade_type.value} {time_interval}")

    results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "klines" / time_interval
    logger.info(f"results_dir={results_dir}")
    if results_dir.exists():
        logger.warning(f"results_dir exists, removing it")
        shutil.rmtree(results_dir)

    msg = f"split_gaps={split_gaps}"
    if split_gaps:
        msg += f" (min_days={min_days}, min_price_chg={min_price_chg})"
    msg += f"; with_vwap={with_vwap}; with_funding_rates={with_funding_rates}"
    logger.info(msg)

    symbols = local_list_kline_symbols(trade_type, time_interval)

    if not symbols:
        logger.warning(f"No symbols found for {trade_type.value} {time_interval}")
        return

    logger.info(f"num_symbols={len(symbols)} ({symbols[0]} -- {symbols[-1]})")

    start_time = time.perf_counter()

    run_func = partial(
        gen_kline,
        trade_type=trade_type,
        time_interval=time_interval,
        split_gaps=split_gaps,
        min_days=min_days,
        min_price_chg=min_price_chg,
        with_vwap=with_vwap,
        with_funding_rates=with_funding_rates,
    )

    with ProcessPoolExecutor(
        max_workers=config.N_JOBS, mp_context=mp.get_context("spawn"), initializer=mp_env_init
    ) as exe:
        tasks = [exe.submit(run_func, symbol=symbol) for symbol in symbols]
        with tqdm(total=len(tasks), desc="Merge klines", unit="task") as pbar:
            for task in as_completed(tasks):
                symbol = task.result()
                pbar.set_postfix_str(symbol)
                pbar.update(1)
    time_elapsed = (time.perf_counter() - start_time) / 60
    logger.ok(f"Finished in {time_elapsed:.2f}mins")
