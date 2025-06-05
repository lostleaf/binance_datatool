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
    with_funding_rates: bool,
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
        with_funding_rates: Whether to include funding rates (Only for perpetual futures)
    Returns:
        Dictionary mapping split symbol names to DataFrames with filled gaps
    """
    df = merge_klines(trade_type, symbol, time_interval, True)

    if df is None or df.is_empty():
        return

    if with_vwap:
        df = df.with_columns((pl.col("quote_volume") / pl.col("volume")).alias(f"avg_price_{time_interval}"))

    if trade_type in (TradeType.um_futures, TradeType.cm_futures) and with_funding_rates:
        df_funding = merge_funding_rates(trade_type, symbol)
        if df_funding is not None and not df_funding.is_empty():
            df = df.join(df_funding, on="candle_begin_time", how="left").fill_null(0)

    split_dfs = {symbol: df}
    if split_gaps:
        df_gap = pl.concat([scan_gaps(df, min_days, min_price_chg), scan_gaps(df, min_days * 2, 0)]).unique(
            "candle_begin_time", keep="last"
        )
        split_dfs = split_by_gaps(df, df_gap, symbol)

    if not split_dfs:
        return

    results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "klines" / time_interval

    # Make sure results directory exists
    results_dir.mkdir(parents=True, exist_ok=True)

    for symbol, df in split_dfs.items():
        df = fill_kline_gaps(df, time_interval)
        df.write_parquet(results_dir / f"{symbol}.pqt")

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
