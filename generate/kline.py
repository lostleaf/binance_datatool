from functools import partial
import shutil
import time
from pathlib import Path
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


def merge_kline_lazy(
    trade_type: TradeType, time_interval: str, symbol: str, with_vwap: bool, with_funding: bool
) -> pl.LazyFrame:
    """
    Merge kline data from AWS and API sources and process it.

    This function processes kline data through several stages:
    1. Merges kline data from AWS historical data and recent Binance API data
    2. Optionally adds VWAP (Volume Weighted Average Price) calculations
    3. Optionally includes funding rates for perpetual futures
    4. Fills gaps to ensure a continuous kline time series

    Args:
        trade_type: Type of trading (spot, um_futures, or cm_futures)
        time_interval: Kline time interval (e.g., '1m', '5m', '1h', '1d')
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        with_vwap: Whether to calculate and include VWAP (Volume Weighted Average Price)
        with_funding: Whether to include funding rates (only applies to perpetual futures)

    Returns:
        pl.LazyFrame: The processed kline data as a Polars LazyFrame
    """
    # Spot markets do not have funding rates so disable funding
    if trade_type == TradeType.spot and with_funding:
        logger.warning(f"Spot kline does not support funding rate, set with_funding to False")
        with_funding = False

    # Merge AWS historical klines with recent Binance API klines
    ldf_kline = merge_klines(trade_type, symbol, time_interval, exclude_empty=False)

    # Return early if no kline data was found
    if ldf_kline is None:
        return

    # Optionally add VWAP as quote_volume/volume with NaN replaced by open
    if with_vwap:
        vwap = (pl.col("quote_volume") / pl.col("volume")).fill_nan(pl.col("open"))
        ldf_kline = ldf_kline.with_columns(vwap.alias(f"vwap{time_interval}"))

    # Optionally join funding rates for futures contracts
    if with_funding:
        ldf_funding = merge_funding_rates(trade_type, symbol)
        if ldf_funding is not None:
            # Left join on candle_begin_time and fill missing values with 0
            ldf_kline = ldf_kline.join(ldf_funding, on="candle_begin_time", how="left", maintain_order="left")
            ldf_kline = ldf_kline.with_columns(pl.col("funding_rate").fill_null(0))

    # Fill gaps to ensure a continuous kline time series
    ldf = fill_kline_gaps(ldf_kline, time_interval, with_vwap, with_funding)
    return ldf


def detect_gaps_lazy(kline_file: Path, min_days: int, min_price_chg: float):
    """
    Detect gaps in kline data based on time and price change criteria.

    Gap detection criteria:
    - Primary gaps: time gap > min_days AND absolute price change > min_price_chg
    - Secondary gaps: time gap > min_days*2 regardless of price change

    Args:
        kline_file: Path to the kline data parquet file
        min_days: Minimum gap duration threshold in days
        min_price_chg: Minimum price change ratio threshold for gap detection

    Returns:
        pl.LazyFrame: A LazyFrame containing detected gaps
    """
    ldf_kline = pl.scan_parquet(kline_file).filter(pl.col("volume") > 0)

    # Scan for primary gaps based on both time and price change criteria
    ldf_gap1 = scan_gaps(ldf_kline, min_days, min_price_chg)

    # Scan for secondary gaps based only on extended time criteria
    ldf_gap2 = scan_gaps(ldf_kline, min_days * 2, 0)

    # Combine gap detections and remove duplicates
    ldf_gap = pl.concat([ldf_gap1, ldf_gap2]).unique("candle_begin_time", keep="last")

    return ldf_gap


def gen_kline(
    results_dir: Path,
    trade_type: TradeType,
    time_interval: str,
    symbols: list[str],
    split_gaps: bool,
    min_days: int,
    min_price_chg: float,
    with_vwap: bool,
    with_funding: bool,
):
    """
    Generate complete kline data from AWS and API sources.

    Pipeline:
    - Merge AWS historical and API recent klines
    - Optionally add VWAP and funding rates
    - Split by gaps if requested
    - Save to parquet files

    Args:
        results_dir: Output directory for parquet files
        trade_type: Market type (spot, um_futures, cm_futures)
        time_interval: Kline interval (1m, 5m, 1h, 1d)
        symbols: Trading pairs to process
        split_gaps: Enable gap-based splitting
        min_days: Minimum gap duration in days
        min_price_chg: Minimum price change threshold
        with_vwap: Include VWAP calculation
        with_funding: Include funding rates
    """
    # Define batch size for polars parallel processing
    batch_size = 32

    # Stage 1: Merge klines
    ldfs = []
    logger.info(f"Defining merge tasks for {len(symbols)} {trade_type.value} {time_interval} symbols")
    t_start = time.perf_counter()

    for symbol in symbols:
        ldf = merge_kline_lazy(trade_type, time_interval, symbol, with_vwap, with_funding)
        ldfs.append(ldf.sink_parquet(results_dir / f"{symbol}.parquet", lazy=True))

    logger.ok(f"Merge tasks defined in {time.perf_counter() - t_start:.2f}s")

    # Execute merge
    logger.info("Executing merge tasks")
    t_start = time.perf_counter()
    with tqdm(total=len(ldfs), desc="Merge klines", unit="task") as pbar:
        for i in range(0, len(ldfs), batch_size):
            batch = ldfs[i : i + batch_size]
            pl.collect_all(batch)
            pbar.update(len(batch))
    time_elapsed = time.perf_counter() - t_start
    time_elapsed = f"{time_elapsed:.2f}s" if time_elapsed < 60 else f"{time_elapsed / 60:.2f}mins"
    logger.ok(f"Merge completed in {time_elapsed}")

    if not split_gaps:
        return

    # Stage 2: Detect gaps
    logger.info(f"Defining gap detection for {len(symbols)} symbols")
    t_start = time.perf_counter()
    gap_ldfs = [detect_gaps_lazy(results_dir / f"{symbol}.parquet", min_days, min_price_chg) for symbol in symbols]
    logger.ok(f"Gap detection defined in {time.perf_counter() - t_start:.2f}s")

    # Execute gap detection
    logger.info("Executing gap detection")
    t_start = time.perf_counter()

    gap_dfs = []
    with tqdm(total=len(gap_ldfs), desc="Gap detection", unit="task") as pbar:
        for i in range(0, len(gap_ldfs), batch_size):
            batch = gap_ldfs[i : i + batch_size]
            batch_results = pl.collect_all(batch)
            pbar.update(len(batch))
            gap_dfs.extend(batch_results)

    symbol_gaps = {s: df for s, df in zip(symbols, gap_dfs) if df.height > 0}
    time_elapsed = time.perf_counter() - t_start
    time_elapsed = f"{time_elapsed:.2f}s" if time_elapsed < 60 else f"{time_elapsed / 60:.2f}mins"
    logger.ok(f"Gap detection completed in {time_elapsed}, num_symbols_with_gaps={len(symbol_gaps)}")

    if not symbol_gaps:
        return

    # Stage 3: Split by gaps
    logger.info("Splitting klines by detected gaps")
    t_start = time.perf_counter()

    for symbol, df_gaps in symbol_gaps.items():
        df_kline = pl.read_parquet(results_dir / f"{symbol}.parquet")
        split_dfs = split_by_gaps(df_kline, df_gaps, symbol)

        for symbol_split, df_split in split_dfs.items():
            df_split.write_parquet(results_dir / f"{symbol_split}.parquet")

    logger.ok(f"Splitting completed in {time.perf_counter() - t_start:.2f}s")


def gen_kline_type(
    trade_type: TradeType,
    time_interval: str,
    split_gaps: bool,
    min_days: int,
    min_price_chg: float,
    with_vwap: bool,
    with_funding: bool,
):
    divider(f"BHDS Merge klines for {trade_type.value} {time_interval}")

    # Define the output directory path for processed results, create if necessary
    results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "klines" / time_interval
    logger.info(f"results_dir={results_dir}")
    if results_dir.exists():
        logger.warning(f"results_dir exists, removing it")
        shutil.rmtree(results_dir)
    results_dir.mkdir(parents=True)

    msg = f"split_gaps={split_gaps}"
    if split_gaps:
        msg += f" (min_days={min_days}, min_price_chg={min_price_chg})"
    msg += f"; with_vwap={with_vwap}; with_funding_rates={with_funding}"
    logger.info(msg)

    symbols = local_list_kline_symbols(trade_type, time_interval)

    if not symbols:
        logger.warning(f"No symbols found for {trade_type.value} {time_interval}")
        return

    logger.info(f"num_symbols={len(symbols)} ({symbols[0]} -- {symbols[-1]})")

    start_time = time.perf_counter()

    gen_kline(
        results_dir=results_dir,
        trade_type=trade_type,
        time_interval=time_interval,
        symbols=symbols,
        split_gaps=split_gaps,
        min_days=min_days,
        min_price_chg=min_price_chg,
        with_vwap=with_vwap,
        with_funding=with_funding,
    )
    time_elapsed = (time.perf_counter() - start_time) / 60
    logger.ok(f"Finished in {time_elapsed:.2f}mins")
