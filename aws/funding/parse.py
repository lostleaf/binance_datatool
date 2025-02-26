import logging
import multiprocessing as mp
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import time
from zipfile import ZipFile

import polars as pl

from aws.client_async import AwsFundingRateClient
from aws.funding.util import local_list_funding_symbols
from config import BINANCE_DATA_DIR, N_JOBS, DataFrequency, TradeType
from util.concurrent import mp_env_init
from util.log_kit import divider, logger


def read_funding_csv(funding_file) -> pl.DataFrame:
    """
    Read and parse a funding rate CSV file from a zip archive.

    This function extracts data from a zipped CSV file containing funding rate information,
    processes it to the correct format, and returns a polars DataFrame.

    Args:
        funding_file (Path): Path to the zipped CSV file containing funding rate data.

    Returns:
        pl.DataFrame: A DataFrame containing the parsed funding rate data with properly
                     formatted timestamps and columns.
    """
    with ZipFile(funding_file) as f:
        filename = f.namelist()[0]
        lines = f.open(filename).readlines()
    if lines[0].decode().startswith("calc_time"):
        lines = lines[1:]

    columns = ["funding_time", "funding_interval_hours", "funding_rate"]
    schema = {
        "funding_time": pl.Int64,
        "funding_interval_hours": pl.Int64,
        "funding_rate": pl.Float64,
    }
    ldf = pl.scan_csv(lines, has_header=False, new_columns=columns, schema_overrides=schema)
    ldf = ldf.with_columns(
        (pl.col("funding_time") - pl.col("funding_time") % (60 * 60 * 1000)).alias("candle_begin_time")
    )
    ldf = ldf.with_columns(
        pl.col("candle_begin_time").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC"),
        pl.col("funding_time").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC"),
    )

    return ldf.collect()


def run_parse_symbol_funding(aws_symbol_funding_dir, parsed_funding_file):
    """
    Parse funding rate files for a specific symbol and return the processed data.

    Args:
        aws_symbol_funding_dir (Path): Directory containing the funding rate files for a symbol
        parsed_funding_file (Path): Output file path for the parsed data
    """
    if not aws_symbol_funding_dir.exists():
        return aws_symbol_funding_dir, 0

    dfs: list[pl.DataFrame] = []

    for funding_file in aws_symbol_funding_dir.glob("*.zip"):
        df = read_funding_csv(funding_file)
        if not df.is_empty():
            dfs.append(df)

    if not dfs:
        return

    df_funding = pl.concat(dfs).sort("candle_begin_time")
    df_funding.write_parquet(parsed_funding_file)


def parse_funding_rates(trade_type: TradeType, symbols: list[str]):
    """
    Parse AWS funding rates for given trade_type, time_interval and a list of symbols.
    Store the results in a single parquet file.

    Args:
        trade_type (TradeType): Type of trade (e.g., SPOT, FUTURES)
        symbols (list[str]): List of symbols to process
    """
    logger.info(f"Start parsing funding rates")

    aws_local_funding_dir = AwsFundingRateClient.LOCAL_DIR / AwsFundingRateClient.get_base_dir(
        trade_type, DataFrequency.monthly
    )

    parsed_funding_dir = BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "funding"
    parsed_funding_dir.mkdir(parents=True, exist_ok=True)

    logger.debug(
        f"trade_type={trade_type.value}, num_symbols={len(symbols)}, n_jobs={N_JOBS}, {symbols[0]} -- {symbols[-1]}"
    )

    logger.debug(f"aws_local_funding_dir={aws_local_funding_dir}")
    logger.debug(f"parsed_funding_dir={parsed_funding_dir}")

    with ProcessPoolExecutor(max_workers=N_JOBS, mp_context=mp.get_context("spawn"), initializer=mp_env_init) as exe:
        tasks = []

        for symbol in symbols:
            aws_symbol_funding_dir = aws_local_funding_dir / symbol
            parsed_symbol_funding_file = parsed_funding_dir / f"{symbol}.pqt"

            task = exe.submit(run_parse_symbol_funding, aws_symbol_funding_dir, parsed_symbol_funding_file)
            tasks.append(task)

        for future in as_completed(tasks):
            future.result()


def parse_type_all_funding_rates(trade_type: TradeType):
    divider(f"BHDS Parse {trade_type.value} Funding Rates")
    symbols = local_list_funding_symbols(trade_type)

    t_start = time.perf_counter()

    parse_funding_rates(trade_type, symbols)
    
    time_elapsed = (time.perf_counter() - t_start) / 60
    logger.ok(f"Finished Parsing {trade_type.value} Funding Rates, Time={time_elapsed:.2f}mins")
