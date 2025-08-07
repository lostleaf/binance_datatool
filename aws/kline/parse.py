import multiprocessing as mp
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from zipfile import ZipFile

import polars as pl
from tqdm import tqdm

import config
from aws.checksum import get_verified_aws_data_files
from aws.client_async import AwsKlineClient
from aws.kline.util import local_list_kline_symbols
from config import DataFrequency, TradeType
from util.concurrent import mp_env_init
from util.log_kit import divider, logger


def read_kline_csv(csv_file):
    """
    Read and parse kline data from a CSV file within a zip archive.

    Args:
        csv_file: Path to the zip file containing the CSV data

    Returns:
        pl.DataFrame: Processed kline data with UTC timestamps
    """
    # Define the essential columns we need from the kline data along with their data types
    useful_columns = {
        "candle_begin_time": pl.Int64,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
        "quote_volume": pl.Float64,
        "trade_num": pl.Int64,
        "taker_buy_base_asset_volume": pl.Float64,
        "taker_buy_quote_asset_volume": pl.Float64,
    }

    # Include additional columns that exist in the raw data but will be filtered out later
    all_columns = list(useful_columns.keys()) + ["close_time", "ignore"]

    # Extract the CSV content from the zip file
    with ZipFile(csv_file) as f:
        # Read all lines from the first file in the zip archive
        lines = f.open(f.namelist()[0]).readlines()

        # Skip the header row if it exists by checking if the first line starts with "open_time"
        if lines[0].decode().startswith("open_time"):
            lines = lines[1:]

    # Process the CSV data using lazy evaluation for memory efficiency
    return (
        # Create a lazy dataframe from the CSV lines with specified column names and types
        pl.scan_csv(lines, has_header=False, new_columns=all_columns, schema_overrides=useful_columns)
        # Select only the useful columns, filtering out the temporary ones
        .select(useful_columns.keys())
        # Convert the timestamp column to proper datetime format with UTC timezone
        .with_columns(
            # Handle different timestamp precision (microseconds vs milliseconds)
            pl.when(pl.col("candle_begin_time").max() >= 10**15)
            .then(pl.col("candle_begin_time").cast(pl.Datetime("us")))
            .otherwise(pl.col("candle_begin_time").cast(pl.Datetime("ms")))
            .dt.replace_time_zone("UTC")
            .dt.cast_time_unit("ms")
            .alias("candle_begin_time")
        ).collect()
    )


def get_kline_file_dt(f: Path) -> date:
    """
    Extract date from kline filename (format: {symbol}-{interval}-{YYYY-MM-DD}.zip)

    Args:
        f: Path to the kline file

    Returns:
        date: Date extracted from the filename
    """
    tks = f.stem.split("-")[-3:]
    return date(year=int(tks[0]), month=int(tks[1]), day=int(tks[2]))


def run_parse_symbol_kline(aws_symbol_kline_dir: Path, parsed_symbol_kline_dir: Path) -> tuple[Path, int]:
    """
    Simplified K-line data parsing function

    Args:
        aws_symbol_kline_dir: Directory containing K-line CSV zip files
        parsed_symbol_kline_dir: Directory for output parquet files

    Returns:
        tuple: (aws_symbol_kline_dir, number of processed files)
    """
    # Create output directory if it doesn't exist
    parsed_symbol_kline_dir.mkdir(parents=True, exist_ok=True)

    # Get all verified AWS data files
    aws_kline_files = get_verified_aws_data_files(aws_symbol_kline_dir)

    # Build date to file mapping
    date_to_file = {}
    for kline_file in aws_kline_files:
        dt = get_kline_file_dt(kline_file)
        date_to_file[dt] = kline_file

    # Find existing parquet files
    existing_dates = set()
    if parsed_symbol_kline_dir.exists():
        for parquet_file in parsed_symbol_kline_dir.glob("*.parquet"):
            try:
                date_str = parquet_file.stem
                dt = datetime.strptime(date_str, "%Y%m%d").date()
                existing_dates.add(dt)
            except ValueError:
                continue

    # Identify missing dates (parquet files that need to be created)
    missing_dates = [dt for dt in date_to_file.keys() if dt not in existing_dates]

    # Process missing dates
    num_processed = 0
    for dt in sorted(missing_dates):
        csv_file = date_to_file[dt]
        try:
            # Read and parse CSV file
            df = read_kline_csv(csv_file)

            # Save as parquet file
            output_file = parsed_symbol_kline_dir / f"{dt.strftime('%Y%m%d')}.parquet"
            df.write_parquet(output_file)

            num_processed += 1
        except Exception as e:
            logger.error(f"Error processing file {csv_file}: {e}")
            continue

    return aws_symbol_kline_dir, num_processed


def parse_klines(trade_type: TradeType, time_interval: str, symbols: list[str], force_update: bool):
    """
    Parse kline CSV data files into optimized parquet format for multiple trading symbols.

    Args:
        trade_type (TradeType): The type of trading data (e.g., SPOT, UM_FUTURES, CM_FUTURES)
        time_interval (str): The kline time interval (e.g., "1m", "5m", "1h", "1d")
        symbols (list[str]): List of trading pair symbols to process (e.g., ["BTCUSDT", "ETHUSDT"])
        force_update (bool): If True, existing parsed data will be deleted and reprocessed;
                             if False, only missing data will be processed
    """
    logger.info(f"Start parse csv klines")
    logger.debug(
        f"trade_type={trade_type.value}, time_interval={time_interval}, num_symbols={len(symbols)}, "
        f"n_jobs={config.N_JOBS}, "
        f"{symbols[0]} -- {symbols[-1]}"
    )

    # AWS local directory contains downloaded kline data organized by trade type
    aws_local_kline_dir = AwsKlineClient.LOCAL_DIR / AwsKlineClient.get_base_dir(trade_type, DataFrequency.daily)
    # Parsed data directory will contain the processed parquet files
    parsed_kline_dir = config.BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "klines"

    logger.debug(f"aws_local_kline_dir={aws_local_kline_dir}")
    logger.debug(f"parsed_kline_dir={parsed_kline_dir}")

    # Start process pool using 'spawn' context for Polars compatibility
    with ProcessPoolExecutor(
        max_workers=config.N_JOBS, mp_context=mp.get_context("spawn"), initializer=mp_env_init
    ) as exe:
        tasks = []

        # Create processing tasks for each symbol
        for symbol in symbols:
            # Define symbol-specific directories
            aws_symbol_kline_dir = aws_local_kline_dir / symbol / time_interval
            parsed_symbol_kline_dir = parsed_kline_dir / symbol / time_interval

            # Handle force update: remove existing parsed data if requested
            if force_update and parsed_symbol_kline_dir.exists():
                shutil.rmtree(parsed_symbol_kline_dir)

            # Submit independent tasks to process all CSVs for a single symbol
            task = exe.submit(run_parse_symbol_kline, aws_symbol_kline_dir, parsed_symbol_kline_dir)
            tasks.append(task)

        # Monitor progress with tqdm progress bar
        with tqdm(total=len(tasks), desc="Parse klines", unit="task") as pbar:
            for future in as_completed(tasks):
                # Retrieve results from completed tasks
                aws_symbol_kline_dir, num_processed = future.result()

                # Extract symbol name from directory path for display
                symbol = aws_symbol_kline_dir.parts[-2]

                # Update progress bar with symbol and processed file count
                pbar.set_postfix_str(f"{symbol}({num_processed})")
                pbar.update(1)


def parse_type_all_klines(trade_type: TradeType, time_interval: str, force_update: bool):
    divider(f"BHDS Parse {trade_type.value} {time_interval} Klines")
    symbols = local_list_kline_symbols(trade_type, time_interval)

    t_start = time.perf_counter()
    parse_klines(trade_type, time_interval, symbols, force_update)
    time_elapsed = (time.perf_counter() - t_start) / 60

    logger.ok(f"Finished Parsing {trade_type.value} {time_interval} Klines, Time={time_elapsed:.2f}mins")
