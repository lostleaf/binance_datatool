import multiprocessing as mp
import shutil
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
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
from util.time import convert_interval_to_timedelta
from util.ts_manager import TSManager, get_partition


def read_csv(csv_file):
    columns = [
        'candle_begin_time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'close_time',
        'quote_volume',
        'trade_num',
        'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume',
        'ignore',
    ]
    schema = {
        'candle_begin_time': pl.Int64,
        'open': pl.Float64,
        'high': pl.Float64,
        'low': pl.Float64,
        'close': pl.Float64,
        'volume': pl.Float64,
        'quote_volume': pl.Float64,
        'trade_num': pl.Int64,
        'taker_buy_base_asset_volume': pl.Float64,
        'taker_buy_quote_asset_volume': pl.Float64,
    }
    with ZipFile(csv_file) as f:
        filename = f.namelist()[0]
        lines = f.open(filename).readlines()

        if lines[0].decode().startswith('open_time'):
            # logger.warning(f'{p} skip header')
            lines = lines[1:]

    # Use Polars to read the CSV file
    ldf = pl.scan_csv(lines, has_header=False, new_columns=columns, schema_overrides=schema)

    # Remove useless columns
    ldf = ldf.drop('ignore', 'close_time')

    df = ldf.collect()

    ts_unit = 'ms'
    if df['candle_begin_time'].max() >= (10**15):
        ts_unit = 'us'

    # Cast column types
    ldf = ldf.with_columns(
        pl.col('candle_begin_time').cast(pl.Datetime(ts_unit)).dt.replace_time_zone('UTC').dt.cast_time_unit('ms')
    )

    return ldf.collect()


def get_kline_file_dt(f: Path) -> date:
    tks = f.stem.split('-')[-3:]
    return date(year=int(tks[0]), month=int(tks[1]), day=int(tks[2]))


def run_parse_symbol_kline(aws_symbol_kline_dir: Path, parsed_symbol_kline_dir: Path, thres: int) -> Path:
    ts_mgr = TSManager(parsed_symbol_kline_dir)

    aws_kline_files = get_verified_aws_data_files(aws_symbol_kline_dir)
    aws_partition_files = defaultdict(set)
    for kline_file in aws_kline_files:
        dt = get_kline_file_dt(kline_file)
        partition_name = get_partition(dt, DataFrequency.monthly)
        aws_partition_files[partition_name].add((kline_file, dt))

    df_cnt = ts_mgr.get_row_count_per_date()
    enough_dts = set()
    if df_cnt is not None:
        df_enough = df_cnt.filter(pl.col('row_count') >= thres)
        enough_dts = set(df_enough['dt'])

    num = 0
    for partition_name, kline_tuples in aws_partition_files.items():
        filtered_kline_files = sorted(kline_file for kline_file, dt in kline_tuples if dt not in enough_dts)
        num += len(filtered_kline_files)
        if not filtered_kline_files:
            continue
        dfs = [read_csv(kline_file) for kline_file in filtered_kline_files]
        df_update = pl.concat(dfs).sort(pl.col('candle_begin_time'))
        ts_mgr.update_partition(partition_name, df_update)

    return aws_symbol_kline_dir, num


def parse_klines(trade_type: TradeType, time_interval: str, symbols: list[str], force_update: bool):
    logger.info(f'Start parse csv klines')
    logger.debug(
        f'trade_type={trade_type.value}, time_interval={time_interval}, num_symbols={len(symbols)}, '
        f'n_jobs={config.N_JOBS}, '
        f'{symbols[0]} -- {symbols[-1]}'
    )

    aws_local_kline_dir = AwsKlineClient.LOCAL_DIR / AwsKlineClient.get_base_dir(trade_type, DataFrequency.daily)
    parsed_kline_dir = config.BINANCE_DATA_DIR / 'parsed_data' / trade_type.value / 'klines'
    
    thres = timedelta(days=1) // convert_interval_to_timedelta(time_interval)
    thres = 1

    logger.debug(f'aws_local_kline_dir={aws_local_kline_dir}')
    logger.debug(f'parsed_kline_dir={parsed_kline_dir}')
    logger.debug(f'thres={thres}')

    with ProcessPoolExecutor(
        max_workers=config.N_JOBS, mp_context=mp.get_context('spawn'), initializer=mp_env_init
    ) as exe:
        tasks = []

        for symbol in symbols:
            aws_symbol_kline_dir = aws_local_kline_dir / symbol / time_interval
            parsed_symbol_kline_dir = parsed_kline_dir / symbol / time_interval

            if force_update and parsed_symbol_kline_dir.exists():
                shutil.rmtree(parsed_symbol_kline_dir)

            task = exe.submit(run_parse_symbol_kline, aws_symbol_kline_dir, parsed_symbol_kline_dir, thres)
            tasks.append(task)
        # with tqdm(total=len(tasks), desc="Processing tasks", unit="task") as pbar:
        for future in as_completed(tasks):
            aws_symbol_kline_dir, num = future.result()
            if num > 0:
                logger.debug(f'{aws_symbol_kline_dir.parts[-2:]} updated {num}')
        for task in as_completed(tasks):
            task.result()


def parse_type_all_klines(trade_type: TradeType, time_interval: str, force_update: bool):
    divider(f'BHDS Parse {trade_type.value} {time_interval} Klines')
    symbols = local_list_kline_symbols(trade_type, time_interval)

    t_start = time.perf_counter()
    parse_klines(trade_type, time_interval, symbols, force_update)
    time_elapsed = (time.perf_counter() - t_start) / 60

    logger.ok(f'Finished Parsing {trade_type.value} {time_interval} Klines, Time={time_elapsed:.2f}mins')
