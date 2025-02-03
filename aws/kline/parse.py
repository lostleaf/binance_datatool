import multiprocessing as mp
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zipfile import ZipFile

import polars as pl

import config
from aws.checksum import get_verified_aws_data_files
from aws.client_async import AwsKlineClient
from aws.kline.util import local_list_kline_symbols
from config import DataFrequency, TradeType
from util.concurrent import mp_env_init
from util.log_kit import divider, logger
from util.time import convert_date
from util.ts_manager import TSManager


def read_csv(csv_file):
    columns = [
        'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
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
        'taker_buy_quote_asset_volume': pl.Float64
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

    # Cast column types
    ldf = ldf.with_columns(pl.col('candle_begin_time').cast(pl.Datetime('ms')).dt.replace_time_zone('UTC'))

    # Only keep klines with volume
    ldf = ldf.filter(pl.col('volume') > 0)

    return ldf.collect()


def get_kline_file_dt(f: Path):
    tokens = f.stem.split('-')[-3:]
    return ''.join(tokens)


def filter_not_parsed_kline_files(aws_symbol_kline_dir, parsed_symbol_kline_dir):
    db = PartitionedPolarsDB(parsed_symbol_kline_dir, DataFrequency.daily)
    exist_dts = db.get_exist_partitions()
    verified_kline_files = get_verified_aws_data_files(aws_symbol_kline_dir)
    dt_files = {get_kline_file_dt(f): f for f in verified_kline_files}
    dts_not_parsed = set(dt_files.keys()) - set(exist_dts)
    files_not_parsed = [dt_files[dt] for dt in dts_not_parsed]
    return files_not_parsed


def parse_save_kline(parsed_symbol_kline_dir, kline_file):
    db = PartitionedPolarsDB(parsed_symbol_kline_dir, DataFrequency.daily)
    dt = get_kline_file_dt(kline_file)
    df = read_csv(kline_file)
    start = convert_date(dt)
    start = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    df.filter(df['candle_begin_time'].is_between(start, end, closed='left'))
    db.set_partition(dt, df)


def parse_klines(trade_type: TradeType, time_interval: str, symbols: list[str]):
    aws_local_kline_dir = AwsKlineClient.LOCAL_DIR / AwsKlineClient.get_base_dir(trade_type, DataFrequency.daily)
    parsed_kline_dir = config.BINANCE_DATA_DIR / 'parsed_data' / trade_type.value / 'klines'

    with ProcessPoolExecutor(max_workers=config.N_JOBS, mp_context=mp.get_context('spawn'),
                             initializer=mp_env_init) as exe:
        tasks = []

        for symbol in symbols:
            aws_symbol_kline_dir = aws_local_kline_dir / symbol / time_interval
            parsed_symbol_kline_dir = parsed_kline_dir / symbol / time_interval
            kline_files = filter_not_parsed_kline_files(aws_symbol_kline_dir, parsed_symbol_kline_dir)
            for kline_file in kline_files:
                task = exe.submit(parse_save_kline, parsed_symbol_kline_dir, kline_file)
                tasks.append(task)

        for task in as_completed(tasks):
            task.result()


def parse_klines(trade_type: TradeType, time_interval: str, symbols: list[str]):
    logger.info(f'Start parse csv klines')
    logger.debug(f'trade_type={trade_type.value}, time_interval={time_interval}, num_symbols={len(symbols)}, '
                 f'n_jobs={config.N_JOBS}, '
                 f'{symbols[0]} -- {symbols[-1]}')

    aws_local_kline_dir = AwsKlineClient.LOCAL_DIR / AwsKlineClient.get_base_dir(trade_type, DataFrequency.daily)
    parsed_kline_dir = config.BINANCE_DATA_DIR / 'parsed_data' / trade_type.value / 'klines'

    logger.debug(f'aws_local_kline_dir={aws_local_kline_dir}')
    logger.debug(f'parsed_kline_dir={parsed_kline_dir}')

    with ProcessPoolExecutor(max_workers=config.N_JOBS, mp_context=mp.get_context('spawn'),
                             initializer=mp_env_init) as exe:
        tasks = []

        for symbol in symbols:
            aws_symbol_kline_dir = aws_local_kline_dir / symbol / time_interval
            parsed_symbol_kline_dir = parsed_kline_dir / symbol / time_interval
            kline_files = filter_not_parsed_kline_files(aws_symbol_kline_dir, parsed_symbol_kline_dir)
            for kline_file in kline_files:
                task = exe.submit(parse_save_kline, parsed_symbol_kline_dir, kline_file)
                tasks.append(task)

        for task in as_completed(tasks):
            task.result()


def parse_type_all_klines(trade_type: TradeType, time_interval: str):
    divider(f'BHDS Parse {trade_type.value} {time_interval} Klines')
    symbols = local_list_kline_symbols(trade_type, time_interval)

    t_start = time.perf_counter()
    parse_klines(trade_type, time_interval, symbols)
    time_elapsed = (time.perf_counter() - t_start) / 60

    logger.ok(f'Finished Parsing {trade_type.value} {time_interval} Klines, Time={time_elapsed:.2f}mins')
