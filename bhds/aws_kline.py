import asyncio
import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import chain
from pathlib import Path
from typing import List, Optional
from zipfile import ZipFile

import polars as pl

from config import Config
from constant import ContractType, TradeType
from util.log_kit import divider, logger
from util.network import create_aiohttp_session

from .aws_basics import (AWS_TIMEOUT_SEC, TYPE_BASE_DIR, AwsClient, aws_download, get_aws_dir)
from .checksum import verify_checksum
from .infer_exchange_info import (infer_cm_futures_info, infer_spot_info, infer_um_futures_info)
from .symbol_filter import CmFuturesFilter, SpotFilter, UmFuturesFilter

from .bhds_util import mp_env_init


def get_kline_path_tokens(trade_type: TradeType):
    return [*TYPE_BASE_DIR[trade_type], 'daily', 'klines']


class AwsDailyKlineClient(AwsClient):

    def __init__(self, session, http_proxy, trade_type):
        super().__init__(session, http_proxy)
        self.trade_type = trade_type

    @property
    def base_dir_tokens(self):
        return get_kline_path_tokens(self.trade_type)

    async def list_symbols(self):
        aws_dir = get_aws_dir(self.base_dir_tokens)
        paths = await self.list_dir(aws_dir)
        symbols = [Path(os.path.normpath(p)).parts[-1] for p in paths]
        return sorted(symbols)

    async def list_kline_files(self, time_interval, symbol):
        aws_dir = get_aws_dir(self.base_dir_tokens + [symbol, time_interval])
        return await sorted(self.list_dir(aws_dir))

    async def batch_list_kline_files(self, time_interval, symbols):
        tasks = []
        for symbol in symbols:
            aws_dir = get_aws_dir(self.base_dir_tokens + [symbol, time_interval])
            tasks.append(self.list_dir(aws_dir))
        results = await asyncio.gather(*tasks)
        return {symbol: list_result for symbol, list_result in zip(symbols, results)}


async def download_aws_klines(trade_type: TradeType, time_interval: str, symbols: List[str], http_proxy: Optional[str]):
    if not symbols:
        return

    logger.info('Start Download Klines from Binance AWS')
    symbols = sorted(symbols)
    logger.debug(f'trade_type={trade_type.value}, time_interval={time_interval}, num_symbols={len(symbols)}, '
                 f'{symbols[0]} -- {symbols[-1]}')
    if http_proxy is not None:
        logger.debug(f'Use proxy, http_proxy={http_proxy}')

    async with create_aiohttp_session(AWS_TIMEOUT_SEC) as session:
        kline_client = AwsDailyKlineClient(session, http_proxy, trade_type)
        kline_files = await kline_client.batch_list_kline_files(time_interval, symbols)
        file_list = list(chain.from_iterable(kline_files.values()))

    aws_download(file_list, http_proxy)


async def download_spot_klines(time_interval: str, quote: str, keep_stablecoins: bool, leverage_coins: bool,
                               http_proxy: Optional[str]):
    divider(f'BHDS Download Spot {time_interval} Klines')
    logger.debug(f'quote={quote}, keep_stablecoins={keep_stablecoins}, keep_leverage_coins={leverage_coins}')

    sym_filter = SpotFilter(quote_asset=quote, keep_stablecoins=keep_stablecoins, keep_leverage_coins=leverage_coins)

    async with create_aiohttp_session(AWS_TIMEOUT_SEC) as session:
        kline_client = AwsDailyKlineClient(session, http_proxy, TradeType.spot)
        symbols = await kline_client.list_symbols()

    exginfo = {symbol: infer_spot_info(symbol) for symbol in symbols}
    exginfo = {k: v for k, v in exginfo.items() if v is not None}
    filtered_symbols = sym_filter(exginfo)

    await download_aws_klines(TradeType.spot, time_interval, filtered_symbols, http_proxy)


async def download_um_futures_klines(time_interval: str, quote: str, contract_type: ContractType,
                                     http_proxy: Optional[str]):
    divider(f'BHDS Download USDⓈ-M Futures {time_interval} Klines')
    logger.debug(f'quote={quote}, contract_type={contract_type}')

    sym_filter = UmFuturesFilter(quote_asset=quote, contract_type=contract_type)

    async with create_aiohttp_session(AWS_TIMEOUT_SEC) as session:
        kline_client = AwsDailyKlineClient(session, http_proxy, TradeType.um_futures)
        symbols = await kline_client.list_symbols()

    exginfo = {symbol: infer_um_futures_info(symbol) for symbol in symbols}
    exginfo = {k: v for k, v in exginfo.items() if v is not None}
    filtered_symbols = sym_filter(exginfo)
    await download_aws_klines(TradeType.um_futures, time_interval, filtered_symbols, http_proxy)


async def download_cm_futures_klines(time_interval: str, contract_type: ContractType, http_proxy: Optional[str]):
    divider(f'BHDS Download COIN-M Futures {time_interval} Klines')
    logger.debug(f'contract_type={contract_type}')

    sym_filter = CmFuturesFilter(contract_type=contract_type)

    async with create_aiohttp_session(AWS_TIMEOUT_SEC) as session:
        kline_client = AwsDailyKlineClient(session, http_proxy, TradeType.cm_futures)
        symbols = await kline_client.list_symbols()

    exginfo = {symbol: infer_cm_futures_info(symbol) for symbol in symbols}
    exginfo = {k: v for k, v in exginfo.items() if v is not None}
    filtered_symbols = sym_filter(exginfo)
    await download_aws_klines(TradeType.cm_futures, time_interval, filtered_symbols, http_proxy)


def read_aws_kline_csv(p):
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
    with ZipFile(p) as f:
        filename = f.namelist()[0]
        lines = f.open(filename).readlines()

        if lines[0].decode().startswith('open_time'):
            # logger.warning(f'{p} skip header')
            lines = lines[1:]

    # Use Polars to read the CSV file
    df_lazy = pl.scan_csv(lines, has_header=False, new_columns=columns, schema_overrides=schema)

    # Remove useless columns
    df_lazy = df_lazy.drop('ignore', 'close_time')

    # Cast column types
    df_lazy = df_lazy.with_columns(pl.col('candle_begin_time').cast(pl.Datetime('ms')).dt.replace_time_zone('UTC'))
    df = df_lazy.collect()

    return df


def verify_kline_file(kline_file: Path):
    is_success, error = verify_checksum(kline_file)

    if not is_success:
        return False, kline_file, error

    try:
        read_aws_kline_csv(kline_file)
    except:
        return False, kline_file, 'Csv file is not readable'

    return True, kline_file, None


def verify_klines(trade_type: TradeType, time_interval: str, symbols: List[str]):
    logger.info(f'Start verify klines checksums')
    logger.debug(f'trade_type={trade_type.value}, time_interval={time_interval}, num_symbols={len(symbols)}, '
                 f'{symbols[0]} -- {symbols[-1]}')
    base_dir_tokens = get_kline_path_tokens(trade_type)
    unverified_files = []
    for symbol in symbols:
        sym_dir = Config.BINANCE_DATA_DIR / 'aws_data' / Path(get_aws_dir(base_dir_tokens + [symbol, time_interval]))
        for kline_file in sym_dir.glob('*.zip'):
            verify_file = kline_file.parent / (kline_file.name + '.verify')
            if not verify_file.exists():
                unverified_files.append(kline_file)
    unverified_files.sort()

    if not unverified_files:
        logger.ok('All files verified')
        return

    logger.debug(f'num_unverified={len(unverified_files)}, n_jobs={Config.N_JOBS}')
    logger.debug(f'first={unverified_files[0]}')
    logger.debug(f'last={unverified_files[-1]}')

    successes: list[Path] = []
    fails: list[tuple[Path, str]] = []
    with ProcessPoolExecutor(max_workers=Config.N_JOBS, mp_context=mp.get_context('spawn'),
                             initializer=mp_env_init) as exe:
        tasks = [exe.submit(verify_kline_file, kline_file) for kline_file in unverified_files]
        for task in as_completed(tasks):
            is_success, kline_file, error = task.result()
            if is_success:
                successes.append(kline_file)
            else:
                fails.append((kline_file, error))

    for kline_file in successes:
        verify_file = kline_file.parent / (kline_file.name + '.verify')
        verify_file.touch()

    for kline_file, error in fails:
        logger.warning(f'Deleting {kline_file}, {error}')
        checksum_file = kline_file.parent / (kline_file.name + '.CHECKSUM')

        kline_file.unlink(missing_ok=True)
        checksum_file.unlink(missing_ok=True)

    logger.ok(f'{len(successes)} verified, {len(fails)} corrupted')


def list_local_kline_symbols(trade_type: TradeType, time_interval: str):
    base_dir_tokens = get_kline_path_tokens(trade_type)
    kline_dir = Config.BINANCE_DATA_DIR / 'aws_data' / Path(get_aws_dir(base_dir_tokens))
    symbols = sorted(p.parts[-2] for p in kline_dir.glob(f'*/{time_interval}'))
    return symbols


def verify_klines_all_symbols(trade_type: TradeType, time_interval: str):
    divider(f'BHDS verify {trade_type.value} {time_interval} Klines')
    symbols = list_local_kline_symbols(trade_type, time_interval)
    verify_klines(trade_type, time_interval, symbols)
