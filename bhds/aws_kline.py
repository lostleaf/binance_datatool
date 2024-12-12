import json
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import chain
from pathlib import Path
from typing import List, Optional

import polars as pl
from dateutil import parser as date_parser

from bhds.aws_basics import AwsDailyKlineClient, get_kline_path_tokens
from bhds.polars_kline import read_aws_kline_csv
from config import Config
from constant import ContractType, TradeType
from util.log_kit import divider, logger
from util.network import create_aiohttp_session

from .aws_basics import (AWS_TIMEOUT_SEC, aws_download, get_aws_dir)
from .bhds_util import mp_env_init
from .checksum import verify_checksum
from .infer_exchange_info import (infer_cm_futures_info, infer_spot_info, infer_um_futures_info)
from .symbol_filter import CmFuturesFilter, SpotFilter, UmFuturesFilter


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


def find_kline_missing_dts(trade_type: TradeType, time_interval: str, symbol: str, gaps: list[tuple[str]]):
    gaps = [(date_parser.parse(st).date(), date_parser.parse(en).date()) for st, en in gaps]

    base_dir_tokens = get_kline_path_tokens(trade_type)
    kline_dir = Config.BINANCE_DATA_DIR / 'aws_data' / Path(get_aws_dir(base_dir_tokens))
    symbol_kline_dir = kline_dir / symbol / time_interval

    zip_files = symbol_kline_dir.glob('*.zip')
    filenames = [f.stem for f in zip_files]
    dts_download = {date_parser.parse(fn.split('-', 2)[-1]).date() for fn in filenames}

    dt_start = min(dts_download)
    dt_end = max(dts_download)
    dt_range = pl.date_range(dt_start, dt_end, '1d', eager=True)

    dts_missing = set(dt_range) - dts_download

    dts_missing = sorted(dt for dt in dts_missing if not any(st_gap <= dt <= en_gap for st_gap, en_gap in gaps))

    return dts_missing


def find_kline_missing_dts_all_symbols(trade_type: TradeType, time_interval: str):
    symbols = list_local_kline_symbols(trade_type, time_interval)
    symbol_dts_missing = dict()

    gap_file = Config.BHDS_KLINE_GAPS_DIR / f'{trade_type.value}.json'
    gap_data = dict()
    if gap_file.exists():
        with open(gap_file, 'r') as fin:
            gap_data = json.load(fin)

    for symbol in symbols:
        gaps = gap_data.get(symbol, [])
        dts_missing = find_kline_missing_dts(trade_type, time_interval, symbol, gaps)
        if dts_missing:
            symbol_dts_missing[symbol] = dts_missing

    return symbol_dts_missing
