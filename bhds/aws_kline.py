import asyncio
import os
from itertools import chain
from pathlib import Path
from typing import List, Optional

from bhds.infer_exchange_info import infer_cm_futures_info, infer_spot_info, infer_um_futures_info
from util.log_kit import divider, logger
from util.network import create_aiohttp_session

from .aws_basics import (AWS_TIMEOUT_SEC, TYPE_BASE_DIR, AwsClient, aws_download, get_aws_dir)
from .symbol_filter import SpotFilter, UmFuturesFilter, CmFuturesFilter
from constant import TradeType, ContractType


class AwsDailyKlineClient(AwsClient):

    def __init__(self, session, http_proxy, trade_type):
        super().__init__(session, http_proxy)
        self.trade_type = trade_type

    @property
    def base_dir(self):
        return [*TYPE_BASE_DIR[self.trade_type], 'daily', 'klines']

    async def list_symbols(self):
        aws_dir = get_aws_dir(self.base_dir)
        paths = await self.list_dir(aws_dir)
        symbols = [Path(os.path.normpath(p)).parts[-1] for p in paths]
        return sorted(symbols)

    async def list_kline_files(self, time_interval, symbol):
        aws_dir = get_aws_dir(self.base_dir + [symbol, time_interval])
        return await sorted(self.list_dir(aws_dir))

    async def batch_list_kline_files(self, time_interval, symbols):
        tasks = []
        for symbol in symbols:
            aws_dir = get_aws_dir(self.base_dir + [symbol, time_interval])
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
