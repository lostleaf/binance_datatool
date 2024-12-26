from itertools import chain
from typing import Optional

from aws.client_async import AwsFundingRateClient
from config import HTTP_TIMEOUT_SEC, ContractType, TradeType
from util.log_kit import divider, logger
from util.network import create_aiohttp_session
from util.symbol_filter import (filter_cm_futures_symbols, filter_um_futures_symbols)


async def download_funding_rates(trade_type: TradeType, symbols: list[str], http_proxy: Optional[str]):
    if not symbols:
        return

    logger.info('Start Download Funding Rates from Binance AWS')
    symbols = sorted(symbols)
    logger.debug(f'trade_type={trade_type.value}, num_symbols={len(symbols)}, {symbols[0]} -- {symbols[-1]}')
    if http_proxy is not None:
        logger.debug(f'Use proxy, http_proxy={http_proxy}')

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        funding_client = AwsFundingRateClient(session=session, trade_type=trade_type, http_proxy=http_proxy)
        funding_files = await funding_client.batch_list_data_files(symbols)
        aws_files = list(chain.from_iterable(funding_files.values()))
        funding_client.aws_download(aws_files)


async def aws_list_kline_symbols(trade_type: TradeType, http_proxy):
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        kline_client = AwsFundingRateClient(sesssion=session, trade_type=trade_type, http_proxy=http_proxy)
        symbols = await kline_client.list_symbols()
    return symbols


async def download_um_futures_funding_rates(quote: str, contract_type: ContractType, http_proxy: Optional[str]):
    divider(f'BHDS Download USDⓈ-M Futures Funding Rates')
    logger.debug(f'quote={quote}, contract_type={contract_type}')

    symbols = await aws_list_kline_symbols(TradeType.um_futures, http_proxy)

    filtered_symbols = filter_um_futures_symbols(quote, contract_type, symbols)
    await download_funding_rates(trade_type=TradeType.um_futures, symbols=filtered_symbols, http_proxy=http_proxy)


async def download_cm_futures_funding_rates(contract_type: ContractType, http_proxy: Optional[str]):
    divider(f'BHDS Download Coin Futures Funding Rates')
    logger.debug(f'contract_type={contract_type}')

    symbols = await aws_list_kline_symbols(TradeType.cm_futures, http_proxy)

    filtered_symbols = filter_cm_futures_symbols(contract_type, symbols)
    await download_funding_rates(trade_type=TradeType.cm_futures, symbols=filtered_symbols, http_proxy=http_proxy)
