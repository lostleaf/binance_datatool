from itertools import chain
from typing import Optional

from bhds.aws_basics import AwsMonthlyFundingRateClient
from bhds.infer_exchange_info import infer_um_futures_info, infer_cm_futures_info
from bhds.symbol_filter import UmFuturesFilter, CmFuturesFilter
from constant import ContractType, TradeType
from util.log_kit import divider, logger
from util.network import create_aiohttp_session

from .aws_basics import (AWS_TIMEOUT_SEC, aws_download)


async def download_aws_funding_rates(trade_type: TradeType, symbols: list[str], http_proxy: Optional[str]):
    if not symbols:
        return

    logger.info('Start Download Funding Rates from Binance AWS')
    symbols = sorted(symbols)
    logger.debug(f'trade_type={trade_type.value}, num_symbols={len(symbols)}, {symbols[0]} -- {symbols[-1]}')
    if http_proxy is not None:
        logger.debug(f'Use proxy, http_proxy={http_proxy}')

    async with create_aiohttp_session(AWS_TIMEOUT_SEC) as session:
        funding_client = AwsMonthlyFundingRateClient(session, http_proxy, trade_type)
        funding_files = await funding_client.batch_list_funding_rate_files(symbols)
        file_list = list(chain.from_iterable(funding_files.values()))

    aws_download(file_list, http_proxy)


async def download_um_futures_funding_rates(quote: str, contract_type: ContractType, http_proxy: Optional[str]):
    divider(f'BHDS Download USDⓈ-M Futures Funding Rates')
    logger.debug(f'quote={quote}, contract_type={contract_type}')

    sym_filter = UmFuturesFilter(quote_asset=quote, contract_type=contract_type)

    async with create_aiohttp_session(AWS_TIMEOUT_SEC) as session:
        funding_client = AwsMonthlyFundingRateClient(session, http_proxy, TradeType.um_futures)
        symbols = await funding_client.list_symbols()

    exginfo = {symbol: infer_um_futures_info(symbol) for symbol in symbols}
    exginfo = {k: v for k, v in exginfo.items() if v is not None}
    filtered_symbols = sym_filter(exginfo)
    await download_aws_funding_rates(TradeType.um_futures, filtered_symbols, http_proxy)


async def download_cm_futures_funding_rates(contract_type: ContractType, http_proxy: Optional[str]):
    divider(f'BHDS Download Coin Futures Funding Rates')
    logger.debug(f'contract_type={contract_type}')

    sym_filter = CmFuturesFilter(contract_type=contract_type)

    async with create_aiohttp_session(AWS_TIMEOUT_SEC) as session:
        funding_client = AwsMonthlyFundingRateClient(session, http_proxy, TradeType.cm_futures)
        symbols = await funding_client.list_symbols()
  
    exginfo = {symbol: infer_cm_futures_info(symbol) for symbol in symbols}
    exginfo = {k: v for k, v in exginfo.items() if v is not None}
    filtered_symbols = sym_filter(exginfo)
    await download_aws_funding_rates(TradeType.cm_futures, filtered_symbols, http_proxy)
