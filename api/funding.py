import asyncio
from typing import Optional

import config
from config import TradeType
from api.binance import BinanceFetcher
from util.log_kit import logger
from util.network import create_aiohttp_session


async def download_api_funding_rates(trade_type: TradeType, symbol: str, http_proxy: Optional[str]):
    logger.info(f'Start Download {trade_type.value} {symbol} Funding Rates from Binance API')

    if http_proxy is not None:
        logger.debug(f'Use proxy, http_proxy={http_proxy}')

    async with create_aiohttp_session(config.HTTP_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type, session, http_proxy)
        df_funding = await fetcher.get_hist_funding_rate(symbol=symbol, limit=1000)
        funding_dir = config.BINANCE_DATA_DIR / 'api_data' / 'funding_rate' / trade_type.value
        funding_dir.mkdir(parents=True, exist_ok=True)
        output_file = funding_dir / f'{symbol}.pqt'
        df_funding.write_parquet(output_file)

    logger.ok(f'{trade_type.value} {symbol} API Funding Rates download successfully, {output_file}')
