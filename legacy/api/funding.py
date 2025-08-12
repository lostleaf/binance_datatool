import asyncio
from pathlib import Path
from typing import Optional

import config
from config import TradeType, BINANCE_DATA_DIR
from api.binance import BinanceFetcher
from aws.funding.util import local_list_funding_symbols
from util.log_kit import divider, logger
from util.network import create_aiohttp_session


async def download_funding_for_symbol(funding_dir: Path, symbol: str, fetcher: BinanceFetcher):
    df_funding = await fetcher.get_hist_funding_rate(symbol=symbol, limit=1000)

    if df_funding is None:
        return

    output_file = funding_dir / f"{symbol}.parquet"
    df_funding.write_parquet(output_file)


async def api_download_funding_rates(trade_type: TradeType, symbols: list[str], http_proxy: Optional[str]):
    logger.info(
        f"Start Download {trade_type.value} {len(symbols)} Symbols({symbols[0]} -- {symbols[-1]}) Funding Rates"
    )

    funding_dir = BINANCE_DATA_DIR / "api_data" / trade_type.value / "funding_rate"
    funding_dir.mkdir(parents=True, exist_ok=True)

    logger.debug(f"Funding rate directory: {funding_dir}")

    if http_proxy is not None:
        logger.debug(f"Use proxy, http_proxy={http_proxy}")

    async with create_aiohttp_session(config.HTTP_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type, session, http_proxy)
        tasks = [download_funding_for_symbol(funding_dir, symbol, fetcher) for symbol in symbols]
        await asyncio.gather(*tasks)

    logger.ok(f"{trade_type.value} {symbols[0]} -- {symbols[-1]} API Funding Rates download successfully")


async def api_download_funding_rates_type_all(trade_type: TradeType, http_proxy: Optional[str]):
    divider(f"BHDS Recent {trade_type.value} Funding Rates API Download")
    symbols = local_list_funding_symbols(trade_type)
    await api_download_funding_rates(trade_type, symbols, http_proxy)
