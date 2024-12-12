import asyncio
from typing import Optional

from dateutil import parser as date_parser

from constant import TradeType
from fetcher.binance import BinanceFetcher
from util.log_kit import logger
from util.network import create_aiohttp_session
from config import Config

API_TIMEOUT_SEC = 15


async def _get_kline(fetcher: BinanceFetcher, symbol: str, time_interval: str, dt: str):
    df = await fetcher.get_kline_df_of_day(symbol, time_interval, dt)
    return df, dt


async def download_api_klines(trade_type: TradeType, time_interval: str, symbol: str, dts: list[str],
                              http_proxy: Optional[str]):
    logger.info('Start Download Klines from Binance AWS')
    dts = sorted(dts)
    logger.debug(f'trade_type={trade_type.value}, time_interval={time_interval}, symbol={symbol}, '
                 f'dates={dts[0]} -- {dts[-1]}')

    if http_proxy is not None:
        logger.debug(f'Use proxy, http_proxy={http_proxy}')

    async with create_aiohttp_session(API_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type, session, http_proxy)
        tasks = [asyncio.create_task(_get_kline(fetcher, symbol, time_interval, dt)) for dt in dts]
        for task in asyncio.as_completed(tasks):
            df, dt = await task
            filename = date_parser.parse(dt).strftime('%Y%m%d') + '.pqt'
            kline_dir = Config.BINANCE_DATA_DIR / 'api_data' / 'kline' / trade_type.value / symbol / time_interval
            kline_dir.mkdir(parents=True, exist_ok=True)
            output_file = kline_dir / filename
            df.write_parquet(output_file)
