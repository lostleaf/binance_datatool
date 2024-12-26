import asyncio
from typing import Optional

import config
from config import TradeType
from api.binance import BinanceFetcher
from util.log_kit import logger
from util.network import create_aiohttp_session
from util.time import convert_date


async def _get_kline(fetcher: BinanceFetcher, symbol: str, time_interval: str, dt: str):
    df = await fetcher.get_kline_df_of_day(symbol, time_interval, dt)
    return df, dt


async def download_api_klines(trade_type: TradeType, time_interval: str, symbol: str, dts: list[str], overwrite: bool,
                              http_proxy: Optional[str]):
    logger.info(f'Start Download {trade_type.value} {symbol} {time_interval} Klines from Binance API')

    dts = [convert_date(dt) for dt in sorted(dts)]

    if http_proxy is not None:
        logger.debug(f'Use proxy, http_proxy={http_proxy}')

    # only download not existed dates if not overwrite
    if not overwrite:
        dts_filtered = []
        for dt in dts:
            filename: str = dt.strftime('%Y%m%d') + '.pqt'
            kline_dir = config.BINANCE_DATA_DIR / 'api_data' / 'kline' / trade_type.value / symbol / time_interval
            output_file = kline_dir / filename
            if not output_file.exists():
                dts_filtered.append(dt)
        dts = dts_filtered

    async with create_aiohttp_session(config.AWS_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type, session, http_proxy)
        while dts:
            server_ts, weight = await fetcher.get_time_and_weight()
            dts_batch = dts[:10]
            dts = dts[10:]
            logger.debug(f'server_time={server_ts}, weight_used={weight}, dates={dts_batch[0]} -- {dts_batch[-1]}')
            tasks = [asyncio.create_task(_get_kline(fetcher, symbol, time_interval, dt)) for dt in dts]
            max_minute_weight, _ = fetcher.get_api_limits()
            if weight > max_minute_weight * 0.9:
                await asyncio.sleep(60)
            for task in asyncio.as_completed(tasks):
                df, dt = await task
                filename = dt.strftime('%Y%m%d') + '.pqt'
                kline_dir = config.BINANCE_DATA_DIR / 'api_data' / 'kline' / trade_type.value / symbol / time_interval
                kline_dir.mkdir(parents=True, exist_ok=True)
                output_file = kline_dir / filename
                df.write_parquet(output_file)

    logger.ok(f'{trade_type.value} {symbol} {time_interval} API klines download successfully')
