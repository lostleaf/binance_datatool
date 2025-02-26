import asyncio
from datetime import timedelta
from typing import Optional

import polars as pl

import config
from api.binance import BinanceFetcher
from aws.kline.util import local_list_kline_symbols
from config import TradeType
from util.log_kit import divider, logger
from util.network import create_aiohttp_session
from util.time import async_sleep_until_run_time, convert_date, convert_interval_to_timedelta, next_run_time
from util.ts_manager import TSManager


async def _get_kline(fetcher: BinanceFetcher, symbol: str, time_interval: str, dt: str):
    df = await fetcher.get_kline_df_of_day(symbol, time_interval, dt)
    return df, symbol, dt


async def api_download_kline(
    trade_type: TradeType,
    time_interval: str,
    sym_dts: list[tuple[str, str]],
    http_proxy: Optional[str],
):
    BATCH_SIZE = 40

    logger.info(f"Start Download {trade_type.value} {time_interval} {len(sym_dts)} Klines from Binance API")
    if http_proxy is not None:
        logger.debug(f"Use proxy, http_proxy={http_proxy}")
    sym_dts = sorted([(sym, convert_date(dt)) for sym, dt in sym_dts])

    async with create_aiohttp_session(config.HTTP_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type, session, http_proxy)
        while sym_dts:
            server_ts, weight = await fetcher.get_time_and_weight()
            batch, sym_dts = sym_dts[:BATCH_SIZE], sym_dts[BATCH_SIZE:]
            logger.debug(f"server_time={server_ts}, weight_used={weight}, start={batch[0]}, end={batch[-1]}")

            max_minute_weight, _ = fetcher.get_api_limits()
            if weight > max_minute_weight - 480:
                logger.info(f"Weight {weight} exceeds the maximum limit, sleep until next minute")
                await async_sleep_until_run_time(next_run_time("1m"))
                continue

            tasks = [asyncio.create_task(_get_kline(fetcher, sym, time_interval, dt)) for sym, dt in batch]

            for task in asyncio.as_completed(tasks):
                df, symbol, dt = await task
                if df is None:
                    continue
                filename = dt.strftime("%Y%m%d") + ".pqt"
                kline_dir = config.BINANCE_DATA_DIR / "api_data" / trade_type.value / "klines" / symbol / time_interval
                kline_dir.mkdir(parents=True, exist_ok=True)
                output_file = kline_dir / filename
                df.write_parquet(output_file)

    logger.ok(f"{trade_type.value} {time_interval} API klines download successfully")


def _get_missing_kline_dates_for_symbol(
    trade_type: TradeType,
    symbol: str,
    time_interval: str,
    overwrite: bool,
) -> list[str]:
    """Get missing kline dates for a single symbol.
    
    Args:
        trade_type: Type of trade (spot or futures)
        symbol: Trading symbol
        time_interval: Time interval for klines
        overwrite: Whether to overwrite existing files
        
    Returns:
        List of dates with missing kline data
    """
    parsed_kline_dir = config.BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "klines"
    parsed_symbol_kline_dir = parsed_kline_dir / symbol / time_interval
    ts_mgr = TSManager(parsed_symbol_kline_dir)
    df_cnt = ts_mgr.get_row_count_per_date(exclude_empty=False)
    expected_num = timedelta(days=1) // convert_interval_to_timedelta(time_interval)

    if df_cnt is None:
        return []

    df_missing = df_cnt.filter(pl.col("row_count") < expected_num)
    dts = set(df_missing["dt"])

    if not overwrite:
        api_kline_dir = config.BINANCE_DATA_DIR / "api_data" / trade_type.value / "klines" / symbol / time_interval
        kline_files = api_kline_dir.glob("*.pqt")
        dts_exist = {convert_date(f.stem) for f in kline_files}
        dts -= dts_exist

    return sorted(dts)


async def api_download_missing_kline_for_symbols(
    trade_type: TradeType,
    symbols: list[str],
    time_interval: str,
    overwrite: bool,
    http_proxy: Optional[str],
):
    """Download missing kline data for multiple symbols.
    
    Args:
        trade_type: Type of trade (spot or futures)
        symbols: List of trading symbols
        time_interval: Time interval for klines
        overwrite: Whether to overwrite existing files
        http_proxy: HTTP proxy to use
        
    """
    sym_dts = []
    
    for symbol in symbols:
        missing_dates = _get_missing_kline_dates_for_symbol(trade_type, symbol, time_interval, overwrite)
        sym_dts.extend((symbol, dt) for dt in missing_dates)
    
    if sym_dts:
        await api_download_kline(trade_type, time_interval, sym_dts, http_proxy)


async def api_download_aws_missing_kline_for_type(
    trade_type: TradeType,
    time_interval: str,
    overwrite: bool,
    http_proxy: Optional[str],
):
    divider(f"BHDS Download missing {trade_type.value} {time_interval} klines from API")

    symbols = local_list_kline_symbols(trade_type, time_interval)
    await api_download_missing_kline_for_symbols(trade_type, symbols, time_interval, overwrite, http_proxy)
    
    logger.ok("All missings downloaded")
