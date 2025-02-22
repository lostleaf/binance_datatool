import asyncio
from datetime import timedelta
from typing import Optional

import polars as pl

import config
from api.binance import BinanceFetcher
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
            if weight > max_minute_weight * 0.8:
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


async def api_download_aws_missing_kline(
    trade_type: TradeType,
    time_interval: str,
    overwrite: bool,
    http_proxy: Optional[str],
):
    divider("Download AWS missing klines from Binance API")

    parsed_kline_dir = config.BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "klines"
    sym_dts = []

    symbols = [f.stem for f in parsed_kline_dir.glob("*")]

    for symbol in symbols:
        parsed_symbol_kline_dir = parsed_kline_dir / symbol / time_interval
        ts_mgr = TSManager(parsed_symbol_kline_dir)
        df_cnt = ts_mgr.get_row_count_per_date(exclude_empty=False)
        expected_num = timedelta(days=1) // convert_interval_to_timedelta(time_interval)

        if df_cnt is None:
            continue

        df_missing = df_cnt.filter(pl.col("row_count") < expected_num)
        dts = set(df_missing["dt"])

        if not overwrite:
            api_kline_dir = config.BINANCE_DATA_DIR / "api_data" / trade_type.value / "klines" / symbol / time_interval
            kline_files = api_kline_dir.glob("*.pqt")
            dts_exist = {convert_date(f.stem) for f in kline_files}
            dts -= dts_exist

        sym_dts.extend((symbol, dt) for dt in sorted(dts))
    if sym_dts:
        await api_download_kline(trade_type, time_interval, sym_dts, http_proxy)
    else:
        logger.ok("All missings downloaded")
