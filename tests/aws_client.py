import asyncio
import os
import random
from itertools import islice

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.network import create_aiohttp_session
from bhds.aws.client import AwsClient, AwsKlineClient


async def test_client(
    session, http_proxy: str | None, trade_type: TradeType, data_freq: DataFrequency, data_type: DataType, title: str
):
    print(f"==== {title} ====")
    client = AwsClient(
        trade_type=trade_type,
        data_freq=data_freq,
        data_type=data_type,
        session=session,
        http_proxy=http_proxy,
    )

    # List symbols under base_dir
    symbols = await client.list_symbols()
    print(f"symbols count: {len(symbols)}")
    random_symbols = random.sample(symbols, min(10, len(symbols)))
    print(f"10 random symbols: {random_symbols}")
    # Check presence of required symbols
    target_symbols = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}
    target_symbols = target_symbols.intersection(symbols)
    print(f"targets in symbols: {target_symbols}")

    # Directly list data files for the three target symbols
    files_map = await client.batch_list_data_files(target_symbols)
    for sym in target_symbols:
        files = files_map.get(sym, [])
        print(f"{sym} files count: {len(files)}")
        print(f"{sym} first file: {files[0]}")
        print(f"{sym} last file: {files[-1]}")


async def test_kline_client(
    session, http_proxy: str | None, trade_type: TradeType, data_freq: DataFrequency, time_interval: str, title: str
):
    print(f"==== {title} ====")
    client = AwsKlineClient(
        trade_type=trade_type,
        data_freq=data_freq,
        time_interval=time_interval,
        session=session,
        http_proxy=http_proxy,
    )

    # List symbols under base_dir
    symbols = await client.list_symbols()
    print(f"symbols count: {len(symbols)}")
    random_symbols = random.sample(symbols, min(10, len(symbols)))
    print(f"10 random symbols: {random_symbols}")
    # Check presence of required symbols
    target_symbols = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}
    target_symbols = target_symbols.intersection(symbols)
    print(f"targets in symbols: {target_symbols}")

    # Directly list data files for the three target symbols
    files_map = await client.batch_list_data_files(target_symbols)
    for sym in target_symbols:
        files = files_map.get(sym, [])
        print(f"{sym} files count: {len(files)}")
        if files:
            print(f"{sym} first file: {files[0]}")
            print(f"{sym} last file: {files[-1]}")


async def main():
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:

        # UM futures monthly funding rates
        try:
            await test_client(
                session,
                http_proxy,
                TradeType.um_futures,
                DataFrequency.monthly,
                DataType.funding_rate,
                title="AwsClient test - UM futures monthly fundingRate",
            )
        except Exception as e:
            print(f"UM futures fundingRate test failed: {e}")
        
        # UM spot daily agg_trade
        try:
            await test_client(
                session,
                http_proxy,
                TradeType.spot,
                DataFrequency.daily,
                DataType.agg_trade,
                title="AwsClient test - UM spot daily agg_trade",
            )
        except Exception as e:
            print(f"UM spot daily agg_trade test failed: {e}")

        # AwsKlineClient - UM futures daily 1m klines
        try:
            await test_kline_client(
                session,
                http_proxy,
                TradeType.um_futures,
                DataFrequency.daily,
                "1m",
                title="AwsKlineClient test - UM futures daily 1m klines",
            )
        except Exception as e:
            print(f"AwsKlineClient UM futures 1m klines test failed: {e}")

        # AwsKlineClient - Spot daily 1h klines
        try:
            await test_kline_client(
                session,
                http_proxy,
                TradeType.spot,
                DataFrequency.daily,
                "1h",
                title="AwsKlineClient test - Spot daily 1h klines",
            )
        except Exception as e:
            print(f"AwsKlineClient spot 1h klines test failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
