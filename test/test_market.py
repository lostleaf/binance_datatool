import asyncio
import pandas as pd

from api.binance import BinanceMarketCMDapi, BinanceMarketUMFapi, BinanceMarketSpotApi
from fetcher import BinanceFetcher
from util import create_aiohttp_session


async def test_spot_market_api():
    async with create_aiohttp_session(3) as session:
        m = BinanceMarketSpotApi(session)
        f = BinanceFetcher(m)
        print(f'Fetcher trade type {f.trade_type}')
        server_time, weight = await m.aioreq_time_and_weight()
        print(f'Server time:, {server_time}, Used weight: {weight}')
        syminfo = await f.get_exchange_info()
        print('Syminfo of BTC swap:', syminfo['BTCUSDT'])


async def test_coin_market_api():
    async with create_aiohttp_session(3) as session:
        m = BinanceMarketCMDapi(session)
        f = BinanceFetcher(m)
        print(f'Fetcher trade type {f.trade_type}')
        server_time, weight = await m.aioreq_time_and_weight()
        print(f'Server time:, {server_time}, Used weight: {weight}')
        syminfo = await f.get_exchange_info()
        print('Syminfo of BTC swap:', syminfo['BTCUSD_PERP'])


async def test_usdt_market_api():
    async with create_aiohttp_session(3) as session:
        m = BinanceMarketUMFapi(session)
        f = BinanceFetcher(m)
        print(f'Fetcher trade type {f.trade_type}')
        server_time, weight = await m.aioreq_time_and_weight()
        print(f'Server time:, {server_time}, Used weight: {weight}')
        syminfo = await f.get_exchange_info()
        print('Syminfo of BTC swap:', syminfo['BTCUSDT'])


if __name__ == '__main__':
    asyncio.run(test_spot_market_api())
    asyncio.run(test_coin_market_api())
    asyncio.run(test_usdt_market_api())
