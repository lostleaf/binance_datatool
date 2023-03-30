import asyncio

from market_api import BinanceCoinFutureMarketApi, BinanceUsdtFutureMarketApi
from util import create_aiohttp_session

async def test_market_api():
    async with create_aiohttp_session(3) as session:
        m = BinanceCoinFutureMarketApi(session, 12)
        server_time, weight = await m.get_timestamp_and_weight()
        print(f'Server time:, {server_time}, Used weight: {weight}')
        syminfo = await m.get_syminfo()
        print(syminfo['BTCUSD_PERP'])
        candles = await m.get_candle('BTCUSD_PERP', '5m', limit=3)
        print(candles.T)

    async with create_aiohttp_session(3) as session:
        m = BinanceUsdtFutureMarketApi(session, 12)
        server_time, weight = await m.get_timestamp_and_weight()
        print(f'Server time:, {server_time}, Used weight: {weight}')
        syminfo = await m.get_syminfo()
        print(syminfo['BTCUSDT'])
        candles = await m.get_candle('BTCUSDT', '5m', limit=3)
        print(candles.T)


if __name__ == '__main__':
    asyncio.run(test_market_api())