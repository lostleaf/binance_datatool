import asyncio
import pandas as pd
from market_api import BinanceCoinFutureMarketApi, BinanceUsdtFutureMarketApi
from crawler import TradingCoinSwapFilter, TradingUsdtSwapFilter
from util import create_aiohttp_session

async def test_coin_market_api():

    async with create_aiohttp_session(3) as session:
        m = BinanceCoinFutureMarketApi(session, 12)
        server_time, weight = await m.get_timestamp_and_weight()
        print(f'Server time:, {server_time}, Used weight: {weight}')
        syminfo = await m.get_syminfo()
        df_syminfo = pd.DataFrame.from_dict(syminfo, orient='index')
        df_syminfo.to_csv('syminfo_coin.csv')
        print('Syminfo of BTC swap:', syminfo['BTCUSD_PERP'])
        candles = await m.get_candle('BTCUSD_PERP', '5m', limit=3)
        print('Last row of BTC:\n', candles.iloc[-1])
        filter = TradingCoinSwapFilter()
        print('Trading swaps:', filter(syminfo))

async def test_usdt_market_api():
    async with create_aiohttp_session(3) as session:
        m = BinanceUsdtFutureMarketApi(session, 12)
        server_time, weight = await m.get_timestamp_and_weight()
        print(f'Server time:, {server_time}, Used weight: {weight}')
        syminfo = await m.get_syminfo()
        print('Syminfo of BTC swap:', syminfo['BTCUSDT'])
        candles = await m.get_candle('BTCUSDT', '5m', limit=3)
        print('Last row of BTC:\n', candles.iloc[-1])
        filter = TradingUsdtSwapFilter()
        print('Trading swaps:', filter(syminfo))


if __name__ == '__main__':
    asyncio.run(test_coin_market_api())
    asyncio.run(test_usdt_market_api())