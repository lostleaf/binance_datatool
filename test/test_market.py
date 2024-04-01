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
        # candles = await m.get_candle('BTCUSD_PERP', '5m', limit=3)
        # print('Last row of BTC:\n', candles.iloc[-1])
        # filter = TradingCoinSwapFilter()
        # print('Trading swaps:', filter(syminfo))
        # df_funding = await m.get_funding_rate()
        # print('FundingRates', df_funding.head())


async def test_usdt_market_api():
    async with create_aiohttp_session(3) as session:
        m = BinanceMarketUMFapi(session)
        f = BinanceFetcher(m)
        print(f'Fetcher trade type {f.trade_type}')
        server_time, weight = await m.aioreq_time_and_weight()
        print(f'Server time:, {server_time}, Used weight: {weight}')
        syminfo = await f.get_exchange_info()
        print('Syminfo of BTC swap:', syminfo['BTCUSDT'])
        # candles = await m.get_candle('BTCUSDT', '5m', limit=3)
        # print('Last row of BTC:\n', candles.iloc[-1])
        # filter = TradingUsdtSwapFilter()
        # print('Trading swaps:', filter(syminfo))
        # df_funding = await m.get_funding_rate()
        # print('FundingRates', df_funding.head())


async def test_usdt_market_api2():
    async with create_aiohttp_session(3) as session:
        m = BinanceUsdtFutureMarketApi(session, 12)
        SYMBOL = 'BIGTIMEUSDT'
        NUM = 499
        INTERVAL = '1h'
        delta = pd.to_timedelta(INTERVAL)
        last_time = None

        syminfo = await m.get_syminfo()
        print(f'Syminfo of {SYMBOL} swap:', syminfo[SYMBOL])

        candle_dfs = []
        for i in range(5):
            if last_time is None:
                candles = await m.get_candle(SYMBOL, '1h', limit=NUM)
            else:
                end_ts = (last_time - delta).value // 1000000
                candles = await m.get_candle(SYMBOL, '1h', limit=NUM, endTime=end_ts)
            last_time = candles['candle_begin_time'].min()
            print(pd.concat([candles.head(2), candles.tail(2)]), candles.shape)
            candle_dfs.append(candles)
            if candles.shape[0] < NUM:
                break

        df_candle = pd.concat(candle_dfs)
        df_candle.sort_values('candle_begin_time', inplace=True, ignore_index=True)
        df_candle.drop_duplicates('candle_begin_time', inplace=True, ignore_index=True)
        print(df_candle)
        server_time, weight = await m.get_timestamp_and_weight()
        print(f'Server time:, {server_time}, Used weight: {weight}')


if __name__ == '__main__':
    asyncio.run(test_spot_market_api())
    asyncio.run(test_coin_market_api())
    asyncio.run(test_usdt_market_api())
