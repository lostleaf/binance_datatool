'''
Abstractions for Binance market data endpoints
API keys are not required here
'''

from typing import Tuple
from abc import ABC, abstractmethod

from .restful_basics import BinanceBaseApi


class BinanceBaseMarketApi(ABC, BinanceBaseApi):

    @abstractmethod
    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        pass

    @abstractmethod
    async def aioreq_klines(self, **kwargs) -> list:
        pass

    @abstractmethod
    async def aioreq_exchange_info(self) -> dict:
        pass


class BinanceMarketUMFapi(BinanceBaseMarketApi):
    """
    Abstraction for binance USDⓈ-M Futures Fapi market endpoints
    """

    PREFIX = 'https://fapi.binance.com/fapi'
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f'{self.PREFIX}/v1/time'
        async with self.session.get(url) as resp:
            weight = int(resp.headers['X-MBX-USED-WEIGHT-1M'])
            timestamp = (await resp.json())['serverTime']
        return timestamp, weight

    async def aioreq_klines(self, **kwargs) -> list:
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        url = f'{self.PREFIX}/v1/klines'
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self) -> dict:
        """
        Get current exchange trading rules and symbol information
        """
        url = f'{self.PREFIX}/v1/exchangeInfo'
        return await self._aio_get(url, None)

    async def aioreq_premium_index(self, **kwargs) -> list:
        """
        Get Mark Price and Funding Rate
        """
        url = f'{self.PREFIX}/v1/premiumIndex'
        return await self._aio_get(url, kwargs)

    async def aioreq_funding_rate(self, **kwargs):
        """
        Get Funding Rate History
        """
        url = f'{self.PREFIX}/v1/fundingRate'
        return await self._aio_get(url, kwargs)

    async def aioreq_book_ticker(self, **kwargs):
        """
        Best price/qty on the order book for a symbol or symbols.
        """
        url = f'{self.PREFIX}/v1/ticker/bookTicker'
        return await self._aio_get(url, kwargs)


class BinanceMarketCMDapi(BinanceBaseMarketApi):
    """
    Abstraction for Binance COIN-M Futures Dapi market endpoints
    """

    PREFIX = 'https://dapi.binance.com/dapi'
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f'{self.PREFIX}/v1/time'
        async with self.session.get(url) as resp:
            weight = int(resp.headers['X-MBX-USED-WEIGHT-1M'])
            timestamp = (await resp.json())['serverTime']
        return timestamp, weight

    async def aioreq_klines(self, **kwargs) -> list:
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        url = f'{self.PREFIX}/v1/klines'
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self) -> dict:
        """
        Get Current exchange trading rules and symbol information
        """
        url = f'{self.PREFIX}/v1/exchangeInfo'
        return await self._aio_get(url, None)

    async def aioreq_premium_index(self, **kwargs) -> list:
        """
        Get Mark Price and Funding Rate
        """
        url = f'{self.PREFIX}/v1/premiumIndex'
        return await self._aio_get(url, kwargs)

    async def aioreq_funding_rate(self, **kwargs):
        """
        Get Funding Rate History
        """
        url = f'{self.PREFIX}/v1/fundingRate'
        return await self._aio_get(url, kwargs)


class BinanceMarketSpotApi(BinanceBaseMarketApi):
    """
    Abstraction for Binance Spot Api market endpoints
    """

    PREFIX = 'https://api.binance.com/api'
    WEIGHT_EFFICIENT_ONCE_CANDLES = 1000
    MAX_MINUTE_WEIGHT = 6000

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f'{self.PREFIX}/v3/time'
        async with self.session.get(url) as resp:
            weight = int(resp.headers['X-MBX-USED-WEIGHT-1M'])
            timestamp = (await resp.json())['serverTime']
        return timestamp, weight

    async def aioreq_klines(self, **kwargs):
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        url = f'{self.PREFIX}/v3/klines'
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self):
        """
        Get Current exchange trading rules and symbol information
        """
        url = f'{self.PREFIX}/v3/exchangeInfo'
        return await self._aio_get(url, None)


def create_binance_market_api(type_, session) -> BinanceBaseMarketApi:
    if type_ == 'spot':
        return BinanceMarketSpotApi(session)
    elif type_ == 'usdt_futures':
        return BinanceMarketUMFapi(session)
    elif type_ == 'coin_futures':
        return BinanceMarketCMDapi(session)
