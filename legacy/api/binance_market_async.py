'''
Abstractions for Binance market data endpoints
API keys are not required here

This design references python-binance. (https://github.com/sammchardy/python-binance)
'''

import json
from abc import ABC, abstractmethod
from typing import Tuple

import aiohttp

from config import TradeType


class BinanceRequestException(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'BinanceRequestException: %s' % self.message


class BinanceAPIException(Exception):

    def __init__(self, response, status_code, text):
        self.code = 0
        try:
            json_res = json.loads(text)
        except ValueError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(response.text)
        else:
            self.code = json_res.get('code')
            self.message = json_res.get('msg')
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)
        self.url = getattr(response, 'url', None)

    def __str__(self):  # pragma: no cover
        return f'APIError(code={self.code}): {self.message} {self.url}'


class BinanceBaseApi:

    def __init__(self, session: aiohttp.ClientSession, proxy: str) -> None:
        self.session = session
        self.proxy = proxy

    async def _handle_response(self, response: aiohttp.ClientResponse):
        """
        Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the response.
        """
        if not str(response.status).startswith('2'):
            raise BinanceAPIException(response, response.status, await response.text())
        try:
            return await response.json()
        except ValueError:
            txt = await response.text()
            raise BinanceRequestException(f'Invalid Response: {txt}')

    async def _aio_get(self, url, params):
        if params is None:
            params = {}
        async with self.session.get(url, params=params, proxy=self.proxy) as resp:
            return await self._handle_response(resp)

    async def _aio_post(self, url, params):
        async with self.session.post(url, data=params, proxy=self.proxy) as resp:
            return await self._handle_response(resp)


class BinanceBaseMarketApi(ABC, BinanceBaseApi):
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400
    MAX_ONCE_CANDLES = 1000

    @abstractmethod
    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        pass

    @abstractmethod
    async def aioreq_klines(self, **kwargs) -> list:
        pass

    @abstractmethod
    async def aioreq_exchange_info(self) -> dict:
        pass

    async def aioreq_premium_index(self, **kwargs) -> list:
        raise NotImplementedError


class BinanceMarketUMFapi(BinanceBaseMarketApi):
    """
    Abstraction for binance USDⓈ-M Futures Fapi market endpoints
    """

    PREFIX = 'https://fapi.binance.com/fapi'
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400
    MAX_ONCE_CANDLES = 1500

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f'{self.PREFIX}/v1/time'
        async with self.session.get(url, proxy=self.proxy) as resp:
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
    MAX_ONCE_CANDLES = 1500

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f'{self.PREFIX}/v1/time'
        async with self.session.get(url, proxy=self.proxy) as resp:
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
    MAX_ONCE_CANDLES = 1000

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f'{self.PREFIX}/v3/time'
        async with self.session.get(url, proxy=self.proxy) as resp:
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


def create_binance_market_api(trade_type: TradeType, session, http_proxy) -> BinanceBaseMarketApi:
    match trade_type:
        case TradeType.spot:
            return BinanceMarketSpotApi(session, http_proxy)
        case TradeType.um_futures:
            return BinanceMarketUMFapi(session, http_proxy)
        case TradeType.cm_futures:
            return BinanceMarketCMDapi(session, http_proxy)
