'''
Abstractions for Binance market data endpoints
API keys are not required here

This design references python-binance. (https://github.com/sammchardy/python-binance)
'''

from abc import ABC

import aiohttp

from bdt_common.enums import TradeType
from bdt_common.exceptions import BinanceAPIException, BinanceRequestException


class BinanceBaseApi(ABC):

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
