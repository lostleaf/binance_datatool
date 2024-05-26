'''
Defines errors and common helper functions
Mostly copied from python-binance (https://github.com/sammchardy/python-binance)
'''

import aiohttp

from .exceptions import BinanceRequestException, BinanceAPIException


class BinanceBaseApi:

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

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
        async with self.session.get(url, params=params) as resp:
            return await self._handle_response(resp)

    async def _aio_post(self, url, params):
        async with self.session.post(url, data=params) as resp:
            return await self._handle_response(resp)
