'''
Defines errors and common helper functions
Mostly copied from python-binance (https://github.com/sammchardy/python-binance)
'''

import json

import aiohttp


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

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


class BinanceBaseApi:

    def __init__(self, session: aiohttp.ClientSession, http_proxy=None) -> None:
        self.session = session
        self.http_proxy = http_proxy

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
        async with self.session.get(url, params=params, proxy=self.http_proxy) as resp:
            return await self._handle_response(resp)

    async def _aio_post(self, url, params):
        async with self.session.post(url, data=params, proxy=self.http_proxy) as resp:
            return await self._handle_response(resp)
