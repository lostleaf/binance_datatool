from abc import abstractmethod
from typing import Optional, Tuple

import aiohttp

from bdt_common.enums import TradeType
from bdt_common.rest_api.base import BinanceBaseApi


class BinanceBaseMarketApi(BinanceBaseApi):
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

    PREFIX = "https://fapi.binance.com/fapi"
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400
    MAX_ONCE_CANDLES = 1500

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f"{self.PREFIX}/v1/time"
        async with self.session.get(url, proxy=self.proxy) as resp:
            weight = int(resp.headers["X-MBX-USED-WEIGHT-1M"])
            timestamp = (await resp.json())["serverTime"]
        return timestamp, weight

    async def aioreq_klines(self, **kwargs) -> list:
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        url = f"{self.PREFIX}/v1/klines"
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self) -> dict:
        """
        Get current exchange trading rules and symbol information
        """
        url = f"{self.PREFIX}/v1/exchangeInfo"
        return await self._aio_get(url, None)

    async def aioreq_premium_index(self, **kwargs) -> list:
        """
        Get Mark Price and Funding Rate
        """
        url = f"{self.PREFIX}/v1/premiumIndex"
        return await self._aio_get(url, kwargs)

    async def aioreq_funding_rate(self, **kwargs):
        """
        Get Funding Rate History
        """
        url = f"{self.PREFIX}/v1/fundingRate"
        return await self._aio_get(url, kwargs)

    async def aioreq_book_ticker(self, **kwargs):
        """
        Best price/qty on the order book for a symbol or symbols.
        """
        url = f"{self.PREFIX}/v1/ticker/bookTicker"
        return await self._aio_get(url, kwargs)


class BinanceMarketCMDapi(BinanceBaseMarketApi):
    """
    Abstraction for Binance COIN-M Futures Dapi market endpoints
    """

    PREFIX = "https://dapi.binance.com/dapi"
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400
    MAX_ONCE_CANDLES = 1500

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f"{self.PREFIX}/v1/time"
        async with self.session.get(url, proxy=self.proxy) as resp:
            weight = int(resp.headers["X-MBX-USED-WEIGHT-1M"])
            timestamp = (await resp.json())["serverTime"]
        return timestamp, weight

    async def aioreq_klines(self, **kwargs) -> list:
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        url = f"{self.PREFIX}/v1/klines"
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self) -> dict:
        """
        Get Current exchange trading rules and symbol information
        """
        url = f"{self.PREFIX}/v1/exchangeInfo"
        return await self._aio_get(url, None)

    async def aioreq_premium_index(self, **kwargs) -> list:
        """
        Get Mark Price and Funding Rate
        """
        url = f"{self.PREFIX}/v1/premiumIndex"
        return await self._aio_get(url, kwargs)

    async def aioreq_funding_rate(self, **kwargs):
        """
        Get Funding Rate History
        """
        url = f"{self.PREFIX}/v1/fundingRate"
        return await self._aio_get(url, kwargs)


class BinanceMarketSpotApi(BinanceBaseMarketApi):
    """
    Abstraction for Binance Spot Api market endpoints
    """

    PREFIX = "https://api.binance.com/api"
    WEIGHT_EFFICIENT_ONCE_CANDLES = 1000
    MAX_MINUTE_WEIGHT = 6000
    MAX_ONCE_CANDLES = 1000

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f"{self.PREFIX}/v3/time"
        async with self.session.get(url, proxy=self.proxy) as resp:
            weight = int(resp.headers["X-MBX-USED-WEIGHT-1M"])
            timestamp = (await resp.json())["serverTime"]
        return timestamp, weight

    async def aioreq_klines(self, **kwargs):
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        url = f"{self.PREFIX}/v3/klines"
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self):
        """
        Get Current exchange trading rules and symbol information
        """
        url = f"{self.PREFIX}/v3/exchangeInfo"
        return await self._aio_get(url, None)


def create_binance_market_api(
    trade_type: TradeType, session: aiohttp.ClientSession, http_proxy: Optional[str]
) -> BinanceBaseMarketApi:
    """Create a Binance market API client based on trade type.

    Factory function to instantiate the appropriate market API client for different
    Binance trading segments (spot, UM futures, CM futures).

    Args:
        trade_type: The market segment type (spot, um_futures, cm_futures).
        session: Aiohttp client session for making HTTP requests.
        http_proxy: Optional HTTP proxy URL for requests.

    Returns:
        BinanceBaseMarketApi: Configured market API client instance.

    Raises:
        ValueError: If the trade type is not supported.

    Examples:
        >>> import aiohttp
        >>> from bdt_common.enums import TradeType
        >>>
        >>> async with aiohttp.ClientSession() as session:
        ...     api = create_binance_market_api(TradeType.spot, session, None)
        ...     exchange_info = await api.aioreq_exchange_info()
    """
    match trade_type:
        case TradeType.spot:
            return BinanceMarketSpotApi(session, http_proxy)
        case TradeType.um_futures:
            return BinanceMarketUMFapi(session, http_proxy)
        case TradeType.cm_futures:
            return BinanceMarketCMDapi(session, http_proxy)
        case _:
            raise ValueError(f"Unsupported trade type: {trade_type}")
