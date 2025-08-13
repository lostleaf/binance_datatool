import asyncio
from datetime import datetime
from datetime import time as dtime
from datetime import timedelta
from decimal import Decimal
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp
import polars as pl
from dateutil import parser as date_parser

from bdt_common.enums import TradeType
from bdt_common.network import async_retry_getter
from bdt_common.rest_api.market import create_binance_market_api
from bdt_common.time import convert_interval_to_timedelta


def _get_from_filters(filters, filter_type, field_name):
    """Extract a specific field value from exchange info filters.

    Args:
        filters: List of filter dictionaries from exchange info
        filter_type: The type of filter to search for (e.g., 'PRICE_FILTER')
        field_name: The field name to extract from the matching filter

    Returns:
        The value of the specified field from the matching filter
    """
    for f in filters:
        if f["filterType"] == filter_type:
            return f[field_name]


def _parse_um_futures_syminfo(info):
    """Parse symbol information for USD-M futures from exchange info.

    Args:
        info: Symbol information dictionary from exchange info API

    Returns:
        Dictionary containing parsed symbol information including trading rules
    """
    filters = info["filters"]
    return {
        "symbol": info["symbol"],
        "contract_type": info["contractType"],
        "status": info["status"],
        "base_asset": info["baseAsset"],
        "quote_asset": info["quoteAsset"],
        "margin_asset": info["marginAsset"],
        "price_tick": Decimal(_get_from_filters(filters, "PRICE_FILTER", "tickSize")),
        "lot_size": Decimal(_get_from_filters(filters, "LOT_SIZE", "stepSize")),
        "min_notional_value": Decimal(_get_from_filters(filters, "MIN_NOTIONAL", "notional")),
    }


def _parse_cm_futures_syminfo(info):
    """Parse symbol information for Coin-M futures from exchange info.

    Args:
        info: Symbol information dictionary from exchange info API

    Returns:
        Dictionary containing parsed symbol information including trading rules
    """
    filters = info["filters"]
    return {
        "symbol": info["symbol"],
        "contract_type": info["contractType"],
        "status": info["contractStatus"],
        "base_asset": info["baseAsset"],
        "quote_asset": info["quoteAsset"],
        "margin_asset": info["marginAsset"],
        "price_tick": Decimal(_get_from_filters(filters, "PRICE_FILTER", "tickSize")),
        "lot_size": Decimal(info["contractSize"]),
    }


def _parse_spot_syminfo(info):
    """Parse symbol information for spot trading from exchange info.

    Args:
        info: Symbol information dictionary from exchange info API

    Returns:
        Dictionary containing parsed symbol information including trading rules
    """
    filters = info["filters"]
    return {
        "symbol": info["symbol"],
        "status": info["status"],
        "base_asset": info["baseAsset"],
        "quote_asset": info["quoteAsset"],
        "price_tick": Decimal(_get_from_filters(filters, "PRICE_FILTER", "tickSize")),
        "lot_size": Decimal(_get_from_filters(filters, "LOT_SIZE", "stepSize")),
        "min_notional_value": Decimal(_get_from_filters(filters, "NOTIONAL", "minNotional")),
    }


class BinanceFetcher:
    """A comprehensive fetcher for Binance market data across different trading types.

    This class provides a unified interface to fetch various types of market data
    from Binance APIs including klines, funding rates, and exchange information
    for spot, USD-M futures, and Coin-M futures markets.

    Attributes:
        TYPE_MAP: Mapping of trade types to their respective symbol info parsers
        trade_type: The type of trading (spot, um_futures, cm_futures)
        market_api: The underlying market API client
        syminfo_parse_func: Function to parse symbol information for the trade type
    """

    # Mapping of trade types to their respective symbol information parsers
    TYPE_MAP = {
        TradeType.um_futures: _parse_um_futures_syminfo,
        TradeType.cm_futures: _parse_cm_futures_syminfo,
        TradeType.spot: _parse_spot_syminfo,
    }

    def __init__(self, trade_type: TradeType, session: aiohttp.ClientSession, http_proxy=None):
        """Initialize the BinanceFetcher with specified trade type and session.

        Args:
            trade_type: The type of trading (spot, um_futures, cm_futures)
            session: aiohttp ClientSession for making HTTP requests
            http_proxy: Optional HTTP proxy configuration

        Raises:
            ValueError: If the trade_type is not supported
        """
        self.trade_type = trade_type
        self.market_api = create_binance_market_api(trade_type, session, http_proxy)

        if trade_type in self.TYPE_MAP:
            self.syminfo_parse_func = self.TYPE_MAP[trade_type]
        else:
            raise ValueError(f"Type {trade_type} not supported")

    def get_api_limits(self) -> tuple[int, int]:
        """Get API rate limits for the current market API.

        Returns:
            Tuple of (max_minute_weight, weight_efficient_once_candles)
        """
        return self.market_api.MAX_MINUTE_WEIGHT, self.market_api.WEIGHT_EFFICIENT_ONCE_CANDLES

    async def get_time_and_weight(self) -> tuple[datetime, int]:
        """Get server time and current API weight usage.

        Returns:
            Tuple of (server_datetime, current_weight)
        """
        server_timestamp, weight = await self.market_api.aioreq_time_and_weight()
        server_timestamp = datetime.fromtimestamp(server_timestamp / 1000, ZoneInfo("UTC"))
        return server_timestamp, weight

    async def get_exchange_info(self) -> dict[str, dict]:
        """Fetch and parse trading rules from the /exchangeinfo API.

        Returns:
            Dictionary mapping symbol names to their parsed trading information
            including price ticks, lot sizes, and other trading rules
        """
        exg_info = await async_retry_getter(self.market_api.aioreq_exchange_info)
        results = dict()
        for info in exg_info["symbols"]:
            results[info["symbol"]] = self.syminfo_parse_func(info)
        return results

    async def get_kline_df(self, symbol, interval, **kwargs) -> Optional[pl.DataFrame]:
        """Fetch kline data for a symbol and convert to polars DataFrame.

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            interval: Kline interval (e.g., '1m', '1h', '1d')
            **kwargs: Additional parameters for the klines API (startTime, endTime, limit)

        Returns:
            DataFrame with columns: candle_begin_time, open, high, low, close, volume,
            quote_volume, trade_num, taker_buy_base_asset_volume, taker_buy_quote_asset_volume
            Returns None if no data is available
        """
        klines = await async_retry_getter(self.market_api.aioreq_klines, symbol=symbol, interval=interval, **kwargs)
        if klines is None:
            return None

        columns = [
            "candle_begin_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trade_num",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ]
        schema = {
            "candle_begin_time": pl.Int64,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "quote_volume": pl.Float64,
            "trade_num": pl.Int64,
            "taker_buy_base_asset_volume": pl.Float64,
            "taker_buy_quote_asset_volume": pl.Float64,
        }
        lf = pl.LazyFrame(klines, schema=columns, orient="row", schema_overrides=schema)
        lf = lf.drop("close_time", "ignore")
        lf = lf.with_columns(pl.col("candle_begin_time").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC"))
        lf = lf.unique("candle_begin_time", keep="last")
        lf = lf.sort("candle_begin_time")
        df = lf.collect()
        return df

    async def get_kline_df_of_day(self, symbol, interval, dt) -> Optional[pl.DataFrame]:
        """Fetch kline data for a specific day and convert to polars DataFrame.

        This method handles the complexity of fetching a full day's data by splitting
        the request into multiple API calls if necessary due to API limits.

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            interval: Kline interval (e.g., '1m', '1h', '1d')
            dt: Date to fetch data for (can be string or date object)

        Returns:
            DataFrame with kline data for the specified day, or None if no data available
        """
        if isinstance(dt, str):
            dt = date_parser.parse(dt).date()
        ts_start = datetime.combine(dt, dtime(0, 0), tzinfo=ZoneInfo("UTC"))
        ts_next = ts_start + timedelta(days=1)
        start_ms = int(ts_start.timestamp()) * 1000
        noon_ms = int(datetime.combine(dt, dtime(12, 0), tzinfo=ZoneInfo("UTC")).timestamp()) * 1000
        end_ms = int(ts_next.timestamp()) * 1000 - 1

        max_once_candles = self.market_api.MAX_ONCE_CANDLES
        num = timedelta(days=1) // convert_interval_to_timedelta(interval)

        if num <= max_once_candles:
            result = await self.get_kline_df(
                symbol, interval, startTime=start_ms, endTime=end_ms, limit=max_once_candles
            )
            lf = result.lazy() if result is not None else None
        else:
            results = await asyncio.gather(
                self.get_kline_df(symbol, interval, startTime=start_ms, endTime=noon_ms, limit=max_once_candles),
                self.get_kline_df(symbol, interval, startTime=noon_ms, endTime=end_ms, limit=max_once_candles),
            )
            results = [r.lazy() for r in results if r is not None]
            lf = pl.concat(results) if results else None

        if lf is None:
            return None

        lf = lf.unique("candle_begin_time", keep="last")
        lf = lf.filter(pl.col("candle_begin_time").is_between(ts_start, ts_next, "left"))
        lf = lf.sort("candle_begin_time")
        df = lf.collect()

        return df

    async def get_realtime_funding_rate(self) -> pl.DataFrame:
        """Fetch current funding rates for all futures symbols.

        Returns:
            DataFrame with columns: next_funding_time, symbol, funding_rate

        Raises:
            RuntimeError: If called on spot trade type (funding rates don't apply to spot)
        """
        if self.trade_type == TradeType.spot:
            raise RuntimeError("Cannot request funding rate for spot")
        data = await async_retry_getter(self.market_api.aioreq_premium_index)
        schema = {"nextFundingTime": pl.Int64, "lastFundingRate": pl.Float64, "symbol": str}
        df = pl.LazyFrame(data, schema_overrides=schema, orient="row")
        df = df.filter(pl.col("nextFundingTime") > 0)
        df = df.select(
            pl.col("nextFundingTime").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC").alias("next_funding_time"),
            pl.col("symbol"),
            pl.col("lastFundingRate").alias("funding_rate"),
        )
        return df.collect()

    async def get_hist_funding_rate(self, symbol, **kwargs) -> Optional[pl.DataFrame]:
        """Fetch historical funding rates for a specific symbol.

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            **kwargs: Additional parameters for the funding rate API (startTime, endTime, limit)

        Returns:
            DataFrame with columns: funding_time, candle_begin_time, funding_rate
            Returns None if no data is available

        Raises:
            RuntimeError: If called on spot trade type (funding rates don't apply to spot)

        Note:
            This method includes a 75-second retry delay on failures due to strict rate limits
        """
        if self.trade_type == TradeType.spot:
            raise RuntimeError("Cannot request funding rate for spot")

        # Wait for 75s after a failure because the rate limit is 500/5mins
        data = await async_retry_getter(self.market_api.aioreq_funding_rate, symbol=symbol, _sleep_seconds=75, **kwargs)

        if data is None:
            return None

        schema = {"fundingTime": pl.Int64, "fundingRate": pl.Float64, "symbol": str}
        df = pl.LazyFrame(data, orient="row", schema_overrides=schema)

        candle_begin_time = pl.col("fundingTime") - pl.col("fundingTime") % (60 * 60 * 1000)
        df = df.select(
            pl.col("fundingTime").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC").alias("funding_time"),
            candle_begin_time.cast(pl.Datetime("ms")).dt.replace_time_zone("UTC").alias("candle_begin_time"),
            pl.col("fundingRate").alias("funding_rate"),
        )
        df = df.sort(["candle_begin_time"])
        df = df.collect()
        return df
