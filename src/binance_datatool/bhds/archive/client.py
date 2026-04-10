"""S3 directory listing client for data.binance.vision."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import aiohttp
import xmltodict
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from binance_datatool.common import S3_HTTP_TIMEOUT_SECONDS, S3_LISTING_PREFIX

if TYPE_CHECKING:
    from collections.abc import Iterable

    from binance_datatool.common.enums import DataFrequency, DataType, TradeType


def _build_prefix(
    trade_type: TradeType,
    data_freq: DataFrequency,
    data_type: DataType,
) -> str:
    """Build an S3 prefix for a market archive directory."""
    return f"data/{trade_type.s3_path}/{data_freq.value}/{data_type.value}/"


def _build_listing_url(prefix: str, marker: str | None = None) -> str:
    """Build an S3 bucket listing URL for a prefix."""
    query: dict[str, str] = {"delimiter": "/", "prefix": prefix}
    if marker is not None:
        query["marker"] = marker
    return f"{S3_LISTING_PREFIX}?{urlencode(query)}"


def _normalize_entries(value: Any) -> list[dict[str, Any]]:
    """Normalize xmltodict values that may be absent, a dict, or a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    msg = f"Unexpected S3 XML entry payload type: {type(value)!r}"
    raise TypeError(msg)


def _extract_prefixes_from_payload(payload: dict[str, Any]) -> list[str]:
    """Extract child prefixes from an S3 listing response payload."""
    result = payload["ListBucketResult"]
    prefixes = _normalize_entries(result.get("CommonPrefixes"))
    return [entry["Prefix"] for entry in prefixes]


def _is_truncated(payload: dict[str, Any]) -> bool:
    """Return whether the S3 listing response is truncated."""
    result = payload["ListBucketResult"]
    return result.get("IsTruncated", "false").lower() == "true"


def _next_marker(payload: dict[str, Any], prefixes: Iterable[str]) -> str | None:
    """Return the marker to use for the next S3 listing page."""
    result = payload["ListBucketResult"]
    marker = result.get("NextMarker")
    if marker:
        return marker

    prefix_list = list(prefixes)
    if prefix_list:
        return prefix_list[-1]

    contents = _normalize_entries(result.get("Contents"))
    if contents:
        return contents[-1]["Key"]
    return None


def _extract_symbol(prefix: str) -> str:
    """Extract the last non-empty path segment from a prefix."""
    return prefix.rstrip("/").rsplit("/", maxsplit=1)[-1]


class ArchiveClient:
    """Client for browsing Binance public archive prefixes."""

    def __init__(
        self,
        *,
        timeout_seconds: int | float = S3_HTTP_TIMEOUT_SECONDS,
        trust_env: bool = True,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.trust_env = trust_env

    def _create_session(self) -> aiohttp.ClientSession:
        """Create an HTTP session for archive requests."""
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        return aiohttp.ClientSession(timeout=timeout, trust_env=self.trust_env)

    async def _fetch_xml(self, session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
        """Fetch and parse a single S3 XML listing page."""
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
            reraise=True,
        ):
            with attempt:
                async with session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text()
                return xmltodict.parse(text)

        msg = "unreachable"
        raise RuntimeError(msg)

    async def list_dir(self, session: aiohttp.ClientSession, prefix: str) -> list[str]:
        """List all child prefixes under an S3 prefix."""
        prefixes: list[str] = []
        marker: str | None = None

        while True:
            payload = await self._fetch_xml(session, _build_listing_url(prefix, marker))
            page_prefixes = _extract_prefixes_from_payload(payload)
            prefixes.extend(page_prefixes)

            if not _is_truncated(payload):
                break

            marker = _next_marker(payload, page_prefixes)
            if marker is None:
                break

        return prefixes

    async def list_symbols(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
    ) -> list[str]:
        """List available symbols from the Binance archive."""
        prefix = _build_prefix(trade_type, data_freq, data_type)

        async with self._create_session() as session:
            child_prefixes = await self.list_dir(session, prefix)

        return sorted(_extract_symbol(prefix) for prefix in child_prefixes)


async def list_symbols(
    trade_type: TradeType,
    data_freq: DataFrequency,
    data_type: DataType,
) -> list[str]:
    """List available symbols using the default archive client."""
    return await ArchiveClient().list_symbols(trade_type, data_freq, data_type)
