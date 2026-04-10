"""Tests for the archive listing client."""

from __future__ import annotations

from typing import Any

import aiohttp
import pytest

from binance_datatool.bhds.archive.client import ArchiveClient
from binance_datatool.common import S3_HTTP_TIMEOUT_SECONDS, DataFrequency, DataType, TradeType


@pytest.mark.asyncio
async def test_archive_client_list_dir_handles_pagination_and_single_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S3 pagination should aggregate prefixes, including single-item pages."""
    responses = [
        {
            "ListBucketResult": {
                "IsTruncated": "true",
                "CommonPrefixes": [
                    {"Prefix": "data/spot/daily/klines/BTCUSDT/"},
                    {"Prefix": "data/spot/daily/klines/ETHUSDT/"},
                ],
            }
        },
        {
            "ListBucketResult": {
                "IsTruncated": "false",
                "CommonPrefixes": {"Prefix": "data/spot/daily/klines/BNBUSDT/"},
            }
        },
    ]
    client = ArchiveClient()

    async def fake_fetch_xml(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
        assert "prefix=data%2Fspot%2Fdaily%2Fklines%2F" in url
        return responses.pop(0)

    monkeypatch.setattr(client, "_fetch_xml", fake_fetch_xml)

    async with aiohttp.ClientSession() as session:
        result = await client.list_dir(session, "data/spot/daily/klines/")

    assert result == [
        "data/spot/daily/klines/BTCUSDT/",
        "data/spot/daily/klines/ETHUSDT/",
        "data/spot/daily/klines/BNBUSDT/",
    ]


@pytest.mark.asyncio
async def test_archive_client_list_symbols_uses_trust_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Archive client sessions should trust standard proxy environment variables."""
    captured: dict[str, Any] = {}

    class FakeSession:
        """Minimal async context manager used to capture session kwargs."""

        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    async def fake_list_dir(self, session: FakeSession, prefix: str) -> list[str]:
        assert prefix == "data/spot/daily/klines/"
        return ["data/spot/daily/klines/ETHUSDT/", "data/spot/daily/klines/BTCUSDT/"]

    monkeypatch.setattr("binance_datatool.bhds.archive.client.aiohttp.ClientSession", FakeSession)
    monkeypatch.setattr(ArchiveClient, "list_dir", fake_list_dir)

    client = ArchiveClient()
    symbols = await client.list_symbols(TradeType.spot, DataFrequency.daily, DataType.klines)

    assert symbols == ["BTCUSDT", "ETHUSDT"]
    assert captured["trust_env"] is True
    assert captured["timeout"].total == S3_HTTP_TIMEOUT_SECONDS


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archive_client_list_symbols_spot_klines_integration() -> None:
    """Spot klines listing should contain common symbols."""
    client = ArchiveClient()
    symbols = await client.list_symbols(TradeType.spot, DataFrequency.daily, DataType.klines)
    assert symbols == sorted(symbols)
    assert "BTCUSDT" in symbols
