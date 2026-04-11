"""Tests for archive workflows."""

from __future__ import annotations

import aiohttp
import pytest

from binance_datatool.bhds.archive import (
    ArchiveClient,
    CmSymbolFilter,
    SpotSymbolFilter,
    UmSymbolFilter,
)
from binance_datatool.bhds.workflow.archive import (
    ArchiveListFilesWorkflow,
    ArchiveListSymbolsWorkflow,
    ListFilesResult,
    ListSymbolsResult,
)
from binance_datatool.common import ContractType, DataFrequency, DataType, TradeType
from conftest import FakeArchiveClient


@pytest.mark.asyncio
async def test_archive_list_symbols_workflow_returns_inferred_and_unmatched_spot_symbols() -> None:
    """Spot workflows should infer known symbols and preserve unmatched raw values."""
    workflow = ArchiveListSymbolsWorkflow(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        client=FakeArchiveClient(symbols=["BTCUSDT", "这是测试币456", "ETHUSDT"]),
    )

    result = await workflow.run()

    assert isinstance(result, ListSymbolsResult)
    assert [info.symbol for info in result.matched] == ["BTCUSDT", "ETHUSDT"]
    assert result.unmatched == ["这是测试币456"]
    assert result.filtered_out == []


@pytest.mark.asyncio
async def test_archive_list_symbols_workflow_applies_spot_filter_and_preserves_order() -> None:
    """Spot workflows should split inferred symbols into matched and filtered-out buckets."""
    workflow = ArchiveListSymbolsWorkflow(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbol_filter=SpotSymbolFilter(
            quote_assets=frozenset({"USDT"}),
            exclude_leverage=True,
            exclude_stable_pairs=True,
        ),
        client=FakeArchiveClient(symbols=["ETHBTC", "BNBUPUSDT", "BTCUSDT", "USDCUSDT"]),
    )

    result = await workflow.run()

    assert [info.symbol for info in result.matched] == ["BTCUSDT"]
    assert [info.symbol for info in result.filtered_out] == ["ETHBTC", "BNBUPUSDT", "USDCUSDT"]
    assert result.unmatched == []


@pytest.mark.asyncio
async def test_archive_list_symbols_workflow_applies_um_filter() -> None:
    """USD-M workflows should filter by quote, contract type, and stable-pair status."""
    workflow = ArchiveListSymbolsWorkflow(
        trade_type=TradeType.um,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbol_filter=UmSymbolFilter(
            quote_assets=frozenset({"USDT"}),
            contract_type=ContractType.perpetual,
            exclude_stable_pairs=True,
        ),
        client=FakeArchiveClient(symbols=["BTCUSDT", "ETHUSDT_240927", "USDCUSDT", "BAD"]),
    )

    result = await workflow.run()

    assert [info.symbol for info in result.matched] == ["BTCUSDT"]
    assert [info.symbol for info in result.filtered_out] == ["ETHUSDT_240927", "USDCUSDT"]
    assert result.unmatched == ["BAD"]


@pytest.mark.asyncio
async def test_archive_list_symbols_workflow_applies_cm_filter() -> None:
    """COIN-M workflows should filter inferred symbols and keep invalid raw values unmatched."""
    workflow = ArchiveListSymbolsWorkflow(
        trade_type=TradeType.cm,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbol_filter=CmSymbolFilter(contract_type=ContractType.delivery),
        client=FakeArchiveClient(symbols=["BTCUSD_PERP", "ETHUSD_240927", "BTCUSD", "BADUSDT"]),
    )

    result = await workflow.run()

    assert [info.symbol for info in result.matched] == ["ETHUSD_240927"]
    assert [info.symbol for info in result.filtered_out] == ["BTCUSD_PERP"]
    assert result.unmatched == ["BTCUSD", "BADUSDT"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archive_list_symbols_workflow_integration_applies_spot_filter() -> None:
    """Real archive listings should flow through infer and spot filtering correctly."""
    workflow = ArchiveListSymbolsWorkflow(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbol_filter=SpotSymbolFilter(
            quote_assets=frozenset({"USDT"}),
            exclude_leverage=True,
            exclude_stable_pairs=True,
        ),
        client=ArchiveClient(),
    )

    result = await workflow.run()

    assert any(info.symbol == "BTCUSDT" for info in result.matched)
    assert result.filtered_out
    assert all(info.quote_asset == "USDT" for info in result.matched)
    assert all(not info.is_leverage for info in result.matched)
    assert all(not info.is_stable_pair for info in result.matched)


@pytest.mark.asyncio
async def test_archive_list_files_workflow_preserves_input_order(sample_archive_files) -> None:
    """File-list workflow results should preserve the caller's symbol order."""
    workflow = ArchiveListFilesWorkflow(
        trade_type=TradeType.um,
        data_freq=DataFrequency.monthly,
        data_type=DataType.funding_rate,
        symbols=["ETHUSDT", "BTCUSDT"],
        client=FakeArchiveClient(
            files_by_symbol={
                "BTCUSDT": sample_archive_files,
                "ETHUSDT": [],
            }
        ),
    )

    result = await workflow.run()

    assert isinstance(result, ListFilesResult)
    assert [entry.symbol for entry in result.per_symbol] == ["ETHUSDT", "BTCUSDT"]
    assert result.per_symbol[0].files == []
    assert result.per_symbol[1].files == sample_archive_files
    assert result.has_failures is False


@pytest.mark.asyncio
async def test_archive_list_files_workflow_captures_per_symbol_errors(sample_archive_files) -> None:
    """Workflow failures should be isolated to the failing symbol."""
    workflow = ArchiveListFilesWorkflow(
        trade_type=TradeType.um,
        data_freq=DataFrequency.monthly,
        data_type=DataType.funding_rate,
        symbols=["BTCUSDT", "ETHUSDT"],
        client=FakeArchiveClient(
            files_by_symbol={"BTCUSDT": sample_archive_files},
            errors_by_symbol={"ETHUSDT": aiohttp.ClientError("boom")},
        ),
    )

    result = await workflow.run()

    assert [entry.symbol for entry in result.per_symbol] == ["BTCUSDT", "ETHUSDT"]
    assert result.per_symbol[0].error is None
    assert result.per_symbol[0].files == sample_archive_files
    assert result.per_symbol[1].error == "boom"
    assert result.per_symbol[1].files == []
    assert result.has_failures is True


def test_archive_list_files_workflow_requires_interval_for_kline_types() -> None:
    """Kline-class data types should require an interval."""
    with pytest.raises(ValueError, match="interval is required"):
        ArchiveListFilesWorkflow(
            trade_type=TradeType.um,
            data_freq=DataFrequency.daily,
            data_type=DataType.klines,
            symbols=["BTCUSDT"],
        )


def test_archive_list_files_workflow_rejects_interval_for_non_kline_types() -> None:
    """Non-kline data types should reject interval arguments."""
    with pytest.raises(ValueError, match="interval is not applicable"):
        ArchiveListFilesWorkflow(
            trade_type=TradeType.um,
            data_freq=DataFrequency.monthly,
            data_type=DataType.funding_rate,
            symbols=["BTCUSDT"],
            interval="1m",
        )
