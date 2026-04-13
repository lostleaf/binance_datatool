"""Tests for archive workflows."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
import pytest

from binance_datatool.bhds.archive import (
    ArchiveClient,
    ArchiveFile,
    Aria2DownloadResult,
    CmSymbolFilter,
    SpotSymbolFilter,
    UmSymbolFilter,
)
from binance_datatool.bhds.workflow.archive import (
    ArchiveDownloadWorkflow,
    ArchiveListFilesWorkflow,
    ArchiveListSymbolsWorkflow,
    DiffResult,
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


@pytest.mark.asyncio
async def test_archive_download_workflow_builds_new_updated_and_skipped_diff(tmp_path) -> None:
    """Download diffing should classify files by timestamp freshness."""
    remote_new = ArchiveFile(
        key="data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-01.zip",
        size=100,
        last_modified=datetime(2026, 4, 1, 8, 6, 34, tzinfo=UTC),
    )
    remote_updated = ArchiveFile(
        key=(
            "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-02.zip.CHECKSUM"
        ),
        size=10,
        last_modified=datetime(2026, 4, 2, 8, 6, 34, tzinfo=UTC),
    )
    remote_skipped = ArchiveFile(
        key="data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-03.zip",
        size=100,
        last_modified=datetime(2026, 4, 1, 8, 6, 34, tzinfo=UTC),
    )

    updated_local = tmp_path / "aws_data" / Path(remote_updated.key)
    updated_local.parent.mkdir(parents=True, exist_ok=True)
    updated_local.write_text("old checksum", encoding="utf-8")
    os.utime(updated_local, (remote_updated.last_modified.timestamp() - 100,) * 2)

    skipped_local = tmp_path / "aws_data" / Path(remote_skipped.key)
    skipped_local.parent.mkdir(parents=True, exist_ok=True)
    skipped_local.write_text("fresh zip", encoding="utf-8")
    os.utime(skipped_local, (remote_skipped.last_modified.timestamp() + 100,) * 2)

    workflow = ArchiveDownloadWorkflow(
        trade_type=TradeType.um,
        data_freq=DataFrequency.monthly,
        data_type=DataType.funding_rate,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        dry_run=True,
        client=FakeArchiveClient(
            files_by_symbol={"BTCUSDT": [remote_new, remote_updated, remote_skipped]}
        ),
    )

    result = await workflow.run()

    assert isinstance(result, DiffResult)
    assert result.total_remote == 3
    assert result.skipped == 1
    assert [(entry.remote.key, entry.reason) for entry in result.to_download] == [
        (remote_new.key, "new"),
        (remote_updated.key, "updated"),
    ]


@pytest.mark.asyncio
async def test_archive_download_workflow_invalidates_new_and_legacy_verified_markers(
    tmp_path,
) -> None:
    """Updated zip or checksum files should clear all stale verification markers."""
    checksum_remote = ArchiveFile(
        key=(
            "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-03.zip.CHECKSUM"
        ),
        size=10,
        last_modified=datetime(2026, 4, 3, 8, 6, 34, tzinfo=UTC),
    )
    local_checksum = tmp_path / "aws_data" / Path(checksum_remote.key)
    local_zip = local_checksum.with_name(local_checksum.name.removesuffix(".CHECKSUM"))
    local_checksum.parent.mkdir(parents=True, exist_ok=True)
    local_checksum.write_text("old checksum", encoding="utf-8")
    local_zip.write_text("zip bytes", encoding="utf-8")
    os.utime(local_checksum, (checksum_remote.last_modified.timestamp() - 100,) * 2)

    legacy_marker = local_zip.parent / f"{local_zip.name}.verified"
    timestamped_marker = local_zip.parent / f"{local_zip.name}.1712678400.verified"
    legacy_marker.write_text("", encoding="utf-8")
    timestamped_marker.write_text("", encoding="utf-8")

    captured_requests: list = []

    def fake_download(requests, **kwargs):  # noqa: ANN001
        del kwargs
        captured_requests.extend(requests)
        return Aria2DownloadResult(requested=len(requests), failed_requests=[])

    workflow = ArchiveDownloadWorkflow(
        trade_type=TradeType.um,
        data_freq=DataFrequency.monthly,
        data_type=DataType.funding_rate,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        dry_run=False,
        client=FakeArchiveClient(files_by_symbol={"BTCUSDT": [checksum_remote]}),
        download_func=fake_download,
    )

    result = await workflow.run()

    assert result.failed == 0
    assert len(captured_requests) == 1
    assert not legacy_marker.exists()
    assert not timestamped_marker.exists()


@pytest.mark.asyncio
async def test_archive_download_workflow_dry_run_keeps_verified_markers(tmp_path) -> None:
    """Dry-run mode should not mutate verification markers."""
    checksum_remote = ArchiveFile(
        key=(
            "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-03.zip.CHECKSUM"
        ),
        size=10,
        last_modified=datetime(2026, 4, 3, 8, 6, 34, tzinfo=UTC),
    )
    local_checksum = tmp_path / "aws_data" / Path(checksum_remote.key)
    local_zip = local_checksum.with_name(local_checksum.name.removesuffix(".CHECKSUM"))
    local_checksum.parent.mkdir(parents=True, exist_ok=True)
    local_checksum.write_text("old checksum", encoding="utf-8")
    local_zip.write_text("zip bytes", encoding="utf-8")
    os.utime(local_checksum, (checksum_remote.last_modified.timestamp() - 100,) * 2)

    legacy_marker = local_zip.parent / f"{local_zip.name}.verified"
    timestamped_marker = local_zip.parent / f"{local_zip.name}.1712678400.verified"
    legacy_marker.write_text("", encoding="utf-8")
    timestamped_marker.write_text("", encoding="utf-8")

    workflow = ArchiveDownloadWorkflow(
        trade_type=TradeType.um,
        data_freq=DataFrequency.monthly,
        data_type=DataType.funding_rate,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        dry_run=True,
        client=FakeArchiveClient(files_by_symbol={"BTCUSDT": [checksum_remote]}),
    )

    result = await workflow.run()

    assert isinstance(result, DiffResult)
    assert legacy_marker.exists()
    assert timestamped_marker.exists()


@pytest.mark.asyncio
async def test_archive_download_workflow_returns_listing_failures_and_download_counts(
    tmp_path,
) -> None:
    """Download results should preserve per-symbol listing failures."""
    remote_file = ArchiveFile(
        key="data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-03.zip",
        size=100,
        last_modified=datetime(2026, 4, 3, 8, 6, 34, tzinfo=UTC),
    )

    def fake_download(requests, **kwargs):  # noqa: ANN001
        del kwargs
        return Aria2DownloadResult(requested=len(requests), failed_requests=requests[:1])

    workflow = ArchiveDownloadWorkflow(
        trade_type=TradeType.um,
        data_freq=DataFrequency.monthly,
        data_type=DataType.funding_rate,
        symbols=["BTCUSDT", "ETHUSDT"],
        bhds_home=tmp_path / "missing-home",
        dry_run=False,
        client=FakeArchiveClient(
            files_by_symbol={"BTCUSDT": [remote_file]},
            errors_by_symbol={"ETHUSDT": aiohttp.ClientError("boom")},
        ),
        download_func=fake_download,
    )

    result = await workflow.run()

    assert result.downloaded == 0
    assert result.failed == 1
    assert result.listing_failed_symbols == 1
    assert result.listing_errors[0].symbol == "ETHUSDT"
    assert (tmp_path / "missing-home").exists()
