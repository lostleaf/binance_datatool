"""Tests for archive workflows."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
import pytest
from loguru import logger

from binance_datatool.bhds.archive import (
    ArchiveClient,
    ArchiveFile,
    Aria2DownloadResult,
    BatchProgressEvent,
    CmSymbolFilter,
    SpotSymbolFilter,
    UmSymbolFilter,
)
from binance_datatool.bhds.workflow.archive import (
    ArchiveDownloadWorkflow,
    ArchiveListFilesWorkflow,
    ArchiveListSymbolsWorkflow,
    ArchiveVerifyWorkflow,
    DiffResult,
    ListFilesResult,
    ListSymbolsResult,
    VerifyDiffResult,
    VerifyResult,
)
from binance_datatool.common import ContractType, DataFrequency, DataType, TradeType
from conftest import FakeArchiveClient


def _write_verify_pair(
    base_dir: Path,
    name: str,
    *,
    content: bytes = b"zip-bytes",
) -> Path:
    """Create a zip/checksum pair for verify workflow tests."""
    zip_path = base_dir / name
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    (base_dir / f"{name}.CHECKSUM").write_text(f"{digest}  {name}\n", encoding="utf-8")
    return zip_path


def _symbol_verify_dir(tmp_path: Path, *, symbol: str = "BTCUSDT", interval: str = "1m") -> Path:
    """Return the default kline verify directory for a symbol."""
    return tmp_path / "aws_data" / "data" / "spot" / "daily" / "klines" / symbol / interval


def _capture_info_logs() -> tuple[list[str], int]:
    """Capture INFO log messages without depending on the rendered sink format."""
    messages: list[str] = []
    sink_id = logger.add(
        lambda message: messages.append(message.record["message"]),
        level="INFO",
        format="{message}",
    )
    return messages, sink_id


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


@pytest.mark.asyncio
async def test_archive_list_symbols_workflow_logs_parameters_and_stats() -> None:
    """Symbol-list workflows should emit semantic start and completion logs."""
    workflow = ArchiveListSymbolsWorkflow(
        trade_type=TradeType.um,
        data_freq=DataFrequency.monthly,
        data_type=DataType.funding_rate,
        symbol_filter=UmSymbolFilter(
            quote_assets=frozenset({"USDT", "USDC"}),
            contract_type=ContractType.perpetual,
            exclude_stable_pairs=True,
        ),
        client=FakeArchiveClient(symbols=["BTCUSDT", "ETHUSDT_240927", "USDCUSDT", "BAD"]),
    )
    messages, sink_id = _capture_info_logs()

    try:
        result = await workflow.run()
    finally:
        logger.remove(sink_id)

    assert result.total_raw_symbols == 4
    assert result.matched_symbols == 1
    assert result.filtered_out_symbols == 2
    assert result.inference_failed_symbols == 1
    assert (
        "listing symbols for trade_type=um data_freq=monthly data_type=fundingRate "
        "quote_assets=USDC,USDT exclude_leverage=False exclude_stable_pairs=True "
        "contract_type=perpetual"
    ) in messages
    assert (
        "listed symbols: total_raw_symbols=4 matched_symbols=1 filtered_out_symbols=2 "
        "inference_failed_symbols=1"
    ) in messages


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


@pytest.mark.asyncio
async def test_archive_list_files_workflow_logs_parameters_and_stats(sample_archive_files) -> None:
    """File-list workflows should emit semantic start and completion logs."""
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
    messages, sink_id = _capture_info_logs()

    try:
        result = await workflow.run()
    finally:
        logger.remove(sink_id)

    assert result.requested_symbols == 2
    assert result.successful_symbols == 1
    assert result.failed_symbols == 1
    assert result.total_remote_files == len(sample_archive_files)
    assert (
        "listing 2 symbols for trade_type=um data_freq=monthly data_type=fundingRate "
        "interval=None"
    ) in messages
    assert (
        "listed files: requested_symbols=2 successful_symbols=1 failed_symbols=1 "
        f"total_remote_files={len(sample_archive_files)}"
    ) in messages


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


def test_archive_download_workflow_uses_tqdm_write_for_batch_messages(
    monkeypatch,
    tmp_path,
) -> None:
    """Progress-mode batch messages should go through tqdm to preserve the bar state."""

    class FakeTqdm:
        def __init__(
            self,
            *,
            total: int,
            disable: bool,
            file,
            unit: str,
            leave: bool,
        ) -> None:
            self.total = total
            self.disable = disable
            self.file = file
            self.unit = unit
            self.leave = leave
            self.messages: list[str] = []
            self.updates: list[int] = []

        def write(self, message: str, *, file) -> None:
            assert file is self.file
            self.messages.append(message)

        def update(self, amount: int) -> None:
            self.updates.append(amount)

        def close(self) -> None:
            return None

    monkeypatch.setattr("binance_datatool.bhds.workflow.archive.download.tqdm", FakeTqdm)

    workflow = ArchiveDownloadWorkflow(
        trade_type=TradeType.um,
        data_freq=DataFrequency.monthly,
        data_type=DataType.funding_rate,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        dry_run=False,
        client=FakeArchiveClient(),
        show_progress=True,
    )

    callback, progress_bar = workflow._build_progress_callback(total_requests=2)
    callback(
        BatchProgressEvent(
            phase="start",
            batch_index=1,
            total_batches=1,
            requested=2,
            attempt=1,
            max_tries=3,
        )
    )
    callback(
        BatchProgressEvent(
            phase="success",
            batch_index=1,
            total_batches=1,
            requested=2,
            attempt=1,
            max_tries=3,
        )
    )

    assert progress_bar.messages == ["Downloading batch 1/1 (2 files)..."]
    assert progress_bar.updates == [2]


def test_verify_workflow_diff_classifies_files(tmp_path) -> None:
    """Verify diffing should split fresh, stale, and legacy-marker files."""
    base_dir = _symbol_verify_dir(tmp_path)
    fresh = _write_verify_pair(base_dir, "fresh.zip", content=b"fresh")
    stale = _write_verify_pair(base_dir, "stale.zip", content=b"stale")
    legacy = _write_verify_pair(base_dir, "legacy.zip", content=b"legacy")
    plain = _write_verify_pair(base_dir, "plain.zip", content=b"plain")

    fresh_marker = fresh.parent / f"{fresh.name}.{int(fresh.stat().st_mtime) + 10}.verified"
    fresh_marker.write_text("", encoding="utf-8")

    stale_marker = stale.parent / f"{stale.name}.{int(stale.stat().st_mtime) - 10}.verified"
    stale_marker.write_text("", encoding="utf-8")

    legacy_marker = legacy.parent / f"{legacy.name}.verified"
    legacy_marker.write_text("", encoding="utf-8")

    workflow = ArchiveVerifyWorkflow(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        interval="1m",
        dry_run=True,
    )

    result = workflow.run()

    assert isinstance(result, VerifyDiffResult)
    assert result.skipped == 1
    assert result.orphan_zips == []
    assert result.orphan_checksums == []
    assert result.to_verify == [legacy, plain, stale]


def test_verify_workflow_diff_detects_orphans(tmp_path) -> None:
    """Verify diffing should classify orphan zip and checksum files."""
    base_dir = _symbol_verify_dir(tmp_path)
    orphan_zip = base_dir / "orphan.zip"
    orphan_zip.parent.mkdir(parents=True, exist_ok=True)
    orphan_zip.write_bytes(b"zip-only")

    orphan_checksum = base_dir / "missing.zip.CHECKSUM"
    orphan_checksum.write_text("abc  missing.zip\n", encoding="utf-8")

    workflow = ArchiveVerifyWorkflow(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        interval="1m",
        dry_run=True,
    )

    result = workflow.run()

    assert isinstance(result, VerifyDiffResult)
    assert result.to_verify == []
    assert result.orphan_zips == [orphan_zip]
    assert result.orphan_checksums == [orphan_checksum]


def test_verify_workflow_dry_run_preserves_files(tmp_path) -> None:
    """Dry-run verify should not mutate files or markers."""
    base_dir = _symbol_verify_dir(tmp_path)
    zip_path = _write_verify_pair(base_dir, "keep.zip", content=b"keep")
    checksum_path = base_dir / "keep.zip.CHECKSUM"
    legacy_marker = base_dir / "keep.zip.verified"
    legacy_marker.write_text("", encoding="utf-8")
    timestamped_marker = base_dir / "keep.zip.123.verified"
    timestamped_marker.write_text("", encoding="utf-8")

    workflow = ArchiveVerifyWorkflow(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        interval="1m",
        dry_run=True,
    )

    result = workflow.run()

    assert isinstance(result, VerifyDiffResult)
    assert zip_path.exists()
    assert checksum_path.exists()
    assert legacy_marker.exists()
    assert timestamped_marker.exists()


def test_verify_workflow_processes_results_default(tmp_path) -> None:
    """Verify should write markers, delete failed pairs, and clean orphans by default."""
    base_dir = _symbol_verify_dir(tmp_path)
    _write_verify_pair(base_dir, "passed.zip", content=b"passed")
    failed = _write_verify_pair(base_dir, "failed.zip", content=b"failed")
    (base_dir / "failed.zip.CHECKSUM").write_text(
        f"{hashlib.sha256(b'wrong').hexdigest()}  failed.zip\n",
        encoding="utf-8",
    )
    failed_marker = base_dir / "failed.zip.100.verified"
    failed_marker.write_text("", encoding="utf-8")

    orphan_zip = base_dir / "orphan.zip"
    orphan_zip.write_bytes(b"orphan")
    orphan_zip_marker = base_dir / "orphan.zip.200.verified"
    orphan_zip_marker.write_text("", encoding="utf-8")

    orphan_checksum = base_dir / "missing.zip.CHECKSUM"
    orphan_checksum.write_text("abc  missing.zip\n", encoding="utf-8")
    orphan_checksum_marker = base_dir / "missing.zip.300.verified"
    orphan_checksum_marker.write_text("", encoding="utf-8")

    workflow = ArchiveVerifyWorkflow(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        interval="1m",
        n_workers=1,
    )

    result = workflow.run()

    assert isinstance(result, VerifyResult)
    assert result.verified == 1
    assert result.failed == 1
    assert result.skipped == 0
    assert result.orphan_zips == 1
    assert result.orphan_checksums == 1
    assert result.failed_details == {failed: "checksum mismatch"}

    passed_markers = list(base_dir.glob("passed.zip.*.verified"))
    assert len(passed_markers) == 1
    assert not (base_dir / "passed.zip.verified").exists()

    assert not failed.exists()
    assert not (base_dir / "failed.zip.CHECKSUM").exists()
    assert not failed_marker.exists()

    assert orphan_zip.exists()
    assert not orphan_zip_marker.exists()
    assert not orphan_checksum.exists()
    assert not orphan_checksum_marker.exists()


def test_verify_workflow_keep_failed_preserves_files(tmp_path) -> None:
    """The keep-failed mode should retain bad zip/checksum files but clear markers."""
    base_dir = _symbol_verify_dir(tmp_path)
    failed = _write_verify_pair(base_dir, "failed.zip", content=b"failed")
    checksum_path = base_dir / "failed.zip.CHECKSUM"
    checksum_path.write_text(
        f"{hashlib.sha256(b'wrong').hexdigest()}  failed.zip\n", encoding="utf-8"
    )
    legacy_marker = base_dir / "failed.zip.verified"
    legacy_marker.write_text("", encoding="utf-8")
    timestamped_marker = base_dir / "failed.zip.100.verified"
    timestamped_marker.write_text("", encoding="utf-8")

    workflow = ArchiveVerifyWorkflow(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        interval="1m",
        keep_failed=True,
        n_workers=1,
    )

    result = workflow.run()

    assert isinstance(result, VerifyResult)
    assert result.failed == 1
    assert failed.exists()
    assert checksum_path.exists()
    assert not legacy_marker.exists()
    assert not timestamped_marker.exists()


def test_verify_workflow_marker_timestamp_rounding(tmp_path) -> None:
    """Marker timestamps should use ceil-rounded source mtimes."""
    base_dir = _symbol_verify_dir(tmp_path)
    zip_path = _write_verify_pair(base_dir, "rounded.zip", content=b"rounded")
    checksum_path = base_dir / "rounded.zip.CHECKSUM"
    os.utime(zip_path, (1000.1, 1000.1))
    os.utime(checksum_path, (1000.9, 1000.9))
    marker = base_dir / "rounded.zip.1001.verified"
    marker.write_text("", encoding="utf-8")

    workflow = ArchiveVerifyWorkflow(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
        symbols=["BTCUSDT"],
        bhds_home=tmp_path,
        interval="1m",
        dry_run=True,
    )

    result = workflow.run()

    assert isinstance(result, VerifyDiffResult)
    assert result.skipped == 1
    assert result.to_verify == []
