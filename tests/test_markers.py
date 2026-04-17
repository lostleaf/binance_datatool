"""Tests for archive marker helpers."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from binance_datatool.bhds.archive.markers import (
    clear_markers,
    is_marker_valid,
    max_source_mtime,
    symbol_dir,
    write_marker,
)
from binance_datatool.common import DataFrequency, DataType, TradeType


def _write_verify_pair(base_dir: Path, name: str) -> Path:
    """Create a zip/checksum pair used by marker tests."""
    zip_path = base_dir / name
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.write_bytes(b"zip-bytes")
    (base_dir / f"{name}.CHECKSUM").write_text("checksum  placeholder\n", encoding="utf-8")
    return zip_path


@pytest.mark.parametrize(
    ("trade_type", "data_freq", "data_type", "symbol", "interval", "expected"),
    [
        (
            TradeType.spot,
            DataFrequency.daily,
            DataType.klines,
            "BTCUSDT",
            "1m",
            Path("aws_data/data/spot/daily/klines/BTCUSDT/1m"),
        ),
        (
            TradeType.um,
            DataFrequency.monthly,
            DataType.funding_rate,
            "BTCUSDT",
            None,
            Path("aws_data/data/futures/um/monthly/fundingRate/BTCUSDT"),
        ),
        (
            TradeType.cm,
            DataFrequency.daily,
            DataType.metrics,
            "BTCUSD_PERP",
            None,
            Path("aws_data/data/futures/cm/daily/metrics/BTCUSD_PERP"),
        ),
    ],
)
def test_symbol_dir_builds_expected_path(
    tmp_path: Path,
    trade_type: TradeType,
    data_freq: DataFrequency,
    data_type: DataType,
    symbol: str,
    interval: str | None,
    expected: Path,
) -> None:
    """The symbol directory should match the BHDS local storage layout."""
    assert symbol_dir(tmp_path, trade_type, data_freq, data_type, symbol, interval) == (
        tmp_path / expected
    )


def test_clear_markers_removes_legacy_and_timestamped_markers(tmp_path: Path) -> None:
    """Marker cleanup should delete both legacy and timestamped marker formats."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    legacy_marker = tmp_path / "sample.zip.verified"
    fresh_marker = tmp_path / "sample.zip.100.verified"
    stale_marker = tmp_path / "sample.zip.200.verified"
    legacy_marker.write_text("", encoding="utf-8")
    fresh_marker.write_text("", encoding="utf-8")
    stale_marker.write_text("", encoding="utf-8")

    clear_markers(zip_path)

    assert not legacy_marker.exists()
    assert not fresh_marker.exists()
    assert not stale_marker.exists()


def test_clear_markers_is_noop_without_existing_markers(tmp_path: Path) -> None:
    """Marker cleanup should not fail when no markers exist."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")

    clear_markers(zip_path)

    assert zip_path.exists()
    assert (tmp_path / "sample.zip.CHECKSUM").exists()


def test_max_source_mtime_uses_newer_zip_mtime(tmp_path: Path) -> None:
    """The zip/checksum freshness timestamp should use the newer zip mtime."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    checksum_path = tmp_path / "sample.zip.CHECKSUM"
    os.utime(zip_path, (1000.9, 1000.9))
    os.utime(checksum_path, (1000.1, 1000.1))

    assert max_source_mtime(zip_path) == 1001


def test_max_source_mtime_uses_newer_checksum_mtime(tmp_path: Path) -> None:
    """The zip/checksum freshness timestamp should use the newer checksum mtime."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    checksum_path = tmp_path / "sample.zip.CHECKSUM"
    os.utime(zip_path, (1000.1, 1000.1))
    os.utime(checksum_path, (1000.9, 1000.9))

    assert max_source_mtime(zip_path) == 1001


def test_is_marker_valid_with_fresh_timestamped_marker(tmp_path: Path) -> None:
    """Fresh timestamped markers should skip verification."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    marker = tmp_path / "sample.zip.1001.verified"
    os.utime(zip_path, (1000.1, 1000.1))
    os.utime(tmp_path / "sample.zip.CHECKSUM", (1000.9, 1000.9))
    marker.write_text("", encoding="utf-8")

    assert is_marker_valid(zip_path) is True


def test_is_marker_valid_with_stale_timestamped_marker(tmp_path: Path) -> None:
    """Stale timestamped markers should not skip verification."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    marker = tmp_path / "sample.zip.1000.verified"
    os.utime(zip_path, (1000.1, 1000.1))
    os.utime(tmp_path / "sample.zip.CHECKSUM", (1000.9, 1000.9))
    marker.write_text("", encoding="utf-8")

    assert is_marker_valid(zip_path) is False


def test_is_marker_valid_without_timestamped_marker(tmp_path: Path) -> None:
    """Missing timestamped markers should force verification."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")

    assert is_marker_valid(zip_path) is False


def test_is_marker_valid_with_legacy_marker_only(tmp_path: Path) -> None:
    """Legacy markers alone should not count as fresh verification state."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    (tmp_path / "sample.zip.verified").write_text("", encoding="utf-8")

    assert is_marker_valid(zip_path) is False


def test_write_marker_creates_fresh_timestamped_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Written markers should use a timestamp no older than the source pair."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    checksum_path = tmp_path / "sample.zip.CHECKSUM"
    os.utime(zip_path, (1000.1, 1000.1))
    os.utime(checksum_path, (1000.9, 1000.9))
    monkeypatch.setattr("binance_datatool.bhds.archive.markers.time.time", lambda: 1000.0)

    write_marker(zip_path)

    markers = list(tmp_path.glob("sample.zip.*.verified"))
    assert len(markers) == 1
    timestamp = int(markers[0].name.removeprefix("sample.zip.").removesuffix(".verified"))
    assert timestamp >= max_source_mtime(zip_path)
