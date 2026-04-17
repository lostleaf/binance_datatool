"""Tests for archive marker helpers."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from binance_datatool.bhds.archive.markers import (
    clear_markers,
    collect_markers_by_zip,
    is_marker_fresh,
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


# ---------------------------------------------------------------------------
# is_marker_fresh
# ---------------------------------------------------------------------------


def test_is_marker_fresh_returns_true_when_timestamp_equals_source_mtime(tmp_path: Path) -> None:
    """A marker timestamp exactly matching the source mtime should be considered fresh."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    os.utime(zip_path, (1000.0, 1000.0))
    os.utime(tmp_path / "sample.zip.CHECKSUM", (1000.0, 1000.0))

    assert is_marker_fresh(zip_path, [1000]) is True


def test_is_marker_fresh_returns_true_when_timestamp_newer_than_source(tmp_path: Path) -> None:
    """A marker timestamp newer than the source files should be considered fresh."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    os.utime(zip_path, (1000.0, 1000.0))
    os.utime(tmp_path / "sample.zip.CHECKSUM", (1000.0, 1000.0))

    assert is_marker_fresh(zip_path, [999, 1001]) is True


def test_is_marker_fresh_returns_false_when_all_timestamps_older_than_source(tmp_path: Path) -> None:
    """Marker timestamps all older than the source files should not be considered fresh."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")
    os.utime(zip_path, (1000.9, 1000.9))
    os.utime(tmp_path / "sample.zip.CHECKSUM", (1000.9, 1000.9))
    # max_source_mtime will be ceil(1000.9) == 1001

    assert is_marker_fresh(zip_path, [1000]) is False


def test_is_marker_fresh_returns_false_for_empty_timestamps(tmp_path: Path) -> None:
    """An empty timestamp list should not be considered fresh."""
    zip_path = _write_verify_pair(tmp_path, "sample.zip")

    assert is_marker_fresh(zip_path, []) is False


# ---------------------------------------------------------------------------
# collect_markers_by_zip
# ---------------------------------------------------------------------------


def test_collect_markers_by_zip_returns_empty_for_no_markers(tmp_path: Path) -> None:
    """collect_markers_by_zip should return an empty dict when no markers exist."""
    zip_path = _write_verify_pair(tmp_path, "a.zip")

    result = collect_markers_by_zip([zip_path])

    assert result == {}


def test_collect_markers_by_zip_collects_legacy_marker(tmp_path: Path) -> None:
    """collect_markers_by_zip should find legacy {zip}.verified markers."""
    zip_path = _write_verify_pair(tmp_path, "a.zip")
    marker = tmp_path / "a.zip.verified"
    marker.touch()

    result = collect_markers_by_zip([zip_path])

    assert result == {zip_path: [marker]}


def test_collect_markers_by_zip_collects_timestamped_markers(tmp_path: Path) -> None:
    """collect_markers_by_zip should find all timestamped {zip}.{ts}.verified markers."""
    zip_path = _write_verify_pair(tmp_path, "a.zip")
    m1 = tmp_path / "a.zip.100.verified"
    m2 = tmp_path / "a.zip.200.verified"
    m1.touch()
    m2.touch()

    result = collect_markers_by_zip([zip_path])

    assert zip_path in result
    assert set(result[zip_path]) == {m1, m2}


def test_collect_markers_by_zip_ignores_markers_for_other_zips(tmp_path: Path) -> None:
    """collect_markers_by_zip should not return markers belonging to other zip files."""
    zip_a = _write_verify_pair(tmp_path, "a.zip")
    _write_verify_pair(tmp_path, "b.zip")
    (tmp_path / "b.zip.100.verified").touch()

    result = collect_markers_by_zip([zip_a])

    assert result == {}


def test_collect_markers_by_zip_handles_multiple_directories(tmp_path: Path) -> None:
    """collect_markers_by_zip should scan each unique directory once."""
    dir_a = tmp_path / "dir_a"
    dir_b = tmp_path / "dir_b"
    zip_a = _write_verify_pair(dir_a, "a.zip")
    zip_b = _write_verify_pair(dir_b, "b.zip")
    m_a = dir_a / "a.zip.100.verified"
    m_b = dir_b / "b.zip.200.verified"
    m_a.touch()
    m_b.touch()

    result = collect_markers_by_zip([zip_a, zip_b])

    assert result == {zip_a: [m_a], zip_b: [m_b]}


def test_collect_markers_by_zip_malformed_marker_is_collected(tmp_path: Path) -> None:
    """Malformed {zip}.bad.verified markers should be collected for cleanup."""
    zip_path = _write_verify_pair(tmp_path, "a.zip")
    malformed = tmp_path / "a.zip.bad.verified"
    malformed.touch()

    result = collect_markers_by_zip([zip_path])

    assert result == {zip_path: [malformed]}
