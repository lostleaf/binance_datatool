"""Verification marker helpers for local archive files."""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from binance_datatool.common import DataFrequency, DataType, TradeType


def symbol_dir(
    bhds_home: Path,
    trade_type: TradeType,
    data_freq: DataFrequency,
    data_type: DataType,
    symbol: str,
    interval: str | None = None,
) -> Path:
    """Build the local directory that stores files for one symbol.

    Args:
        bhds_home: Root directory for local BHDS data storage.
        trade_type: Market segment.
        data_freq: Partition frequency.
        data_type: Dataset type.
        symbol: Symbol directory name.
        interval: Optional interval subdirectory for kline-class data.

    Returns:
        Local directory path for the symbol.
    """
    path = (
        bhds_home
        / "aws_data"
        / "data"
        / trade_type.s3_path
        / data_freq.value
        / data_type.value
        / symbol
    )
    if interval is not None:
        path /= interval
    return path


def clear_markers(zip_path: Path) -> None:
    """Delete all verification markers for a zip file.

    Args:
        zip_path: Zip file whose legacy and timestamped markers should be
            removed.
    """
    markers = [zip_path.parent / f"{zip_path.name}.verified"]
    markers.extend(sorted(zip_path.parent.glob(f"{zip_path.name}.*.verified")))
    for marker_path in markers:
        marker_path.unlink(missing_ok=True)


def max_source_mtime(zip_path: Path) -> int:
    """Return the normalized freshness timestamp for a zip/checksum pair.

    Args:
        zip_path: Zip file path.

    Returns:
        The ceiling of the newer mtime between the zip and its checksum.
    """
    checksum_path = zip_path.parent / f"{zip_path.name}.CHECKSUM"
    return math.ceil(max(zip_path.stat().st_mtime, checksum_path.stat().st_mtime))


def is_marker_valid(zip_path: Path) -> bool:
    """Return whether a zip file already has a fresh timestamped marker.

    Args:
        zip_path: Zip file path.

    Returns:
        ``True`` when at least one timestamped marker is fresh enough for
        the current zip/checksum pair, otherwise ``False``.
    """
    timestamps: list[int] = []
    for marker_path in zip_path.parent.glob(f"{zip_path.name}.*.verified"):
        timestamp_str = (
            marker_path.name.removeprefix(zip_path.name).removesuffix(".verified").strip(".")
        )
        try:
            timestamps.append(int(timestamp_str))
        except ValueError:
            continue

    if not timestamps:
        return False

    return max(timestamps) >= max_source_mtime(zip_path)


def write_marker(zip_path: Path) -> None:
    """Create a fresh timestamped marker for a verified zip file.

    Args:
        zip_path: Verified zip file path.
    """
    timestamp = max(int(time.time()), max_source_mtime(zip_path))
    marker_path = zip_path.parent / f"{zip_path.name}.{timestamp}.verified"
    marker_path.touch()
