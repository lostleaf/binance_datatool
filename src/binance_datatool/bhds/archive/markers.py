"""Verification marker helpers for local archive files."""

from __future__ import annotations

import math
import time
from collections import defaultdict
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


def write_marker(zip_path: Path) -> None:
    """Create a fresh timestamped marker for a verified zip file.

    Args:
        zip_path: Verified zip file path.
    """
    timestamp = max(int(time.time()), max_source_mtime(zip_path))
    marker_path = zip_path.parent / f"{zip_path.name}.{timestamp}.verified"
    marker_path.touch()


def _match_marker_to_zip(marker_name: str, zip_names: set[str]) -> str | None:
    """Match a ``*.verified`` filename to the zip file it belongs to.

    Handles both legacy (``{zip}.verified``) and timestamped
    (``{zip}.{ts}.verified``) patterns. Malformed marker names that cannot be
    matched are treated as belonging to the zip for cleanup purposes.

    Returns:
        The matching zip filename, or ``None`` if no match is found.
    """
    without_suffix = marker_name.removesuffix(".verified")
    if without_suffix in zip_names:
        return without_suffix
    last_dot = without_suffix.rfind(".")
    if last_dot != -1 and without_suffix[:last_dot] in zip_names:
        return without_suffix[:last_dot]
    return None


def is_marker_fresh(zip_path: Path, timestamps: list[int]) -> bool:
    """Return True if any marker timestamp is no older than the zip/checksum pair.

    Args:
        zip_path: Zip file to check. Both the zip and its sibling ``.CHECKSUM``
            file must exist on disk; this function calls ``max_source_mtime``
            which stats both files.
        timestamps: List of marker timestamps for this zip.

    Returns:
        True if the newest marker timestamp is at least as recent as the source
        files; False when the list is empty or all timestamps are stale.
    """
    return bool(timestamps) and max(timestamps) >= max_source_mtime(zip_path)


def collect_markers_by_zip(zip_paths: list[Path]) -> dict[Path, list[Path]]:
    """Collect all verification markers for a list of zip files.

    Scans each unique parent directory once and maps each zip path to its
    matching marker paths. Zip paths with no markers are omitted from the
    returned mapping.

    Args:
        zip_paths: Zip file paths to find markers for.

    Returns:
        Mapping from zip path to its associated marker paths.
    """
    dirs_to_zip_names: dict[Path, set[str]] = defaultdict(set)
    for zip_path in zip_paths:
        dirs_to_zip_names[zip_path.parent].add(zip_path.name)

    markers_by_zip: dict[Path, list[Path]] = defaultdict(list)
    for dir_path, zip_names in dirs_to_zip_names.items():
        for marker_path in dir_path.glob("*.verified"):
            zip_name = _match_marker_to_zip(marker_path.name, zip_names)
            if zip_name is not None:
                markers_by_zip[dir_path / zip_name].append(marker_path)

    return dict(markers_by_zip)
