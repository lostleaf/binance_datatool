"""Workflow for verifying local archive files."""

from __future__ import annotations

import multiprocessing
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import TYPE_CHECKING

from loguru import logger

from binance_datatool.bhds.archive import (
    clear_markers,
    collect_markers_by_zip,
    is_marker_fresh,
    symbol_dir,
    verify_single_file,
    write_marker,
)
from binance_datatool.common.progress import ProgressEvent, make_reporter

from ._shared import validate_interval
from .results import VerifyDiffResult, VerifyResult

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from binance_datatool.bhds.archive import VerifyFileResult
    from binance_datatool.common import DataFrequency, DataType, TradeType

_SCAN_WORKERS = 16


@dataclass(slots=True)
class _SymbolScanResult:
    """Per-symbol scan outcome used internally by the verify workflow."""

    to_verify: list[Path]
    orphan_zips: list[Path]
    orphan_checksums: list[Path]
    skipped: int


def _parse_marker_timestamps(
    verified_names: set[str],
    zip_names: set[str],
) -> dict[str, list[int]]:
    """Parse timestamped verified markers and group by zip name.

    Args:
        verified_names: Set of ``*.verified`` filenames from a symbol directory.
        zip_names: Set of known ``*.zip`` filenames in the same directory.

    Returns:
        Mapping from zip filename to its list of marker timestamps.
    """
    markers: dict[str, list[int]] = defaultdict(list)
    for name in verified_names:
        without_suffix = name.removesuffix(".verified")
        last_dot = without_suffix.rfind(".")
        if last_dot == -1:
            continue
        ts_str = without_suffix[last_dot + 1 :]
        zip_name = without_suffix[:last_dot]
        try:
            ts = int(ts_str)
        except ValueError:
            continue
        if zip_name in zip_names:
            markers[zip_name].append(ts)
    return markers


class ArchiveVerifyWorkflow:
    """Workflow for verifying local archive zip files against SHA256 checksums."""

    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        symbols: Sequence[str],
        bhds_home: Path,
        interval: str | None = None,
        keep_failed: bool = False,
        dry_run: bool = False,
        n_workers: int | None = None,
        progress_bar: bool = False,
    ) -> None:
        """Initialize the verify workflow.

        Args:
            trade_type: Market segment to query.
            data_freq: Partition frequency.
            data_type: Dataset type.
            symbols: Symbols to verify, preserving caller order.
            bhds_home: Root directory for local BHDS data storage.
            interval: Interval directory for kline-class data types.
            keep_failed: When ``True``, retain failed zip and checksum files
                instead of deleting them.
            dry_run: When ``True``, scan and classify files without verifying
                or mutating the filesystem.
            n_workers: Process pool size. Defaults to ``max(1, cpu_count - 2)``.
            progress_bar: Whether to display a tqdm progress bar on stderr.
        """
        validate_interval(data_type, interval)

        self.trade_type = trade_type
        self.data_freq = data_freq
        self.data_type = data_type
        self.symbols = list(symbols)
        self.bhds_home = bhds_home
        self.interval = interval
        self.keep_failed = keep_failed
        self.dry_run = dry_run
        self.n_workers = n_workers or max(1, (os.cpu_count() or 1) - 2)
        self.progress_bar = progress_bar

    def _scan_symbol(self, symbol: str) -> _SymbolScanResult:
        """Scan one symbol directory and classify its files using pre-globbed sets."""
        local_symbol_dir = symbol_dir(
            self.bhds_home,
            self.trade_type,
            self.data_freq,
            self.data_type,
            symbol,
            self.interval,
        )
        if not local_symbol_dir.exists():
            return _SymbolScanResult(to_verify=[], orphan_zips=[], orphan_checksums=[], skipped=0)

        zip_paths = sorted(local_symbol_dir.glob("*.zip"))
        zip_names = {p.name for p in zip_paths}
        checksum_names = {p.name for p in local_symbol_dir.glob("*.zip.CHECKSUM")}
        verified_names = {p.name for p in local_symbol_dir.glob("*.verified")}

        marker_ts = _parse_marker_timestamps(verified_names, zip_names)

        to_verify: list[Path] = []
        orphan_zips: list[Path] = []
        skipped = 0

        for zip_path in zip_paths:
            if f"{zip_path.name}.CHECKSUM" not in checksum_names:
                orphan_zips.append(zip_path)
                continue

            timestamps = marker_ts.get(zip_path.name, [])
            if is_marker_fresh(zip_path, timestamps):
                skipped += 1
            else:
                to_verify.append(zip_path)

        orphan_checksums = sorted(
            local_symbol_dir / cn
            for cn in checksum_names
            if cn.removesuffix(".CHECKSUM") not in zip_names
        )

        return _SymbolScanResult(
            to_verify=to_verify,
            orphan_zips=orphan_zips,
            orphan_checksums=orphan_checksums,
            skipped=skipped,
        )

    def _scan(self) -> VerifyDiffResult:
        """Scan local symbol directories and classify verify work."""
        if not self.symbols:
            return VerifyDiffResult(to_verify=[], skipped=0, orphan_zips=[], orphan_checksums=[])

        to_verify: list[Path] = []
        orphan_zips: list[Path] = []
        orphan_checksums: list[Path] = []
        skipped = 0

        max_workers = min(len(self.symbols), _SCAN_WORKERS)

        with (
            make_reporter(
                self.progress_bar,
                total=len(self.symbols),
                description="Scan",
            ) as reporter,
            ThreadPoolExecutor(max_workers=max_workers) as executor,
        ):
            futures = {
                executor.submit(self._scan_symbol, symbol): symbol
                for symbol in self.symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                result = future.result()
                to_verify.extend(result.to_verify)
                orphan_zips.extend(result.orphan_zips)
                orphan_checksums.extend(result.orphan_checksums)
                skipped += result.skipped
                reporter.tick(ProgressEvent(name=symbol, ok=True))

        return VerifyDiffResult(
            to_verify=to_verify,
            skipped=skipped,
            orphan_zips=orphan_zips,
            orphan_checksums=orphan_checksums,
        )

    def _clean_orphans(self, diff_result: VerifyDiffResult) -> None:
        """Apply orphan cleanup rules before verification."""
        for zip_path in diff_result.orphan_zips:
            clear_markers(zip_path)

        for checksum_path in diff_result.orphan_checksums:
            zip_path = checksum_path.with_name(checksum_path.name.removesuffix(".CHECKSUM"))
            checksum_path.unlink(missing_ok=True)
            clear_markers(zip_path)

    def _verify_paths(self, zip_paths: list[Path]) -> list[VerifyFileResult]:
        """Verify zip files in parallel using a process pool."""
        if not zip_paths:
            return []

        with (
            make_reporter(
                self.progress_bar,
                total=len(zip_paths),
                description="Verify",
            ) as reporter,
            ProcessPoolExecutor(
                max_workers=self.n_workers,
                mp_context=multiprocessing.get_context("spawn"),
            ) as executor,
        ):
            futures = [executor.submit(verify_single_file, zip_path) for zip_path in zip_paths]
            results: list[VerifyFileResult] = []
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                reporter.tick(ProgressEvent(name=result.zip_path.name, ok=result.passed))

        result_by_path = {result.zip_path: result for result in results}
        return [result_by_path[zip_path] for zip_path in zip_paths]

    def _apply_all_verify_results(
        self, results: list[VerifyFileResult],
    ) -> tuple[int, dict[Path, str]]:
        """Apply filesystem side effects for all verify outcomes.

        Pre-collects markers per directory to avoid per-file glob calls.

        Returns:
            Tuple of (verified count, failed details mapping).
        """
        markers_by_zip = collect_markers_by_zip([result.zip_path for result in results])

        verified = 0
        failed_details: dict[Path, str] = {}

        for result in results:
            zip_path = result.zip_path

            for marker_path in markers_by_zip.get(zip_path, []):
                marker_path.unlink(missing_ok=True)

            if result.passed:
                write_marker(zip_path)
                verified += 1
                continue

            failed_details[zip_path] = result.detail
            if not self.keep_failed:
                checksum_path = zip_path.parent / f"{zip_path.name}.CHECKSUM"
                zip_path.unlink(missing_ok=True)
                checksum_path.unlink(missing_ok=True)

        return verified, failed_details

    def run(self) -> VerifyDiffResult | VerifyResult:
        """Run the local verify workflow.

        Returns:
            :class:`VerifyDiffResult` when ``dry_run`` is enabled, otherwise
            :class:`VerifyResult` with aggregated counts and per-file failure
            details.
        """
        logger.info("scanning local store for verify work")
        diff_result = self._scan()
        logger.info(
            "scan complete: {} already verified, {} to verify, {} orphan zip(s),"
            " {} orphan checksum(s)",
            diff_result.skipped,
            len(diff_result.to_verify),
            len(diff_result.orphan_zips),
            len(diff_result.orphan_checksums),
        )

        if self.dry_run:
            return diff_result

        if diff_result.orphan_zips or diff_result.orphan_checksums:
            logger.info(
                "cleaning {} orphan zip(s) and {} orphan checksum(s)",
                len(diff_result.orphan_zips),
                len(diff_result.orphan_checksums),
            )
        self._clean_orphans(diff_result)

        logger.info("verifying {} file(s) with {} worker(s)", len(diff_result.to_verify), self.n_workers)
        verify_results = self._verify_paths(diff_result.to_verify)
        verified, failed_details = self._apply_all_verify_results(verify_results)

        logger.info("verify complete: {} passed, {} failed", verified, len(failed_details))

        return VerifyResult(
            skipped=diff_result.skipped,
            verified=verified,
            orphan_zips=len(diff_result.orphan_zips),
            orphan_checksums=len(diff_result.orphan_checksums),
            failed_details=failed_details,
        )
