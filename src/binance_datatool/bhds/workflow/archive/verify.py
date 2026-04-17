"""Workflow for verifying local archive files."""

from __future__ import annotations

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TYPE_CHECKING

from binance_datatool.bhds.archive import (
    clear_markers,
    is_marker_valid,
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

    def _scan(self) -> VerifyDiffResult:
        """Scan local symbol directories and classify verify work."""
        to_verify: list[Path] = []
        orphan_zips: list[Path] = []
        orphan_checksums: list[Path] = []
        skipped = 0

        for symbol in self.symbols:
            local_symbol_dir = symbol_dir(
                self.bhds_home,
                self.trade_type,
                self.data_freq,
                self.data_type,
                symbol,
                self.interval,
            )
            if not local_symbol_dir.exists():
                continue

            zip_paths = sorted(local_symbol_dir.glob("*.zip"))
            checksum_paths = sorted(local_symbol_dir.glob("*.zip.CHECKSUM"))

            for zip_path in zip_paths:
                checksum_path = zip_path.parent / f"{zip_path.name}.CHECKSUM"
                if not checksum_path.exists():
                    orphan_zips.append(zip_path)
                    continue

                if is_marker_valid(zip_path):
                    skipped += 1
                else:
                    to_verify.append(zip_path)

            for checksum_path in checksum_paths:
                zip_path = checksum_path.with_name(checksum_path.name.removesuffix(".CHECKSUM"))
                if not zip_path.exists():
                    orphan_checksums.append(checksum_path)

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

    def _apply_verify_result(self, result: VerifyFileResult) -> None:
        """Apply filesystem side effects for one verify outcome."""
        zip_path = result.zip_path
        checksum_path = zip_path.parent / f"{zip_path.name}.CHECKSUM"
        clear_markers(zip_path)

        if result.passed:
            write_marker(zip_path)
            return

        if self.keep_failed:
            return

        zip_path.unlink(missing_ok=True)
        checksum_path.unlink(missing_ok=True)

    def run(self) -> VerifyDiffResult | VerifyResult:
        """Run the local verify workflow.

        Returns:
            :class:`VerifyDiffResult` when ``dry_run`` is enabled, otherwise
            :class:`VerifyResult` with aggregated counts and per-file failure
            details.
        """
        diff_result = self._scan()
        if self.dry_run:
            return diff_result

        self._clean_orphans(diff_result)
        verify_results = self._verify_paths(diff_result.to_verify)
        failed_details: dict[Path, str] = {}
        verified = 0

        for result in verify_results:
            self._apply_verify_result(result)
            if result.passed:
                verified += 1
                continue
            failed_details[result.zip_path] = result.detail

        return VerifyResult(
            skipped=diff_result.skipped,
            verified=verified,
            orphan_zips=len(diff_result.orphan_zips),
            orphan_checksums=len(diff_result.orphan_checksums),
            failed_details=failed_details,
        )
