"""Workflow for downloading archive files."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from loguru import logger
from tqdm import tqdm

from binance_datatool.bhds.archive import (
    ArchiveClient,
    DownloadRequest,
    clear_markers,
    download_archive_files,
)

from .list_files import ArchiveListFilesWorkflow
from .results import DiffEntry, DiffResult, DownloadResult, SymbolListingError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from binance_datatool.bhds.archive import Aria2DownloadResult, BatchProgressEvent
    from binance_datatool.common import DataFrequency, DataType, TradeType

    from .results import ListFilesResult


class ArchiveDownloadWorkflow:
    """Workflow for diffing and downloading archive files to the local BHDS store."""

    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        symbols: Sequence[str],
        bhds_home: Path,
        interval: str | None = None,
        dry_run: bool = False,
        inherit_aria2_proxy: bool = False,
        show_progress: bool = False,
        client: ArchiveClient | None = None,
        download_func: Callable[..., Aria2DownloadResult] | None = None,
    ) -> None:
        """Initialize the download workflow.

        Args:
            trade_type: Market segment to query.
            data_freq: Partition frequency.
            data_type: Dataset type.
            symbols: Symbols to download, preserving caller order.
            bhds_home: Root directory for local BHDS data storage.
            interval: Interval directory for kline-class data types.
            dry_run: When ``True``, compute the diff without downloading.
            inherit_aria2_proxy: Whether aria2c should inherit proxy env vars.
            show_progress: Whether to display a tqdm progress bar on stderr.
            client: Optional pre-configured archive client.
            download_func: Optional download callable for dependency injection.
        """
        self.trade_type = trade_type
        self.data_freq = data_freq
        self.data_type = data_type
        self.symbols = list(symbols)
        self.bhds_home = bhds_home
        self.interval = interval
        self.dry_run = dry_run
        self.inherit_aria2_proxy = inherit_aria2_proxy
        self.show_progress = show_progress
        self.client = client or ArchiveClient()
        self.download_func = download_func or download_archive_files

    def _build_diff_result(self, list_result: ListFilesResult) -> DiffResult:
        """Compute the download diff from remote file listings."""
        to_download: list[DiffEntry] = []
        skipped = 0
        listing_errors: list[SymbolListingError] = []

        for entry in list_result.per_symbol:
            if entry.error is not None:
                listing_errors.append(SymbolListingError(symbol=entry.symbol, error=entry.error))
                continue

            for remote_file in entry.files:
                local_path = self.bhds_home / "aws_data" / Path(remote_file.key)
                if (
                    local_path.exists()
                    and local_path.stat().st_mtime >= remote_file.last_modified.timestamp()
                ):
                    skipped += 1
                    continue

                reason: Literal["new", "updated"] = "updated" if local_path.exists() else "new"
                to_download.append(
                    DiffEntry(remote=remote_file, local_path=local_path, reason=reason)
                )

        return DiffResult(
            to_download=to_download,
            skipped=skipped,
            total_remote=list_result.total_remote_files,
            listing_errors=listing_errors,
        )

    def _invalidate_verified_markers(self, entries: Sequence[DiffEntry]) -> None:
        """Delete stale verify markers for updated zip or checksum files."""
        targets: set[Path] = set()
        for entry in entries:
            if entry.reason != "updated":
                continue

            path = entry.local_path
            if path.name.endswith(".CHECKSUM"):
                targets.add(path.with_name(path.name.removesuffix(".CHECKSUM")))
            elif path.name.endswith(".zip"):
                targets.add(path)

        for zip_path in targets:
            clear_markers(zip_path)

    def _build_progress_callback(
        self,
        total_requests: int,
    ) -> tuple[Callable[[BatchProgressEvent], None], tqdm]:
        """Create a callback that prints batch progress to stderr."""
        progress_bar = tqdm(
            total=total_requests,
            disable=not self.show_progress,
            file=sys.stderr,
            unit="file",
            leave=True,
        )

        def emit(message: str) -> None:
            """Write batch-status messages without corrupting the tqdm cursor state."""
            if self.show_progress:
                progress_bar.write(message, file=sys.stderr)
                return
            logger.info(message)

        def callback(event: BatchProgressEvent) -> None:
            if event.phase == "start":
                if event.attempt == 1:
                    emit(
                        f"Downloading batch {event.batch_index}/{event.total_batches} "
                        f"({event.requested} files)..."
                    )
                else:
                    emit(
                        f"Retrying batch {event.batch_index}/{event.total_batches} "
                        f"({event.requested} files), attempt {event.attempt}/{event.max_tries}..."
                    )
                return

            if event.phase in {"success", "failed"}:
                progress_bar.update(event.requested)

        return callback, progress_bar

    async def run(self) -> DiffResult | DownloadResult:
        """Execute the workflow and return either a diff or a download result.

        Returns:
            :class:`DiffResult` when ``dry_run`` is enabled, otherwise
            :class:`DownloadResult` with aggregated counts and errors.
        """
        list_workflow = ArchiveListFilesWorkflow(
            trade_type=self.trade_type,
            data_freq=self.data_freq,
            data_type=self.data_type,
            symbols=self.symbols,
            interval=self.interval,
            client=self.client,
        )
        list_result = await list_workflow.run()
        logger.info("scanning local store for diff")
        diff_result = self._build_diff_result(list_result)
        logger.info(
            "diff complete: {} up to date, {} to download",
            diff_result.skipped,
            len(diff_result.to_download),
        )

        if self.dry_run:
            return diff_result

        if not diff_result.to_download:
            return DownloadResult(
                total_remote=diff_result.total_remote,
                skipped=diff_result.skipped,
                downloaded=0,
                failed=0,
                listing_errors=diff_result.listing_errors,
            )

        self.bhds_home.mkdir(parents=True, exist_ok=True)
        self._invalidate_verified_markers(diff_result.to_download)

        requests = [
            DownloadRequest(url=entry.url, local_path=entry.local_path)
            for entry in diff_result.to_download
        ]

        logger.info("downloading {} file(s)", len(requests))
        callback, progress_bar = self._build_progress_callback(len(requests))
        try:
            aria2_result = self.download_func(
                requests,
                inherit_proxy=self.inherit_aria2_proxy,
                progress_callback=callback,
            )
        finally:
            progress_bar.close()

        return DownloadResult(
            total_remote=diff_result.total_remote,
            skipped=diff_result.skipped,
            downloaded=aria2_result.succeeded,
            failed=len(aria2_result.failed_requests),
            listing_errors=diff_result.listing_errors,
        )
