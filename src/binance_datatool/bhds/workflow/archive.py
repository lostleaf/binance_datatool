"""Archive workflows."""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import aiohttp
from loguru import logger
from tqdm import tqdm

from binance_datatool.bhds.archive import (
    ArchiveClient,
    DownloadRequest,
    download_archive_files,
)
from binance_datatool.common import (
    S3_DOWNLOAD_PREFIX,
    S3_HTTP_TIMEOUT_SECONDS,
    TradeType,
    infer_cm_info,
    infer_spot_info,
    infer_um_info,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from binance_datatool.bhds.archive import (
        ArchiveFile,
        Aria2DownloadResult,
        BatchProgressEvent,
        SymbolFilter,
    )
    from binance_datatool.common import DataFrequency, DataType, SymbolInfo


@dataclass(slots=True)
class ListSymbolsResult:
    """Structured result for archive symbol listing."""

    matched: list[SymbolInfo]
    unmatched: list[str]
    filtered_out: list[SymbolInfo]


@dataclass(slots=True)
class SymbolListFilesResult:
    """Result for listing files under one symbol.

    A successful listing of an empty directory is represented by ``files``
    equal to ``[]`` with ``error`` still ``None``; this is distinct from a
    failed listing, which also carries an empty ``files`` list but sets
    ``error`` to a non-empty description of the failure.
    """

    symbol: str
    files: list[ArchiveFile]
    error: str | None = None


@dataclass(slots=True)
class ListFilesResult:
    """Aggregate result for listing files across multiple symbols."""

    per_symbol: list[SymbolListFilesResult]

    @property
    def has_failures(self) -> bool:
        """Return whether any requested symbol failed."""
        return any(entry.error is not None for entry in self.per_symbol)

    @property
    def total_remote_files(self) -> int:
        """Return the total number of successfully listed remote files."""
        return sum(len(entry.files) for entry in self.per_symbol if entry.error is None)


@dataclass(slots=True)
class SymbolListingError:
    """Structured per-symbol listing error."""

    symbol: str
    error: str


@dataclass(slots=True)
class DiffEntry:
    """One file selected for download."""

    remote: ArchiveFile
    local_path: Path
    reason: Literal["new", "updated"]

    @property
    def url(self) -> str:
        """Return the direct download URL for this remote file."""
        return f"{S3_DOWNLOAD_PREFIX}/{self.remote.key}"


@dataclass(slots=True)
class DiffResult:
    """Structured diff result for archive downloads."""

    to_download: list[DiffEntry]
    skipped: int
    total_remote: int
    listing_errors: list[SymbolListingError]

    @property
    def listing_failed_symbols(self) -> int:
        """Return the number of symbols whose remote listing failed."""
        return len(self.listing_errors)


@dataclass(slots=True)
class DownloadResult:
    """Structured result for a download run."""

    total_remote: int
    skipped: int
    downloaded: int
    failed: int
    listing_errors: list[SymbolListingError]

    @property
    def listing_failed_symbols(self) -> int:
        """Return the number of symbols whose remote listing failed."""
        return len(self.listing_errors)


def _infer_symbol_info(trade_type: TradeType, symbol: str) -> SymbolInfo | None:
    """Infer typed symbol metadata for the requested market segment.

    Args:
        trade_type: Market segment being listed.
        symbol: Raw symbol from the archive directory listing.

    Returns:
        Parsed symbol metadata, or ``None`` when inference fails.
    """
    match trade_type:
        case TradeType.spot:
            return infer_spot_info(symbol)
        case TradeType.um:
            return infer_um_info(symbol)
        case TradeType.cm:
            return infer_cm_info(symbol)


class ArchiveListSymbolsWorkflow:
    """Workflow for listing available symbols from the archive.

    Fetches raw symbols via :class:`ArchiveClient`, infers typed metadata
    per market segment, and optionally applies a typed symbol filter.
    """

    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        symbol_filter: SymbolFilter | None = None,
        client: ArchiveClient | None = None,
    ) -> None:
        """Initialize the workflow.

        Args:
            trade_type: Market segment to query.
            data_freq: Partition frequency.
            data_type: Dataset type.
            symbol_filter: Optional typed symbol filter for inferred metadata.
            client: Optional pre-configured archive client.  A default
                :class:`ArchiveClient` is created when ``None``.
        """
        self.trade_type = trade_type
        self.data_freq = data_freq
        self.data_type = data_type
        self.symbol_filter = symbol_filter
        self.client = client or ArchiveClient()

    async def run(self) -> ListSymbolsResult:
        """Execute the workflow and return structured symbol results.

        Returns:
            Inferred symbols split into matched, unmatched, and filtered-out buckets.
        """
        raw_symbols = await self.client.list_symbols(
            self.trade_type, self.data_freq, self.data_type
        )

        inferred: list[SymbolInfo] = []
        unmatched: list[str] = []
        for symbol in raw_symbols:
            info = _infer_symbol_info(self.trade_type, symbol)
            if info is None:
                unmatched.append(symbol)
                continue
            inferred.append(info)

        if self.symbol_filter is None:
            return ListSymbolsResult(matched=inferred, unmatched=unmatched, filtered_out=[])

        matched: list[SymbolInfo] = []
        filtered_out: list[SymbolInfo] = []
        for info in inferred:
            if self.symbol_filter.matches(info):
                matched.append(info)
            else:
                filtered_out.append(info)

        return ListSymbolsResult(
            matched=matched,
            unmatched=unmatched,
            filtered_out=filtered_out,
        )


class ArchiveListFilesWorkflow:
    """Workflow for listing archive files under one or more symbol directories.

    Fetches file metadata concurrently via :class:`ArchiveClient` while
    preserving the caller-provided symbol order and isolating per-symbol
    failures so that one bad symbol does not abort the entire batch.
    """

    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        symbols: Sequence[str],
        interval: str | None = None,
        client: ArchiveClient | None = None,
    ) -> None:
        """Initialize the workflow.

        Args:
            trade_type: Market segment to query.
            data_freq: Partition frequency.
            data_type: Dataset type.
            symbols: Symbols to list, preserving caller order.
            interval: Interval directory for kline-class data types.
            client: Optional pre-configured archive client.
        """
        if data_type.has_interval_layer and interval is None:
            msg = "interval is required for kline-class data_type"
            raise ValueError(msg)
        if not data_type.has_interval_layer and interval is not None:
            msg = "interval is not applicable to non-kline data_type"
            raise ValueError(msg)

        self.trade_type = trade_type
        self.data_freq = data_freq
        self.data_type = data_type
        self.symbols = list(symbols)
        self.interval = interval
        self.client = client or ArchiveClient()

    def _create_session(self) -> aiohttp.ClientSession:
        """Create a shared HTTP session for one workflow run."""
        timeout = aiohttp.ClientTimeout(total=S3_HTTP_TIMEOUT_SECONDS)
        return aiohttp.ClientSession(timeout=timeout, trust_env=True)

    async def run(self) -> ListFilesResult:
        """Execute the workflow and return per-symbol file results.

        Opens a single shared HTTP session and issues one concurrent
        :meth:`ArchiveClient.list_symbol_files` call per requested symbol.
        Per-symbol exceptions are captured into
        :attr:`SymbolListFilesResult.error` rather than raised, so the
        returned :class:`ListFilesResult` always covers every requested
        symbol in caller-provided input order.

        Returns:
            Aggregate result whose ``per_symbol`` list preserves the
            caller-provided symbol order.
        """
        logger.info(
            "listing {} symbols for trade_type={} data_freq={} data_type={} interval={}",
            len(self.symbols),
            self.trade_type.value,
            self.data_freq.value,
            self.data_type.value,
            self.interval,
        )

        async with self._create_session() as session:
            tasks = [
                self.client.list_symbol_files(
                    self.trade_type,
                    self.data_freq,
                    self.data_type,
                    symbol,
                    self.interval,
                    session=session,
                )
                for symbol in self.symbols
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        per_symbol: list[SymbolListFilesResult] = []
        for symbol, outcome in zip(self.symbols, results, strict=True):
            if isinstance(outcome, Exception):
                per_symbol.append(
                    SymbolListFilesResult(symbol=symbol, files=[], error=str(outcome))
                )
                continue

            per_symbol.append(SymbolListFilesResult(symbol=symbol, files=outcome))

        return ListFilesResult(per_symbol=per_symbol)


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

    def _local_path_for_key(self, key: str) -> Path:
        """Map a remote archive key to the local aws_data path."""
        return self.bhds_home / "aws_data" / Path(key)

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
                local_path = self._local_path_for_key(remote_file.key)
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

    def _verified_target_for_path(self, path: Path) -> Path | None:
        """Return the zip path whose verify markers should be invalidated."""
        if path.name.endswith(".CHECKSUM"):
            return path.with_name(path.name.removesuffix(".CHECKSUM"))
        if path.name.endswith(".zip"):
            return path
        return None

    def _invalidate_verified_markers(self, entries: Sequence[DiffEntry]) -> None:
        """Delete stale verify markers for updated zip or checksum files."""
        targets = {
            target
            for entry in entries
            if entry.reason == "updated"
            for target in [self._verified_target_for_path(entry.local_path)]
            if target is not None
        }

        for zip_path in targets:
            legacy_marker = zip_path.parent / f"{zip_path.name}.verified"
            legacy_marker.unlink(missing_ok=True)
            for marker_path in zip_path.parent.glob(f"{zip_path.name}.*.verified"):
                marker_path.unlink(missing_ok=True)

    def _print_scan_summary(self, diff_result: DiffResult) -> None:
        """Print the pre-download scan summary to stderr."""
        print(
            f"Scanning remote: {len(self.symbols)} symbols, {diff_result.total_remote} files",
            file=sys.stderr,
        )
        print(
            f"Scanning local: {diff_result.skipped} up to date, "
            f"{len(diff_result.to_download)} to download",
            file=sys.stderr,
        )

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
            leave=False,
        )

        def callback(event: BatchProgressEvent) -> None:
            if event.phase == "start":
                if event.attempt == 1:
                    print(
                        f"Downloading batch {event.batch_index}/{event.total_batches} "
                        f"({event.requested} files)...",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"Retrying batch {event.batch_index}/{event.total_batches} "
                        f"({event.requested} files), attempt {event.attempt}/{event.max_tries}...",
                        file=sys.stderr,
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
        diff_result = self._build_diff_result(list_result)

        if self.dry_run:
            return diff_result

        self._print_scan_summary(diff_result)

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
