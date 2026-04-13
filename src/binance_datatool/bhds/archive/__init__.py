"""Archive access helpers."""

from binance_datatool.bhds.archive.client import ArchiveClient, ArchiveFile, list_symbols
from binance_datatool.bhds.archive.downloader import (
    Aria2DownloadResult,
    Aria2NotFoundError,
    BatchProgressEvent,
    DownloadRequest,
    download_archive_files,
)
from binance_datatool.bhds.archive.filter import (
    CmSymbolFilter,
    SpotSymbolFilter,
    SymbolFilter,
    UmSymbolFilter,
    build_symbol_filter,
)

__all__ = [
    "ArchiveClient",
    "ArchiveFile",
    "Aria2DownloadResult",
    "Aria2NotFoundError",
    "BatchProgressEvent",
    "CmSymbolFilter",
    "DownloadRequest",
    "SpotSymbolFilter",
    "SymbolFilter",
    "UmSymbolFilter",
    "build_symbol_filter",
    "download_archive_files",
    "list_symbols",
]
