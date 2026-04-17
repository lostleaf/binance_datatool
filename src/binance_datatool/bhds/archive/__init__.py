"""Archive access helpers."""

from binance_datatool.bhds.archive.checksum import (
    VerifyFileResult,
    calc_sha256,
    read_expected_checksum,
    verify_single_file,
)
from binance_datatool.bhds.archive.client import (
    ArchiveClient,
    ArchiveFile,
    SymbolListingResult,
    list_symbols,
)
from binance_datatool.bhds.archive.downloader import (
    Aria2DownloadResult,
    Aria2NotFoundError,
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
from binance_datatool.bhds.archive.markers import (
    clear_markers,
    is_marker_valid,
    max_source_mtime,
    symbol_dir,
    write_marker,
)

__all__ = [
    "ArchiveClient",
    "ArchiveFile",
    "Aria2DownloadResult",
    "Aria2NotFoundError",
    "CmSymbolFilter",
    "DownloadRequest",
    "SpotSymbolFilter",
    "SymbolListingResult",
    "SymbolFilter",
    "UmSymbolFilter",
    "VerifyFileResult",
    "build_symbol_filter",
    "calc_sha256",
    "download_archive_files",
    "list_symbols",
    "clear_markers",
    "is_marker_valid",
    "max_source_mtime",
    "read_expected_checksum",
    "symbol_dir",
    "verify_single_file",
    "write_marker",
]
