"""Archive access helpers."""

from binance_datatool.archive.checksum import (
    VerifyFileResult,
    calc_sha256,
    read_expected_checksum,
    verify_single_file,
)
from binance_datatool.archive.client import (
    ArchiveClient,
    ArchiveFile,
    SymbolListingResult,
    list_symbols,
)
from binance_datatool.archive.downloader import (
    Aria2DownloadResult,
    Aria2NotFoundError,
    DownloadRequest,
    download_archive_files,
)
from binance_datatool.archive.filter import (
    CmSymbolFilter,
    SpotSymbolFilter,
    SymbolFilter,
    UmSymbolFilter,
    build_symbol_filter,
)
from binance_datatool.archive.symbol_dir import (
    SymbolArchiveDir,
    create_symbol_archive_dir,
)

__all__ = [
    "ArchiveClient",
    "ArchiveFile",
    "Aria2DownloadResult",
    "Aria2NotFoundError",
    "CmSymbolFilter",
    "DownloadRequest",
    "SpotSymbolFilter",
    "SymbolArchiveDir",
    "SymbolListingResult",
    "SymbolFilter",
    "UmSymbolFilter",
    "VerifyFileResult",
    "build_symbol_filter",
    "calc_sha256",
    "create_symbol_archive_dir",
    "download_archive_files",
    "list_symbols",
    "read_expected_checksum",
    "verify_single_file",
]
