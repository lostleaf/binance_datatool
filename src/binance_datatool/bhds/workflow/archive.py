"""Archive workflows."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiohttp
from loguru import logger

from binance_datatool.bhds.archive import ArchiveClient
from binance_datatool.common import TradeType, infer_cm_info, infer_spot_info, infer_um_info

if TYPE_CHECKING:
    from collections.abc import Sequence

    from binance_datatool.bhds.archive import ArchiveFile, SymbolFilter
    from binance_datatool.common import DataFrequency, DataType, SymbolInfo


@dataclass(slots=True)
class ListSymbolsResult:
    """Structured result for archive symbol listing."""

    matched: list[SymbolInfo]
    unmatched: list[str]
    filtered_out: list[SymbolInfo]


@dataclass(slots=True)
class SymbolListFilesResult:
    """Result for listing files under one symbol."""

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
    """Workflow for listing archive files under one or more symbol directories."""

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
        if isinstance(self.client, ArchiveClient):
            timeout = aiohttp.ClientTimeout(total=self.client.timeout_seconds)
            return aiohttp.ClientSession(timeout=timeout, trust_env=self.client.trust_env)

        return aiohttp.ClientSession()

    async def run(self) -> ListFilesResult:
        """Execute the workflow and return per-symbol file results."""
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
