"""Archive workflows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from binance_datatool.bhds.archive import ArchiveClient
from binance_datatool.common import TradeType, infer_cm_info, infer_spot_info, infer_um_info

if TYPE_CHECKING:
    from binance_datatool.bhds.archive import SymbolFilter
    from binance_datatool.common import DataFrequency, DataType, SymbolInfo


@dataclass(slots=True)
class ListSymbolsResult:
    """Structured result for archive symbol listing."""

    matched: list[SymbolInfo]
    unmatched: list[str]
    filtered_out: list[SymbolInfo]


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

    Thin orchestration layer that delegates to :class:`ArchiveClient`.
    Future phases may add filtering, configuration loading, or caching.
    """

    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        symbol_filter: SymbolFilter | None = None,
        client: ArchiveClient | None = None,
    ) -> None:
        """Initialise the workflow.

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
        raw_symbols = await self.client.list_symbols(self.trade_type, self.data_freq, self.data_type)

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
