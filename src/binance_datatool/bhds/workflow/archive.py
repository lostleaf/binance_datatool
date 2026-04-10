"""Archive workflows."""

from __future__ import annotations

from typing import TYPE_CHECKING

from binance_datatool.bhds.archive import ArchiveClient

if TYPE_CHECKING:
    from binance_datatool.common import DataFrequency, DataType, TradeType


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
        client: ArchiveClient | None = None,
    ) -> None:
        """Initialise the workflow.

        Args:
            trade_type: Market segment to query.
            data_freq: Partition frequency.
            data_type: Dataset type.
            client: Optional pre-configured archive client.  A default
                :class:`ArchiveClient` is created when ``None``.
        """
        self.trade_type = trade_type
        self.data_freq = data_freq
        self.data_type = data_type
        self.client = client or ArchiveClient()

    async def run(self) -> list[str]:
        """Execute the workflow and return a sorted symbol list.

        Returns:
            Sorted list of symbol names from the archive.
        """
        return await self.client.list_symbols(self.trade_type, self.data_freq, self.data_type)
