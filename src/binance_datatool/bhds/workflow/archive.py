"""Archive workflows."""

from __future__ import annotations

from typing import TYPE_CHECKING

from binance_datatool.bhds.archive import ArchiveClient

if TYPE_CHECKING:
    from binance_datatool.common import DataFrequency, DataType, TradeType


class ArchiveListSymbolsWorkflow:
    """Workflow for listing available symbols from the archive."""

    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        client: ArchiveClient | None = None,
    ) -> None:
        self.trade_type = trade_type
        self.data_freq = data_freq
        self.data_type = data_type
        self.client = client or ArchiveClient()

    async def run(self) -> list[str]:
        """Execute the workflow."""
        return await self.client.list_symbols(self.trade_type, self.data_freq, self.data_type)
