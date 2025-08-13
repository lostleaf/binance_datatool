"""
Funding rate data completion strategy implementation
"""

from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Tuple

from bdt_common.enums import ContractType, DataFrequency, DataType, TradeType
from bdt_common.rest_api.fetcher import BinanceFetcher
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsPathBuilder

from .base import BaseCompletion


class RecentFundingCompletion(BaseCompletion):
    """Recent funding rate data completion strategy

    Completes recent 1000 funding rate records using get_hist_funding_rate.
    """

    def __init__(
        self,
        trade_type: TradeType,
        base_dir: str,
        fetcher: BinanceFetcher,
        contract_type: ContractType = ContractType.perpetual,
    ):
        self.contract_type = contract_type
        self.path_builder = AwsPathBuilder(
            trade_type=trade_type, data_freq=DataFrequency.monthly, data_type=DataType.funding_rate
        )

        # Create local AWS client
        local_client = LocalAwsClient(base_dir=base_dir, path_builder=self.path_builder)

        super().__init__(
            trade_type=trade_type,
            data_type=DataType.funding_rate,
            data_freq=DataFrequency.monthly,
            local_client=local_client,
            fetcher=fetcher,
        )

    def get_missings(self, symbols: List[str], limit: int = 1000) -> List[Tuple[Callable[..., Awaitable], Dict, Path]]:
        """Get missing funding rate data information for symbols

        Args:
            symbols: Trading symbol list
            limit: Record count limit to fetch

        Returns:
            List of tuples containing:
            - Fetcher async function to call
            - Keyword arguments dict for the function
            - File path where to save the result
        """
        tasks = []
        for symbol in symbols:
            # Get symbol directory and output file path
            symbol_dir = self.local_client.get_symbol_dir(symbol)
            output_file = symbol_dir / "latest.parquet"

            # Create task tuple: (fetch_function, kwargs, save_path)
            task = (self.fetcher.get_hist_funding_rate, {"symbol": symbol, "limit": limit}, output_file)
            tasks.append(task)

        return tasks
