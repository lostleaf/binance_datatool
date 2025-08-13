"""Kline data completion strategy implementation

Intelligent completion strategy for daily kline data.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Tuple

from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import logger
from bdt_common.rest_api.fetcher import BinanceFetcher
from bhds.api.completion.base import BaseCompletion
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder


class DailyKlineCompletion(BaseCompletion):
    """
    Daily kline data completion strategy

    Completes missing date kline data using get_kline_df_of_day.
    """

    def __init__(self, trade_type: TradeType, interval: str, base_dir: str, fetcher: BinanceFetcher):
        self.path_builder = AwsKlinePathBuilder(
            trade_type=trade_type, data_freq=DataFrequency.daily, time_interval=interval
        )

        # Create local AWS client
        local_client = LocalAwsClient(base_dir=base_dir, path_builder=self.path_builder)

        super().__init__(
            trade_type=trade_type,
            data_type=DataType.kline,
            data_freq=DataFrequency.daily,
            local_client=local_client,
            fetcher=fetcher,
            interval=interval,
        )

    def get_missings(self, symbols: List[str]) -> List[Tuple[Callable[..., Awaitable], Dict, Path]]:
        """Get missing kline data information for symbols

        Args:
            symbols: Trading symbol list

        Returns:
            List of tuples containing:
            - Fetcher async function to call
            - Keyword arguments dict for the function
            - File path where to save the result
        """
        missing_tasks = []

        for symbol in symbols:
            try:
                # Get list of missing dates for this symbol
                missing_dates = self._get_missing_dates(symbol)

                if not missing_dates:
                    continue

                # Create tasks for each missing date
                for date_str in missing_dates:
                    # Build file path
                    symbol_dir = self.local_client.get_symbol_dir(symbol)
                    filename = f"{symbol}-{self.interval}-{date_str}.parquet"
                    file_path = symbol_dir / filename

                    # Create task tuple
                    fetch_func = self.fetcher.get_kline_df_of_day
                    kwargs_dict = {"symbol": symbol, "interval": self.interval, "dt": date_str}

                    missing_tasks.append((fetch_func, kwargs_dict, file_path))

            except Exception as e:
                logger.exception(f"Failed to get missing dates for symbol {symbol}: {e}")
                continue

        return missing_tasks

    def _get_missing_dates(self, symbol: str) -> List[str]:
        """Get list of missing dates"""
        # Get symbol data directory
        symbol_dir = self.local_client.get_symbol_dir(symbol)

        if not symbol_dir.exists():
            # If directory doesn't exist, return empty list
            return []

        # Get existing date files
        existing_dates = set()
        for file_path in symbol_dir.glob("*.parquet"):
            # Extract date from filename (format: SYMBOL-INTERVAL-YYYY-MM-DD.parquet)
            parts = file_path.stem.split("-")
            if len(parts) >= 3:
                try:
                    date_str = "-".join(parts[-3:])
                    dt = datetime.strptime(date_str, "%Y-%m-%d").date()  # Convert to date object
                    existing_dates.add(dt)
                except ValueError:
                    continue

        # If no existing data, return empty list
        if not existing_dates:
            return []

        # Find date range from existing data
        min_date = min(existing_dates)
        max_date = max(existing_dates)

        # If max_date is before min_date, return empty list
        if max_date < min_date:
            return []

        # Generate expected date range between min and max
        expected_dates = set()
        current_date = min_date
        while current_date <= max_date:
            expected_dates.add(current_date)
            current_date += timedelta(days=1)

        # Find missing dates
        missing_dates = expected_dates - existing_dates

        # Convert back to string format and sort
        return sorted([dt.strftime("%Y-%m-%d") for dt in missing_dates])
