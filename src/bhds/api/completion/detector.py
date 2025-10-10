"""Data missing detectors

Provides independent detector classes for identifying missing data without BinanceFetcher dependency.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import logger
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder, AwsPathBuilder

from .task import CompletionOperation, CompletionTask


class BaseDetector(ABC):
    """Abstract base class for data missing detectors"""

    def __init__(self, trade_type: TradeType, base_dir: Path):
        """Initialize base detector with common parameters

        Args:
            trade_type: Trade type (spot/um_futures/cm_futures)
            base_dir: Base directory for parsed data
        """
        self.trade_type = trade_type
        self.base_dir = Path(base_dir)
        self.path_builder = self._create_path_builder()
        self.local_client = LocalAwsClient(base_dir=self.base_dir, path_builder=self.path_builder)

    @abstractmethod
    def _create_path_builder(self) -> AwsPathBuilder:
        """Create appropriate path builder for the detector type

        Returns:
            Path builder instance
        """
        pass

    @abstractmethod
    def detect(self, symbols: list[str]) -> list[CompletionTask]:
        """Detect missing data for given symbols

        Args:
            symbols: List of trading symbols

        Returns:
            List[CompletionTask]: Tasks describing missing data fetch operations
        """
        pass


class DailyKlineDetector(BaseDetector):
    """Daily kline data missing detector"""

    def __init__(self, trade_type: TradeType, interval: str, base_dir: Path):
        """Initialize with path-related parameters only, no BinanceFetcher needed

        Args:
            trade_type: Trade type (spot/um_futures/cm_futures)
            interval: Time interval for kline data (1m, 5m, 1h, etc.)
            base_dir: Base directory for parsed data
        """
        self.interval = interval
        super().__init__(trade_type, base_dir)

    def _create_path_builder(self) -> AwsKlinePathBuilder:
        """Create kline path builder"""
        return AwsKlinePathBuilder(
            trade_type=self.trade_type, data_freq=DataFrequency.daily, time_interval=self.interval
        )

    def detect(self, symbols: list[str]) -> list[CompletionTask]:
        """Detect missing daily kline data

        Args:
            symbols: List of trading symbols

        Returns:
            List[CompletionTask]: Tasks describing missing data fetch operations
        """
        missing_tasks: list[CompletionTask] = []

        for symbol in symbols:
            try:
                missing_dates = self._get_missing_dates(symbol)

                for date_str in missing_dates:
                    # Build save path
                    symbol_dir = self.local_client.get_symbol_dir(symbol)
                    filename = f"{symbol}-{self.interval}-{date_str}.parquet"
                    save_path = symbol_dir / filename

                    # Create completion task describing the fetch operation
                    missing_tasks.append(
                        CompletionTask(
                            operation=CompletionOperation.GET_KLINE_DF_OF_DAY,
                            params={"symbol": symbol, "interval": self.interval, "dt": date_str},
                            save_path=save_path,
                        )
                    )

            except Exception as e:
                logger.exception(f"Failed to detect missing dates for {symbol}: {e}")
                continue

        return missing_tasks

    def _get_missing_dates(self, symbol: str) -> list[str]:
        """Get missing date list for specified symbol

        Infer date range from existing data files and find missing dates
        """
        symbol_dir = self.local_client.get_symbol_dir(symbol)

        if not symbol_dir.exists():
            return []

        # Collect existing dates
        existing_dates = set()
        for file_path in symbol_dir.glob("*.parquet"):
            parts = file_path.stem.split("-")
            if len(parts) >= 3:
                try:
                    date_str = "-".join(parts[-3:])
                    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                    existing_dates.add(dt)
                except ValueError:
                    continue

        if not existing_dates:
            return []

        # Calculate date range
        min_date = min(existing_dates)
        max_date = max(existing_dates)

        if max_date < min_date:
            return []

        # Generate expected date range
        expected_dates = set()
        current_date = min_date
        while current_date <= max_date:
            expected_dates.add(current_date)
            current_date += timedelta(days=1)

        # Find missing dates
        missing_dates = expected_dates - existing_dates

        return sorted([dt.strftime("%Y-%m-%d") for dt in missing_dates])


class FundingRateDetector(BaseDetector):
    """Funding rate data missing detector"""

    def __init__(self, trade_type: TradeType, base_dir: Path):
        """Initialize with path-related parameters only, no BinanceFetcher needed

        Args:
            trade_type: Trade type (spot/um_futures/cm_futures)
            base_dir: Base directory for parsed data
        """
        super().__init__(trade_type, base_dir)

    def _create_path_builder(self) -> AwsPathBuilder:
        """Create funding rate path builder"""
        return AwsPathBuilder(
            trade_type=self.trade_type, data_freq=DataFrequency.monthly, data_type=DataType.funding_rate
        )

    def detect(self, symbols: list[str]) -> list[CompletionTask]:
        """Detect funding rate data that needs updating

        Args:
            symbols: List of trading symbols

        Returns:
            List[CompletionTask]: Tasks describing missing data fetch operations
        """
        limit = 1000
        missing_tasks: list[CompletionTask] = []

        for symbol in symbols:
            try:
                # Build save path
                symbol_dir = self.local_client.get_symbol_dir(symbol)
                save_path = symbol_dir / "latest.parquet"

                # Create completion task describing the fetch operation
                missing_tasks.append(
                    CompletionTask(
                        operation=CompletionOperation.GET_HIST_FUNDING_RATE,
                        params={"symbol": symbol, "limit": limit},
                        save_path=save_path,
                    )
                )

            except Exception as e:
                logger.exception(f"Failed to create funding task for {symbol}: {e}")
                continue

        logger.info(f"Detected {len(missing_tasks)} funding rate tasks")
        return missing_tasks


def create_detector(
    data_type: DataType, trade_type: TradeType, base_dir: str | Path, interval: str | None = None
) -> BaseDetector:
    """
    Factory method to create appropriate detector based on data type.

    Args:
        data_type: Market data type (kline/funding_rate)
        trade_type: Trade type (spot/um_futures/cm_futures)
        base_dir: Base directory for parsed data
        interval: Required for kline data type (1m, 5m, 1h, etc.)

    Returns:
        Appropriate detector instance

    Raises:
        ValueError: If required parameters are missing for specific data types
    """
    base_dir = Path(base_dir)
    match data_type:
        case DataType.kline:
            if interval is None:
                raise ValueError("interval is required for kline detector")
            return DailyKlineDetector(trade_type=trade_type, interval=interval, base_dir=base_dir)
        case DataType.funding_rate:
            return FundingRateDetector(trade_type=trade_type, base_dir=base_dir)
        case _:
            raise ValueError(f"No detector available for data type: {data_type}")
