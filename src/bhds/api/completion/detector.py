"""Data missing detectors

Provides independent detector classes for identifying missing data without BinanceFetcher dependency.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Tuple

from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import logger
from bdt_common.rest_api.fetcher import BinanceFetcher
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder, AwsPathBuilder


class DailyKlineDetector:
    """Daily kline data missing detector"""
    
    def __init__(self, trade_type: TradeType, interval: str, base_dir: str):
        """Initialize with path-related parameters only, no BinanceFetcher needed"""
        self.trade_type = trade_type
        self.interval = interval
        self.path_builder = AwsKlinePathBuilder(
            trade_type=trade_type,
            data_freq=DataFrequency.daily,
            time_interval=interval
        )
        self.local_client = LocalAwsClient(
            base_dir=base_dir,
            path_builder=self.path_builder
        )
    
    def detect(self, symbols: List[str]) -> List[Tuple[Callable, Dict, Path]]:
        """Detect missing daily kline data
        
        Args:
            symbols: List of trading symbols
            
        Returns:
            List[Tuple]: (unbound method, parameter dict, save path)
        """
        missing_tasks = []
        
        for symbol in symbols:
            try:
                missing_dates = self._get_missing_dates(symbol)
                
                for date_str in missing_dates:
                    # Build save path
                    symbol_dir = self.local_client.get_symbol_dir(symbol)
                    filename = f"{symbol}-{self.interval}-{date_str}.parquet"
                    save_path = symbol_dir / filename
                    
                    # Create task tuple with unbound method
                    task = (
                        BinanceFetcher.get_kline_df_of_day,
                        {"symbol": symbol, "interval": self.interval, "dt": date_str},
                        save_path
                    )
                    missing_tasks.append(task)
                    
            except Exception as e:
                logger.exception(f"Failed to detect missing dates for {symbol}: {e}")
                continue
        
        logger.info(f"Detected {len(missing_tasks)} missing kline tasks")
        return missing_tasks
    
    def _get_missing_dates(self, symbol: str) -> List[str]:
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


class FundingRateDetector:
    """Funding rate data missing detector"""
    
    def __init__(
        self,
        trade_type: TradeType,
        base_dir: str,
        contract_type=None  # Keep for compatibility
    ):
        """Initialize with path-related parameters only, no BinanceFetcher needed"""
        self.trade_type = trade_type
        self.path_builder = AwsPathBuilder(
            trade_type=trade_type,
            data_freq=DataFrequency.monthly,
            data_type=DataType.funding_rate
        )
        self.local_client = LocalAwsClient(
            base_dir=base_dir,
            path_builder=self.path_builder
        )
    
    def detect(self, symbols: List[str], limit: int = 1000) -> List[Tuple[Callable, Dict, Path]]:
        """Detect funding rate data that needs updating
        
        Args:
            symbols: List of trading symbols
            limit: Number of records to fetch
            
        Returns:
            List[Tuple]: (unbound method, parameter dict, save path)
        """
        missing_tasks = []
        
        for symbol in symbols:
            try:
                # Build save path
                symbol_dir = self.local_client.get_symbol_dir(symbol)
                save_path = symbol_dir / "latest.parquet"
                
                # Create task tuple with unbound method
                task = (
                    BinanceFetcher.get_hist_funding_rate,
                    {"symbol": symbol, "limit": limit},
                    save_path
                )
                missing_tasks.append(task)
                
            except Exception as e:
                logger.exception(f"Failed to create funding task for {symbol}: {e}")
                continue
        
        logger.info(f"Detected {len(missing_tasks)} funding rate tasks")
        return missing_tasks