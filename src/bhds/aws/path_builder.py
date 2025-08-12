from pathlib import PurePosixPath
from bdt_common.enums import DataFrequency, DataType, TradeType


class AwsPathBuilder:
    """
    AWS path builder for constructing Binance AWS data directory paths.
    """
    
    def __init__(self, trade_type: TradeType, data_freq: DataFrequency, data_type: DataType):
        """
        Initialize AWS path builder.
        
        Args:
            trade_type: Trade type (spot/futures)
            data_freq: Data frequency (daily/monthly)
            data_type: Market data type (kline/funding/liquidation)
        """
        self.trade_type = trade_type
        self.data_freq = data_freq
        self.data_type = data_type
        self.base_dir = PurePosixPath("data") / trade_type / data_freq / data_type

    def get_symbol_dir(self, symbol: str) -> PurePosixPath:
        """
        Get the directory path for the specified trading pair.
        
        Args:
            symbol: Trading pair symbol (e.g.: BTCUSDT)
            
        Returns:
            PurePosixPath object representing the trading pair directory path
        """
        return self.base_dir / symbol


class AwsKlinePathBuilder(AwsPathBuilder):
    """
    AWS Kline data path builder.
    
    Extends AwsPathBuilder to provide Kline-specific functionality, including time intervals (1m, 5m, 1h, 1d, etc.).
    """
    
    def __init__(self, trade_type: TradeType, data_freq: DataFrequency, time_interval: str):
        """
        Initialize AWS Kline path builder.
        
        Args:
            trade_type: Trade type (spot/futures)
            data_freq: Data frequency (daily/monthly)
            time_interval: Kline time interval (1m, 5m, 1h, 1d, etc.)
        """
        super().__init__(trade_type, data_freq, DataType.kline)
        self.time_interval = time_interval

    def get_symbol_dir(self, symbol: str) -> PurePosixPath:
        """
        Get the directory path for the specified trading pair and time interval.
        
        Args:
            symbol: Trading pair symbol (e.g.: BTCUSDT)
            
        Returns:
            PurePosixPath object representing the trading pair directory path with time interval subdirectory
        """
        return self.base_dir / symbol / self.time_interval