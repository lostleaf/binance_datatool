from pathlib import PurePosixPath
from typing import Union

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


def create_path_builder(
    trade_type: TradeType,
    data_freq: DataFrequency,
    data_type: DataType,
    time_interval: str | None = None,
) -> AwsPathBuilder:
    """Create an AWS path builder for Binance market data.

    Returns an `AwsKlinePathBuilder` when `data_type == DataType.kline`, and an `AwsPathBuilder` otherwise. 
    For kline data, `time_interval` is required (e.g. "1m", "5m", "1h").

    Args:
      trade_type: Market type, e.g. `TradeType.spot`, `TradeType.um_futures`, `TradeType.cm_futures`.
      data_freq: Data frequency such as `DataFrequency.daily` or `DataFrequency.monthly`.
      data_type: Data category, e.g. `DataType.kline`, `DataType.funding_rate`, `DataType.agg_trades`.
      time_interval: Candle interval used only for kline data. Required when `data_type == DataType.kline`.

    Returns:
      `AwsKlinePathBuilder` for klines, otherwise `AwsPathBuilder`.

    Raises:
      ValueError: If `data_type == DataType.kline` and `time_interval` is not provided.

    Examples:
      >>> create_path_builder(TradeType.spot, DataFrequency.daily, DataType.kline, "1m")
      AwsKlinePathBuilder(...)
      >>> create_path_builder(TradeType.um_futures, DataFrequency.monthly, DataType.funding_rate)
      AwsPathBuilder(...)
    """

    if data_type == DataType.kline:
        if time_interval is None:
            raise ValueError("time_interval is required for kline data type")
        return AwsKlinePathBuilder(trade_type=trade_type, data_freq=data_freq, time_interval=time_interval)
    else:
        return AwsPathBuilder(trade_type=trade_type, data_freq=data_freq, data_type=data_type)
