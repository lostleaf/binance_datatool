import asyncio

from .aws_candle import (
    convert_aws_candle_csv,
    get_aws_all_coin_perpetual,
    get_aws_all_usdt_perpetual,
    get_aws_candle,
    verify_aws_candle,
    get_aws_all_usdt_spot,
)
from .aws_trades import get_aws_aggtrades
from .compare import compare_aws_quantclass_candle
from .quantclass_candle import convert_quantclass_candle_csv


class Bhds:
    """
    Binance Historical Data Service

    Supports types: spot, usdt_futures, coin_futures
    """

    def get_aws_candle(self, typ, time_interval, *symbols):
        """
        Downloads daily candlestick data from Binance's AWS data center. All available dates will be downloaded.
        """
        asyncio.run(get_aws_candle(typ, time_interval, symbols))

    def get_aws_all_coin_perpetual(self, time_interval):
        """
        Downloads all coin perpetual daily candlestick data from Binance's AWS data center.
        """
        asyncio.run(get_aws_all_coin_perpetual(time_interval))

    def get_aws_all_usdt_perpetual(self, time_interval):
        """
        Downloads all USDT perpetual daily candlestick data from Binance's AWS data center.
        """
        asyncio.run(get_aws_all_usdt_perpetual(time_interval))

    def get_aws_all_usdt_spot(self, time_interval):
        """
        Downloads all spot USDT pairs daily candlestick data from Binance's AWS data center.
        """
        asyncio.run(get_aws_all_usdt_spot(time_interval))

    def get_aws_aggtrades(self, typ, recent=30, *symbols):
        """
        Downloads daily aggtrades data from Binance's AWS data center. Only recent dates will be downloaded.
        """
        asyncio.run(get_aws_aggtrades(typ, recent, symbols))

    def verify_aws_candle(self, typ, time_interval, verify_num=False):
        """
        Verifies the integrity of all AWS candlestick data and deletes incorrect data.
        """
        verify_aws_candle(typ, time_interval, verify_num)

    def convert_aws_candle_csv(self, typ, time_interval):
        """
        Converts and merges downloaded candlestick data into Pandas Feather format.
        """
        convert_aws_candle_csv(typ, time_interval)

    def convert_quantclass_candle_csv(self, typ, time_interval, fill_gap=True):
        """
        Converts quantclass candlestick data into Pandas Parquet format.
        """
        convert_quantclass_candle_csv(typ, time_interval, fill_gap)

    def compare_aws_quantclass_candle(self, typ, time_interval, *symbols):
        """
        Compare AWS candle with Quantclass
        """
        for symbol in symbols:
            compare_aws_quantclass_candle(typ, time_interval, symbol)
