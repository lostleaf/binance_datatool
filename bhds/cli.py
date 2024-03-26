import asyncio

from .aws_candle import convert_aws_candle_csv, get_aws_candle, verify_all_candle


class Bhds:
    """
    Binance Historical Data Service

    Supported types: spot, usdt_futures, coin_futures
    """

    def get_aws_candle(self, typ, time_interval, *symbols):
        """
        Download candlestick data from Binance's AWS data center
        """
        for symbol in symbols:
            asyncio.run(get_aws_candle(typ, symbol, time_interval))

    def verify_all_candle(self, typ, time_interval):
        """
        Verify all candlestick data integrity and delete wrong data
        """
        verify_all_candle(typ, time_interval)

    def convert_aws_candle_csv(self, typ, time_interval):
        """
        Convert and merge downloaded candlestick data to Pandas Feather
        """
        convert_aws_candle_csv(typ, time_interval)
