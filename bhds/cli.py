import asyncio

from .aws_candle import convert_aws_candle_csv, get_aws_candle, verify_candle


class Bhds:
    """
    Binance Historical Data Service
    """

    def get_aws_candle(self, typ, symbol, time_interval):
        """
        Download candlestick data from Binance's AWS data center
        """
        asyncio.run(get_aws_candle(typ, symbol, time_interval))

    def verify_candle(self, typ, symbol, time_interval):
        """
        Verify the candlestick data integrity and delete wrong data
        """
        verify_candle(typ, symbol, time_interval)

    def convert_aws_candle_csv(self, typ, time_interval):
        """
        Convert and merge downloaded candlestick data to Pandas Feather
        """
        convert_aws_candle_csv(typ, time_interval)
