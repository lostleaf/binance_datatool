import asyncio

from .aws_candle import (convert_aws_candle_csv, get_aws_all_coin_perpetual, get_aws_all_usdt_perpetual,
                         get_aws_all_usdt_spot, get_aws_candle, verify_aws_candle, download_aws_missing_from_api)
from .aws_trades import get_aws_aggtrades, verify_aws_aggtrades
from .compare import compare_aws_quantclass_candle
from .exchange_info import update_exchange_info
from .fix_data import check_gaps, fix_candle
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

    def get_aws_all_coin_perpetual(self, *time_intervals):
        """
        Downloads all coin perpetual daily candlestick data from Binance's AWS data center.
        """
        for time_interval in time_intervals:
            asyncio.run(get_aws_all_coin_perpetual(time_interval))

    def get_aws_all_usdt_perpetual(self, *time_intervals):
        """
        Downloads all USDT perpetual daily candlestick data from Binance's AWS data center.
        """
        for time_interval in time_intervals:
            asyncio.run(get_aws_all_usdt_perpetual(time_interval))

    def get_aws_all_usdt_spot(self, *time_intervals):
        """
        Downloads all spot USDT pairs daily candlestick data from Binance's AWS data center.
        Leveraged coins and stablecoins are excluded.
        """
        for time_interval in time_intervals:
            asyncio.run(get_aws_all_usdt_spot(time_interval))

    def get_aws_all(self, *time_intervals):
        """
        Downloads USDT spot, USDT perpetual and coin perpetual from AWS data center.
        """
        self.get_aws_all_usdt_spot(*time_intervals)
        self.get_aws_all_usdt_perpetual(*time_intervals)
        self.get_aws_all_coin_perpetual(*time_intervals)

    def get_aws_aggtrades(self, typ, *symbols, recent=30):
        """
        Downloads daily aggtrades data from Binance's AWS data center. Only recent dates will be downloaded.
        """
        asyncio.run(get_aws_aggtrades(typ, recent, symbols))

    def verify_aws_candle(self, typ, *time_intervals):
        """
        Verifies the integrity of all AWS candlestick data and deletes incorrect data.
        """
        for time_interval in time_intervals:
            verify_aws_candle(typ, time_interval)

    def verify_aws_aggtrades(self, typ):
        """
        Verifies the integrity of all AWS aggtrades data and deletes incorrect data.
        """
        verify_aws_aggtrades(typ)

    def convert_aws_candle_csv(self, typ, *time_intervals):
        """
        Converts and merges downloaded candlestick data into Pandas Parquet format.
        """
        for time_interval in time_intervals:
            convert_aws_candle_csv(typ, time_interval)

    def convert_quantclass_candle_csv(self, typ, time_interval):
        """
        Converts quantclass candlestick data into Pandas Parquet format.
        """
        convert_quantclass_candle_csv(typ, time_interval)

    def compare_aws_quantclass_candle(self, typ, time_interval, *symbols):
        """
        Compare AWS candle with Quantclass
        """
        for symbol in symbols:
            compare_aws_quantclass_candle(typ, time_interval, symbol)

    def check_gaps(self, source, typ, time_interval, hours_threshold=48):
        """
        Check and print gaps over hours_threshold
        """
        check_gaps(source, typ, time_interval, hours_threshold)

    def fix_candle(self, source, typ, time_interval):
        """
        Split and fill gaps for candlestick data
        """
        fix_candle(source, typ, time_interval)

    def update_exchange_info(self, typ):
        """
        Get exchange info from Binance api and update local json configs
        """
        asyncio.run(update_exchange_info(typ))

    def download_aws_missing_candle(self, typ, *time_intervals):
        """
        Download aws missing candlestick data from api
        """
        for time_interval in time_intervals:
            asyncio.run(download_aws_missing_from_api(typ, time_interval))
