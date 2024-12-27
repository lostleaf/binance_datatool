from aws.client_async import AwsKlineClient
from config import DataFreq, TradeType


def local_list_kline_symbols(trade_type: TradeType, time_interval: str):
    kline_dir = AwsKlineClient.LOCAL_DIR / AwsKlineClient.get_base_dir(trade_type=trade_type, data_freq=DataFreq.daily)
    symbols = sorted(p.parts[-2] for p in kline_dir.glob(f'*/{time_interval}'))
    return symbols
