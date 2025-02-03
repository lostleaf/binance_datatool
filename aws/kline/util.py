from config import DataFrequency, TradeType


def local_list_kline_symbols(trade_type: TradeType, time_interval: str):
    from aws.client_async import AwsKlineClient
    kline_dir = AwsKlineClient.LOCAL_DIR / AwsKlineClient.get_base_dir(trade_type=trade_type, data_freq=DataFrequency.daily)
    symbols = sorted(p.parts[-2] for p in kline_dir.glob(f'*/{time_interval}'))
    return symbols


def split_into_batches(arr: list, batch_size: int):
    return [arr[i:i + batch_size] for i in range(0, len(arr), batch_size)]
