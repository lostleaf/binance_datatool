from aws.client_async import AwsFundingRateClient
from config import DataFrequency, TradeType


def local_list_funding_symbols(trade_type: TradeType):
    """List all locally available funding rate symbols."""
    funding_dir = AwsFundingRateClient.LOCAL_DIR / AwsFundingRateClient.get_base_dir(
        trade_type=trade_type, data_freq=DataFrequency.monthly
    )
    symbols = sorted(p.name for p in funding_dir.glob("*") if p.is_dir())
    return symbols