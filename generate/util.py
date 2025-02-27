from config import BINANCE_DATA_DIR
from config.config import TradeType


def list_results_kline_symbols(trade_type: TradeType, time_interval: str):
    results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "klines" / time_interval
    symbols = sorted(p.stem for p in results_dir.glob("*.pqt"))
    return symbols
