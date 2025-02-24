import typer
from typing_extensions import Annotated

from config import TradeType
from generate.kline import merge_and_split_gaps, merge_and_split_gaps_type_all
from generate.resample import resampled_kline
from util.log_kit import divider

app = typer.Typer()


@app.command()
def merged_split_kline(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    time_interval: Annotated[str, typer.Argument(help="K-line time interval, e.g., '1m', '5m', '1h'")],
    symbol: Annotated[str, typer.Argument(help="Trading pair symbol, e.g., 'BTCUSDT'")],
    split_gaps: Annotated[bool, typer.Option(help="Whether to split data by gaps")] = True,
    min_days: Annotated[int, typer.Option(help="Minimum gap days threshold")] = 1,
    min_price_chg: Annotated[float, typer.Option(help="Minimum price change ratio threshold")] = 0.1,
    with_vwap: Annotated[bool, typer.Option(help="Whether to calculate VWAP")] = True,
):
    """
    Merge AWS and API kline data for a single symbol.

    Scan for gaps and split data by gaps in kline data where if split_gaps is True.

    Add avg_price_{time_interval} column if with_vwap is True.

    Gaps are defined as:
    1. gap1: time gap > min_days AND absolute price change > min_price_chg;
    2. gap2: time gap > min_days*2 regardless of price change.
    """
    divider(f"Generate merged and split kline data for {symbol} {trade_type.value} {time_interval}")
    merge_and_split_gaps(
        trade_type=trade_type,
        time_interval=time_interval,
        symbol=symbol,
        split_gaps=split_gaps,
        min_days=min_days,
        min_price_chg=min_price_chg,
        with_vwap=with_vwap,
    )


@app.command()
def merged_split_kline_type_all(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    time_interval: Annotated[str, typer.Argument(help="K-line time interval, e.g., '1m', '5m', '1h'")],
    split_gaps: Annotated[bool, typer.Option(help="Whether to split data by gaps")] = True,
    min_days: Annotated[int, typer.Option(help="Minimum gap days threshold")] = 1,
    min_price_chg: Annotated[float, typer.Option(help="Minimum price change ratio threshold")] = 0.1,
    with_vwap: Annotated[bool, typer.Option(help="Whether to calculate VWAP")] = True,
):
    """
    Merge AWS and API kline data for all symbols of given trade type and time interval.
    """
    merge_and_split_gaps_type_all(
        trade_type=trade_type,
        time_interval=time_interval,
        split_gaps=split_gaps,
        min_days=min_days,
        min_price_chg=min_price_chg,
        with_vwap=with_vwap,
    )


@app.command()
def resample_kline(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    symbol: Annotated[str, typer.Argument(help="Trading pair symbol, e.g., 'BTCUSDT'")],
    resample_interval: Annotated[str, typer.Argument(help="Resample interval, e.g., '1h', '4h'")],
    base_offset: Annotated[str, typer.Argument(help="Base offset, e.g., '5m', '15m', '30m'")],
):
    """
    Resample kline data for a single symbol.
    """
    divider(f"Resample kline {trade_type.value} {resample_interval} {symbol} base offset {base_offset}")
    resampled_kline(trade_type=trade_type, symbol=symbol, resample_interval=resample_interval, base_offset=base_offset)
