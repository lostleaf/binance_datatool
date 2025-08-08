import typer
from typing_extensions import Annotated

from config import TradeType
from util.log_kit import divider

from .kline import gen_kline, gen_kline_type
from .resample_lazy import resample_kline_type, resample_kline

app = typer.Typer()


@app.command()
def kline(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    time_interval: Annotated[str, typer.Argument(help="K-line time interval, e.g., '1m', '5m', '1h'")],
    symbol: Annotated[str, typer.Argument(help="Trading pair symbol, e.g., 'BTCUSDT'")],
    split_gaps: Annotated[bool, typer.Option(help="Whether to split data by gaps")] = True,
    min_days: Annotated[int, typer.Option(help="Minimum gap days threshold")] = 1,
    min_price_chg: Annotated[float, typer.Option(help="Minimum price change ratio threshold")] = 0.1,
    with_vwap: Annotated[bool, typer.Option(help="Whether to calculate VWAP")] = True,
    with_funding_rates: Annotated[bool, typer.Option(help="Whether to include funding rates")] = True,
):
    """
    Merge AWS and API kline data for a single symbol.

    Add avg_price_{time_interval} column if with_vwap is True.

    Add funding_rate for perpetual futures if with_funding_rates is True.

    Scan for gaps and split data by gaps in kline data where if split_gaps is True.

    Gaps are defined as:
    1. gap1: time gap > min_days AND absolute price change > min_price_chg;
    2. gap2: time gap > min_days*2 regardless of price change.
    """
    divider(f"Generate merged and split kline data for {symbol} {trade_type.value} {time_interval}")
    gen_kline(
        trade_type=trade_type,
        time_interval=time_interval,
        symbol=symbol,
        split_gaps=split_gaps,
        min_days=min_days,
        min_price_chg=min_price_chg,
        with_vwap=with_vwap,
        with_funding=with_funding_rates,
    )


@app.command()
def kline_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    time_interval: Annotated[str, typer.Argument(help="K-line time interval, e.g., '1m', '5m', '1h'")],
    split_gaps: Annotated[bool, typer.Option(help="Whether to split data by gaps")] = True,
    min_days: Annotated[int, typer.Option(help="Minimum gap days threshold")] = 1,
    min_price_chg: Annotated[float, typer.Option(help="Minimum price change ratio threshold")] = 0.1,
    with_vwap: Annotated[bool, typer.Option(help="Whether to calculate VWAP")] = True,
    with_funding_rates: Annotated[bool, typer.Option(help="Whether to include funding rates")] = True,
):
    """
    Merge AWS and API kline data for all symbols of given trade type and time interval.

    Refer to kline command for more details.
    """
    gen_kline_type(
        trade_type=trade_type,
        time_interval=time_interval,
        split_gaps=split_gaps,
        min_days=min_days,
        min_price_chg=min_price_chg,
        with_vwap=with_vwap,
        with_funding_rates=with_funding_rates,
    )


@app.command()
def resample(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    symbol: Annotated[str, typer.Argument(help="Trading pair symbol, e.g., 'BTCUSDT'")],
    resample_interval: Annotated[str, typer.Argument(help="Resample interval, e.g., '1h', '4h'")],
    base_offset: Annotated[str, typer.Argument(help="Base offset, e.g., '5m', '15m', '30m'")],
):
    """
    Resample kline data for a single symbol.
    All multiples of base_offset will be generated.
    """
    divider(f"Resample kline {trade_type.value} {resample_interval} {symbol} base offset {base_offset}")
    resample_kline(trade_type=trade_type, symbol=symbol, resample_interval=resample_interval, base_offset=base_offset)


@app.command()
def resample_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    resample_interval: Annotated[str, typer.Argument(help="Resample interval, e.g., '1h', '4h'")],
    base_offset: Annotated[str, typer.Argument(help="Base offset, e.g., '5m', '15m', '30m'")],
):
    """
    Resample kline data for all symbols of given trade type and resample interval.
    """
    resample_kline_type(trade_type=trade_type, resample_interval=resample_interval, base_offset=base_offset)
