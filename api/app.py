import asyncio
import os
from typing import Optional

import typer
from typing_extensions import Annotated

from config import TradeType

from .funding import api_download_funding_rates, api_download_funding_rates_type_all
from .kline import api_download_aws_missing_kline_for_type, api_download_kline, api_download_missing_kline_for_symbols

app = typer.Typer()

HTTP_PROXY = os.getenv("HTTP_PROXY", None) or os.getenv("http_proxy", None)


@app.command()
def download_kline(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_interval: Annotated[
        str,
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    symbol: Annotated[
        str,
        typer.Argument(help="A trading symbol, e.g., 'BTCUSDT' or 'ETHUSDT'."),
    ],
    dts: Annotated[
        list[str],
        typer.Argument(help="A list trading dates, e.g., '20200101 20210203'."),
    ],
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance klines for specific symbol and dates from Kline API
    """
    sym_dts = [(symbol, dt) for dt in dts]
    asyncio.run(api_download_kline(trade_type, time_interval, sym_dts, http_proxy))


@app.command()
def download_aws_missing_kline_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_interval: Annotated[
        str,
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    overwrite: Annotated[bool, typer.Option(help="Whether to overwrite existing files")] = False,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance kline data from the Kline API for the provided trade_type that have missing dates in AWS
    """
    asyncio.run(api_download_aws_missing_kline_for_type(trade_type, time_interval, overwrite, http_proxy))


@app.command()
def download_aws_missing_kline(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    time_interval: Annotated[
        str,
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    symbols: Annotated[
        list[str],
        typer.Argument(help="List of trading symbols, e.g., 'BTCUSDT ETHUSDT'."),
    ],
    overwrite: Annotated[bool, typer.Option(help="Whether to overwrite existing files")] = False,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance kline data from the Kline API for given symbols that have missing dates in AWS
    """

    asyncio.run(api_download_missing_kline_for_symbols(trade_type, symbols, time_interval, overwrite, http_proxy))


@app.command()
def download_recent_funding(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    symbols: Annotated[list[str], typer.Argument(help="Trading symbols, e.g., 'BTCUSDT' or 'ETHUSDT'.")],
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance funding rate for specific symbol from Binance API
    """
    asyncio.run(api_download_funding_rates(trade_type, symbols, http_proxy))



@app.command()
def download_recent_funding_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance funding rate for all symbols of a specific trade type from Binance API
    """
    asyncio.run(api_download_funding_rates_type_all(trade_type, http_proxy))

