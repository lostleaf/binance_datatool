import asyncio
import os
from typing import Optional

import typer
from typing_extensions import Annotated

from config import ContractType, TradeType

from .download import download_cm_futures_funding_rates, download_funding_rates, download_um_futures_funding_rates
from .parse import parse_funding_rates, parse_type_all_funding_rates
from .verify import verify_funding_rates, verify_type_all_funding_rates

app = typer.Typer()

HTTP_PROXY = os.getenv("HTTP_PROXY", None) or os.getenv("http_proxy", None)


@app.command()
def download(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    symbols: Annotated[
        list[str],
        typer.Argument(help="A list of trading symbols, e.g., 'BTCUSDT ETHUSDT'."),
    ],
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance funding rates for specific symbols from AWS data center
    """
    asyncio.run(download_funding_rates(trade_type, symbols, http_proxy))


@app.command()
def download_um_futures(
    quote: Annotated[str, typer.Option(help="The quote currency, e.g., 'USDT', 'USDC', 'BTC'.")] = "USDT",
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance USDⓈ-M Futures funding rates
    """
    asyncio.run(download_um_futures_funding_rates(quote, contract_type, http_proxy))


@app.command()
def download_cm_futures(
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance Coin Futures funding rates
    """
    asyncio.run(download_cm_futures_funding_rates(contract_type, http_proxy))


@app.command()
def verify(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    symbols: Annotated[list[str], typer.Argument(help="A list of trading symbols, e.g., 'BTCUSDT ETHUSDT'.")],
):
    """
    Verify Binance funding rates for specific symbols from AWS data center
    """
    verify_funding_rates(trade_type, symbols)


@app.command()
def verify_type_all(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
):
    """
    Verify Binance funding rates for all symbols with the given trade type
    """
    verify_type_all_funding_rates(trade_type)


@app.command()
def parse(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    symbols: Annotated[list[str], typer.Argument(help="A list of trading symbols, e.g., 'BTCUSDT ETHUSDT'.")],
):
    """
    Parse Binance funding rates for all symbols with the given trade type
    """
    parse_funding_rates(trade_type, symbols)


@app.command()
def parse_type_all(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
):
    """
    Parse Binance funding rates for all symbols with the given trade type
    """
    parse_type_all_funding_rates(trade_type)
