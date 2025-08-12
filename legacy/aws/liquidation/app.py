import asyncio
import os
from typing import Optional

import typer
from typing_extensions import Annotated

from config import ContractType, TradeType

from .download import download_cm_futures_liquidation, download_liquidation, download_um_futures_liquidation

app = typer.Typer()

HTTP_PROXY = os.getenv('HTTP_PROXY', None) or os.getenv('http_proxy', None)


@app.command()
def download(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    symbols: Annotated[
        list[str],
        typer.Argument(help="A list of trading symbols, e.g., 'BTCUSDT ETHUSDT'."),
    ],
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance liquidation snapshot for specific symbols from AWS data center
    '''
    asyncio.run(download_liquidation(trade_type, symbols, http_proxy))


@app.command()
def download_um_futures(
    quote: Annotated[str, typer.Option(help="The quote currency, e.g., 'USDT', 'USDC', 'BTC'.")] = 'USDT',
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance USDⓈ-M Futures liquidation snapshot
    '''
    # http_proxy = 'http://127.0.0.1:1082'
    asyncio.run(download_um_futures_liquidation(quote, contract_type, http_proxy))


@app.command()
def download_cm_futures(
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance Coin Futures liquidation snapshot
    '''
    asyncio.run(download_cm_futures_liquidation(contract_type, http_proxy))
