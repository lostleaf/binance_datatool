import asyncio
import os
from typing import Optional
from typing_extensions import Annotated

import typer

from bhds import aws_kline
from constant import ContractType, TradeType

app = typer.Typer()

HTTP_PROXY = os.getenv('HTTP_PROXY', None) or os.getenv('http_proxy', None)


@app.command()
def download_aws_klines(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_interval: Annotated[
        str,
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    symbols: Annotated[
        list[str],
        typer.Argument(help="A list of trading symbols, e.g., 'BTCUSDT ETHUSDT'."),
    ],
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance klines for specific symbols from AWS data center
    '''
    asyncio.run(aws_kline.download_aws_klines(trade_type, time_interval, symbols, http_proxy))


@app.command()
def download_spot_klines(
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    quote: Annotated[str, typer.Option(help="The quote currency, e.g., 'USDT', 'USDC', 'BTC'.")] = 'USDT',
    stablecoins: Annotated[
        bool,
        typer.Option(help="Whether to include stablecoin symbols, such as 'USDCUSDT'."),
    ] = False,
    leverage_coins: Annotated[
        bool,
        typer.Option(help="Whether to include leveraged coin symbols, such as 'BTCUPUSDT'."),
    ] = False,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance spot klines from AWS data center
    '''
    for time_interval in time_intervals:
        asyncio.run(aws_kline.download_spot_klines(time_interval, quote, stablecoins, leverage_coins, http_proxy))


@app.command()
def download_um_futures_klines(
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    quote: Annotated[str, typer.Option(help="The quote currency, e.g., 'USDT', 'USDC', 'BTC'.")] = 'USDT',
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance USDⓈ-M Futures klines from AWS data center
    '''
    for time_interval in time_intervals:
        asyncio.run(aws_kline.download_um_futures_klines(time_interval, quote, contract_type, http_proxy))


@app.command()
def download_cm_futures_klines(
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance COIN-M Futures klines from AWS data center
    '''
    for time_interval in time_intervals:
        asyncio.run(aws_kline.download_cm_futures_klines(time_interval, contract_type, http_proxy))


@app.command()
def verify_klines(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_interval: Annotated[
        str,
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    symbols: Annotated[
        list[str],
        typer.Argument(help="A list of trading symbols, e.g., 'BTCUSDT ETHUSDT'."),
    ],
):
    '''
    Verify Binance Klines checksums and delete corrupted data for specific symbols
    '''
    aws_kline.verify_klines(trade_type, time_interval, symbols)


@app.command()
def verify_klines_all_symbols(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
):
    '''
    Verify Binance Klines for all symbols with the given trade type and time interval
    '''
    for time_interval in time_intervals:
        aws_kline.verify_klines_all_symbols(trade_type, time_interval)


if __name__ == '__main__':
    app()
