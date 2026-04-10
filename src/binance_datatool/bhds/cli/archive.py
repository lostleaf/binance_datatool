"""Archive CLI commands."""

import asyncio
from typing import Annotated

import typer

from binance_datatool.bhds.cli import archive_app
from binance_datatool.bhds.workflow.archive import ArchiveListSymbolsWorkflow
from binance_datatool.common import DataFrequency, DataType, TradeType


@archive_app.command("list-symbols")
def list_symbols_command(
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    data_freq: Annotated[
        DataFrequency,
        typer.Option("--freq", help="Partition frequency."),
    ] = DataFrequency.daily,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type."),
    ] = DataType.klines,
) -> None:
    """List symbol directories under a Binance archive prefix."""
    workflow = ArchiveListSymbolsWorkflow(trade_type, data_freq, data_type)
    for symbol in asyncio.run(workflow.run()):
        typer.echo(symbol)
