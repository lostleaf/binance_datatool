"""Archive CLI commands."""

import asyncio
from typing import Annotated

import typer

from binance_datatool.bhds.archive import build_symbol_filter
from binance_datatool.bhds.cli import archive_app
from binance_datatool.bhds.workflow.archive import ArchiveListSymbolsWorkflow
from binance_datatool.common import ContractType, DataFrequency, DataType, TradeType


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
    quotes: Annotated[
        list[str] | None,
        typer.Option("--quote", help="Filter by quote asset. Repeat to allow multiple values."),
    ] = None,
    exclude_leverage: Annotated[
        bool,
        typer.Option("--exclude-leverage", help="Exclude leveraged spot tokens."),
    ] = False,
    exclude_stables: Annotated[
        bool,
        typer.Option("--exclude-stables", help="Exclude stablecoin pairs."),
    ] = False,
    contract_type: Annotated[
        ContractType | None,
        typer.Option("--contract-type", help="Filter futures symbols by contract type."),
    ] = None,
) -> None:
    """List symbol directories under a Binance archive prefix.

    Prints one symbol per line to stdout.
    """
    symbol_filter = build_symbol_filter(
        trade_type=trade_type,
        quote_assets=frozenset(quote.upper() for quote in quotes) if quotes else None,
        exclude_leverage=exclude_leverage,
        exclude_stable_pairs=exclude_stables,
        contract_type=contract_type,
    )
    workflow = ArchiveListSymbolsWorkflow(
        trade_type=trade_type,
        data_freq=data_freq,
        data_type=data_type,
        symbol_filter=symbol_filter,
    )
    for info in asyncio.run(workflow.run()).matched:
        typer.echo(info.symbol)
