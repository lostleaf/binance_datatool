"""Archive CLI commands."""

import asyncio
import sys
from datetime import UTC
from typing import Annotated

import typer
from loguru import logger

from binance_datatool.bhds.archive import build_symbol_filter
from binance_datatool.bhds.cli import archive_app
from binance_datatool.bhds.workflow.archive import (
    ArchiveListFilesWorkflow,
    ArchiveListSymbolsWorkflow,
)
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


def _resolve_symbols(symbols: list[str] | None) -> list[str]:
    """Resolve symbol inputs from CLI arguments or piped stdin."""
    if symbols:
        return [symbol.strip().upper() for symbol in symbols if symbol.strip()]

    if not sys.stdin.isatty():
        return [line.strip().upper() for line in sys.stdin.read().splitlines() if line.strip()]

    return []


def _validate_interval(data_type: DataType, interval: str | None) -> None:
    """Validate whether an interval matches the selected data type."""
    if data_type.has_interval_layer and interval is None:
        raise typer.BadParameter(
            "Option '--interval' is required for kline-class data types.",
            param_hint="--interval",
        )
    if not data_type.has_interval_layer and interval is not None:
        raise typer.BadParameter(
            "Option '--interval' is only valid for kline-class data types.",
            param_hint="--interval",
        )


def _matches_output_filter(
    key: str,
    *,
    only_zip: bool,
    only_checksum: bool,
) -> bool:
    """Return whether a listed file should be printed."""
    if only_zip:
        return not key.endswith(".CHECKSUM")
    if only_checksum:
        return key.endswith(".CHECKSUM")
    return True


def _format_relative_path(
    key: str,
    trade_type: TradeType,
    data_freq: DataFrequency,
    data_type: DataType,
) -> str:
    """Format a key relative to the shared archive prefix."""
    prefix = f"data/{trade_type.s3_path}/{data_freq.value}/{data_type.value}/"
    return key.removeprefix(prefix)


@archive_app.command("list-files")
def list_files_command(
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    symbols: Annotated[list[str] | None, typer.Argument(help="Symbols to list files for.")] = None,
    data_freq: Annotated[
        DataFrequency,
        typer.Option("--freq", help="Partition frequency."),
    ] = DataFrequency.daily,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type."),
    ] = DataType.klines,
    interval: Annotated[
        str | None,
        typer.Option("--interval", help="Interval for kline-class data types."),
    ] = None,
    long_format: Annotated[
        bool,
        typer.Option("-l", "--long", help="Print three-column TSV output."),
    ] = False,
    only_zip: Annotated[
        bool,
        typer.Option("--only-zip", help="Print only .zip files."),
    ] = False,
    only_checksum: Annotated[
        bool,
        typer.Option("--only-checksum", help="Print only .zip.CHECKSUM files."),
    ] = False,
) -> None:
    """List archive files under one or more symbol directories."""
    if only_zip and only_checksum:
        raise typer.BadParameter(
            "Options '--only-zip' and '--only-checksum' are mutually exclusive.",
            param_hint="--only-zip",
        )

    _validate_interval(data_type, interval)

    resolved_symbols = _resolve_symbols(symbols)
    if not resolved_symbols:
        raise typer.BadParameter("No symbols given.", param_hint="SYMBOLS")

    workflow = ArchiveListFilesWorkflow(
        trade_type=trade_type,
        data_freq=data_freq,
        data_type=data_type,
        symbols=resolved_symbols,
        interval=interval,
    )
    result = asyncio.run(workflow.run())

    for entry in result.per_symbol:
        if entry.error is not None:
            logger.error("{}: {}", entry.symbol, entry.error)
            continue

        for archive_file in entry.files:
            if not _matches_output_filter(
                archive_file.key,
                only_zip=only_zip,
                only_checksum=only_checksum,
            ):
                continue

            relative_path = _format_relative_path(
                archive_file.key,
                trade_type=trade_type,
                data_freq=data_freq,
                data_type=data_type,
            )
            if long_format:
                timestamp = archive_file.last_modified.astimezone(UTC).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                typer.echo(f"{archive_file.size}\t{timestamp}\t{relative_path}")
            else:
                typer.echo(relative_path)

    if result.has_failures:
        raise typer.Exit(code=2)
