"""Typer application for bhds."""

from __future__ import annotations

from typing import Annotated

import typer

from binance_datatool.common import configure_cli_logging

app = typer.Typer(name="bhds", help="Binance Historical Data Service CLI.", add_completion=False)
archive_app = typer.Typer(name="archive", help="Browse data.binance.vision archive paths.")
app.add_typer(archive_app)


@app.callback()
def root_callback(
    ctx: typer.Context,
    verbose: Annotated[
        int,
        typer.Option(
            "-v",
            "--verbose",
            count=True,
            help="Increase log verbosity (-v=INFO, -vv=DEBUG).",
        ),
    ] = 0,
    bhds_home: Annotated[
        str | None,
        typer.Option("--bhds-home", help="Override the BHDS_HOME data directory."),
    ] = None,
) -> None:
    """Configure shared CLI state before running a command."""
    configure_cli_logging(verbose)
    ctx.obj = {} if ctx.obj is None else dict(ctx.obj)
    ctx.obj["bhds_home_override"] = bhds_home


# Register sub-command modules (side-effect import).
from binance_datatool.bhds.cli import archive as _archive  # noqa: F401,E402
