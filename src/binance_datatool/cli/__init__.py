"""Typer application for binance-datatool."""

from __future__ import annotations

from typing import Annotated

import typer

from binance_datatool.common import configure_cli_logging

app = typer.Typer(name="binance-datatool", help="Binance DataTool CLI.", add_completion=False)
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
    archive_home: Annotated[
        str | None,
        typer.Option(
            "--archive-home",
            help="Override the local archive data directory.",
        ),
    ] = None,
) -> None:
    """Configure shared CLI state before running a command."""
    configure_cli_logging(verbose)
    ctx.obj = {} if ctx.obj is None else dict(ctx.obj)
    ctx.obj["archive_home_override"] = archive_home


# Register sub-command modules (side-effect import).
from binance_datatool.cli import archive as _archive  # noqa: F401,E402
