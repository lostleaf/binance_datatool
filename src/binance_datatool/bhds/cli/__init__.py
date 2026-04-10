"""Typer application for bhds."""

import typer

app = typer.Typer(name="bhds", help="Binance Historical Data Service CLI.", add_completion=False)
archive_app = typer.Typer(name="archive", help="Browse data.binance.vision archive paths.")
app.add_typer(archive_app)

from binance_datatool.bhds.cli import archive as _archive  # noqa: F401,E402
