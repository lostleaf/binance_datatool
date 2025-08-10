"""
BIDS CLI - Binance Historical Data Service

Command-line interface for downloading, processing, and maintaining
Binance cryptocurrency market data using AWS historical archives and Binance APIs.
"""

import typer

from . import __version__

app = typer.Typer(
    name="bhds",
    help="Binance Historical Data Service - CLI tool for crypto market data",
    add_completion=False
)


@app.command()
def version():
    """Show version information."""
    typer.echo(f"bhds version {__version__}")


@app.command()
def hello(
    name: str = typer.Argument(..., help="Name to greet"),
    count: int = typer.Option(1, "--count", "-c", help="Number of greetings"),
):
    """Simple hello command for testing CLI functionality."""
    for _ in range(count):
        typer.echo(f"Hello, {name}!")


if __name__ == "__main__":
    app()