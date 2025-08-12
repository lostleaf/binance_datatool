"""
BIDS CLI - Binance Historical Data Service

Command-line interface for downloading, processing, and maintaining
Binance cryptocurrency market data using AWS historical archives and Binance APIs.
"""

import asyncio

import typer

from bhds.tasks.aws_download import AwsDownloadTask

from . import __version__

app = typer.Typer(name="bhds", help="Binance Historical Data Service - CLI tool", add_completion=False)


@app.command()
def version():
    """Show version information."""
    typer.echo(f"bhds version {__version__}")


@app.command()
def aws_download(config_path: str = typer.Argument(..., help="Path to YAML config for AWS download task")):
    """Run AWS download task from a YAML configuration file."""
    task = AwsDownloadTask(config_path)
    asyncio.run(task.run())


if __name__ == "__main__":
    app()
