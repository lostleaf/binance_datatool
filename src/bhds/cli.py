"""
BIDS CLI - Binance Historical Data Service

Command-line interface for downloading, processing, and maintaining
Binance cryptocurrency market data using AWS historical archives and Binance APIs.
"""

import asyncio

import typer

from bhds.tasks.aws_download import AwsDownloadTask
from bhds.tasks.holo_1m_kline import GenHolo1mKlineTask
from bhds.tasks.holo_resample import HoloResampleTask
from bhds.tasks.parse_aws_data import ParseAwsDataTask

from . import __version__

app = typer.Typer(name="bhds", help="Binance Historical Data Service - CLI tool", add_completion=False)


@app.command()
def version():
    """Show version information."""
    typer.echo(f"bhds version {__version__}")


@app.command()
def aws_download(config_paths: list[str] = typer.Argument(..., help="Paths to YAML configs for AWS download tasks")):
    """Run AWS download tasks from YAML configuration files."""
    for config_path in config_paths:
        task = AwsDownloadTask(config_path)
        asyncio.run(task.run())


@app.command()
def parse_aws_data(
    config_paths: list[str] = typer.Argument(..., help="Paths to YAML configs for parse AWS data tasks")
):
    """Parse AWS downloaded data from CSV to Parquet with optional API completion."""
    for config_path in config_paths:
        task = ParseAwsDataTask(config_path)
        asyncio.run(task.run())


@app.command()
def holo_1m_kline(
    config_paths: list[str] = typer.Argument(..., help="Paths to YAML configs for generate holo 1m kline tasks")
):
    """Generate holo 1m kline from parsed data."""
    for config_path in config_paths:
        GenHolo1mKlineTask(config_path).run()


@app.command()
def resample(
    config_paths: list[str] = typer.Argument(..., help="Paths to YAML configs for holo kline resample tasks")
):
    """Resample holo 1m klines to higher time frames."""
    for config_path in config_paths:
        HoloResampleTask(config_path).run()


if __name__ == "__main__":
    app()
