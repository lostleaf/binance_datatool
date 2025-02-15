import typer

from aws.funding.app import app as aws_funding
from aws.liquidation.app import app as aws_liquidation
from aws.kline.app import app as aws_kline
from api.app import app as api_data

app = typer.Typer()

app.add_typer(
    aws_funding,
    name="aws_funding",
    help="Commands for maintaining Binance AWS funding rate data.",
)
app.add_typer(
    aws_kline,
    name="aws_kline",
    help="Commands for maintaining Binance AWS K-line data.",
)
app.add_typer(api_data, name="api_data", help="Commands for maintaining Binance API data.")
app.add_typer(
    aws_liquidation, name="aws_liquidation", help="Commands for maintaining Binance AWS liquidation snapshot data."
)
if __name__ == "__main__":
    app()
