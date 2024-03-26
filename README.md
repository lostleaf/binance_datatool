# Crypto Toolbox

Useful services for cryptocurrency quant trading

Currently, the following services are included
- BHDS: Binance historical data service

## Prerequisites

Please refer to the `environment.yml` for conda environment setup.

The BHDS service depends on `aria2`, a lightweight cross platform download utility, 
Linux/MacOS x86_64 users may install with `conda`, or system package managers:

``` bash
conda install -c conda-forge aria2
```

For Arm64 MacOS users, install with

``` bash
brew install aria2
```

## BHDS

Usages (printed by `python cli.py bhds`):

```
NAME
    cli.py bhds - Binance Historical Data Service

SYNOPSIS
    cli.py bhds COMMAND

DESCRIPTION
    Supported types: spot, usdt_futures, coin_futures

COMMANDS
    COMMAND is one of the following:

     convert_aws_candle_csv
       Convert and merge downloaded candlestick data to Pandas Feather

     get_aws_candle
       Download daily candlestick data from Binance's AWS data center. All available dates will be downloaded.

     verify_all_candle
       Verify all candlestick data integrity and delete wrong data
```

For example, to download 1hour candlestick data of perpetual contracts BTCUDST, ETHUSDT and LTCUSDT from Binance,
and then merge them info Pandas Feather format, a suggested download procedure is shown as following:

``` bash
# Download and verify the candlestick for the first time
python cli.py bhds get_aws_candle usdt_futures 1h BTCUSDT ETHUSDT LTCUSDT
python cli.py bhds verify_all_candle usdt_futures 1h

# Download and verify the candlestick for the second time, in case some files are missing
python cli.py bhds get_aws_candle usdt_futures 1h BTCUSDT ETHUSDT LTCUSDT
python cli.py bhds verify_all_candle usdt_futures 1h

# Convert and merge into a single Feather file
python cli.py bhds convert_aws_candle_csv usdt_futures 1h
```