# Crypto Toolbox

Useful services for cryptocurrency quant trading.

Currently, the services included are:
- BHDS: Binance Historical Data Service.

## Prerequisites

Please refer to the `environment.yml` file for the setup of the conda environment.

By default, the BHDS service uses `$HOME/crypto_data` as the base directory. All data will be downloaded into this base directory. The base directory can be changed by setting the environment variable `CRYPTO_BASE_DIR`.

The BHDS service requires `aria2`, a lightweight cross-platform download utility. 
Linux/MacOS x86_64 users may install it with `conda` or through system package managers:

``` bash
conda install -c conda-forge aria2
```

For Arm64 MacOS users, installation can be done with:

``` bash
brew install aria2
```

## BHDS

Usage (as printed by `python cli.py bhds`):

```
NAME
    cli.py bhds - Binance Historical Data Service

SYNOPSIS
    cli.py bhds COMMAND

DESCRIPTION
    Supports types: spot, usdt_futures, coin_futures

COMMANDS
    COMMAND is one of the following:

     convert_aws_candle_csv
       Converts and merges downloaded candlestick data into Pandas Feather format.

     get_aws_candle
       Downloads daily candlestick data from Binance's AWS data center. All available dates will be downloaded.

     verify_all_candle
       Verifies the integrity of all candlestick data and deletes incorrect data.
```

For example, to download 1-hour candlestick data for perpetual contracts BTCUSDT, ETHUSDT, and LTCUSDT from Binance, and then merge them into Pandas Feather format, a suggested download procedure is as follows:

``` bash
# Download and verify the candlestick data for the first time
python cli.py bhds get_aws_candle usdt_futures 1h BTCUSDT ETHUSDT LTCUSDT
python cli.py bhds verify_all_candle usdt_futures 1h

# Download and verify the candlestick data for the second time, in case some files are missing
python cli.py bhds get_aws_candle usdt_futures 1h BTCUSDT ETHUSDT LTCUSDT
python cli.py bhds verify_all_candle usdt_futures 1h

# Convert and merge into a single Feather file
python cli.py bhds convert_aws_candle_csv usdt_futures 1h
```