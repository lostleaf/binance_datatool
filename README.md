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

Please refer to `binance_data.sh` for examples of using BHDS

### Download candlestick data and verify checksum

```bash
# Download all usdt perpetual candle data
python cli.py bhds get_aws_all_usdt_perpetual 1h 1m
# Verify usdt perpetual candle data and delete corrupted
python cli.py bhds verify_aws_candle usdt_futures 1h 1m

# Download all coin perpetual candle data
python cli.py bhds get_aws_all_coin_perpetual 1h 1m
# Verify coin perpetual candle data and delete corrupted
python cli.py bhds verify_aws_candle coin_futures 1h 1m

# Download all usdt spot candle data
python cli.py bhds get_aws_all_usdt_spot 1h 1m
# Verify spot candle data and delete corrupted
python cli.py bhds verify_aws_candle spot 1h 1m
```

### Merge downloaded candlestick csv files and convert to Pandas parquet format

```bash
# Convert usdt perpetual
python cli.py bhds convert_aws_candle_csv usdt_futures 1h 1m

# Convert coin perpetual
python cli.py bhds convert_aws_candle_csv coin_futures 1h 1m

# Convert usdt spot
python cli.py bhds convert_aws_candle_csv spot 1h 1m
```

### Download aggtrades for recent days and verify checksum

```bash
python cli.py bhds get_aws_aggtrades usdt_futures --recent=30 BTCUSDT ETHUSDT
python cli.py bhds verify_aws_aggtrades usdt_futures
```

After the download procedure has successfully finished, the structure under `$CRYPTO_BASE_DIR` should look like:

```
CRYPTO_BASE_DIR
└── binance_data
    ├── aws_data
    │   └── data
    │       ├── futures
    │       │   ├── cm
    │       │   │   └── daily
    │       │   │       └── klines  [46 entries exceeds filelimit, not opening dir]
    │       │   └── um
    │       │       └── daily
    │       │           ├── aggTrades
    │       │           │   ├── BTCUSDT  [96 entries exceeds filelimit, not opening dir]
    │       │           │   └── ETHUSDT  [96 entries exceeds filelimit, not opening dir]
    │       │           └── klines  [297 entries exceeds filelimit, not opening dir]
    │       └── spot
    │           └── daily
    │               └── klines  [440 entries exceeds filelimit, not opening dir]
    └── candle_parquet
        ├── coin_futures
        │   ├── 1h  [46 entries exceeds filelimit, not opening dir]
        │   └── 1m  [46 entries exceeds filelimit, not opening dir]
        ├── spot
        │   ├── 1h  [440 entries exceeds filelimit, not opening dir]
        │   └── 1m  [439 entries exceeds filelimit, not opening dir]
        └── usdt_futures
            ├── 1h  [297 entries exceeds filelimit, not opening dir]
            └── 1m  [296 entries exceeds filelimit, not opening dir]
```
