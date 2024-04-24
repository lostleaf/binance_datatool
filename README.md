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

### Download candlestick data and verify checksum

```bash
# Download all usdt perpetual candle data
$PY cli.py bhds get_aws_all_usdt_perpetual 1h 1m
# Verify usdt perpetual candle data and delete corrupted
$PY cli.py bhds verify_aws_candle usdt_futures 1h 1m

# Download all coin perpetual candle data
$PY cli.py bhds get_aws_all_coin_perpetual 1h 1m
# Verify coin perpetual candle data and delete corrupted
$PY cli.py bhds verify_aws_candle coin_futures 1h 1m

# Download all usdt spot candle data
$PY cli.py bhds get_aws_all_usdt_spot 1h 1m
# Verify spot candle data and delete corrupted
$PY cli.py bhds verify_aws_candle spot 1h 1m
```

### Merge downloaded candlestick csv files and convert to Pandas parquet format

```bash
# Convert usdt perpetual
$PY cli.py bhds convert_aws_candle_csv usdt_futures 1h 1m

# Convert coin perpetual
$PY cli.py bhds convert_aws_candle_csv coin_futures 1h 1m

# Convert usdt spot
$PY cli.py bhds convert_aws_candle_csv spot 1h 1m
```

### Download aggtrades for recent days and verify checksum

```bash
python cli.py bhds get_aws_aggtrades usdt_futures --recent=30 BTCUSDT ETHUSDT
python cli.py bhds verify_aws_aggtrades usdt_futures
```

After the download procedure has successfully finished, the structure under `$CRYPTO_BASE_DIR` should look like:

```
CRYPTO_BASE_DIR
├── binance_data
│   ├── aws_data
│   │   └── data
│   │       └── futures
│   │           └── um
│   │               └── daily
│   │                   └── klines
│   │                       ├── BTCUSDT
│   │                       │   └── 1h  [4641 entries exceeds filelimit, not opening dir]
│   │                       ├── ETHUSDT
│   │                       │   └── 1h  [4641 entries exceeds filelimit, not opening dir]
│   │                       └── LTCUSDT
│   │                           └── 1h  [4614 entries exceeds filelimit, not opening dir]
│   └── candle_parquet
│       └── usdt_futures
│           └── 1h
│               ├── BTCUSDT.fea
│               ├── ETHUSDT.fea
│               └── LTCUSDT.fea
```