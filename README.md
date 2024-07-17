# Binance DataTool

Binance DataTool is a Comprehensive data toolbox for Binance quantitative trading, created by lostleaf.eth and contributed by community.

The suite currently includes:
- [BHDS](#bhds): Binance Historical Data Service. BHDS retrieves historical market data from the [Binance AWS data center](https://data.binance.vision/) and the market data API, transforming raw data into the Pandas Parquet format.
- BMAC: Binance Marketdata Async Client.

## Dependencies

Refer to the `environment.yml` file for setting up the conda environment.

By default, the BHDS service utilizes `$HOME/crypto_data` as the base directory. All data is downloaded into this directory. Modify the base directory by setting the `CRYPTO_BASE_DIR` environment variable.

The BHDS service uses `aria2`, an efficient cross-platform command line download software. 

Linux/MacOS x86_64 users may install it using `conda` or their system's package managers:

``` bash
conda install -c conda-forge aria2
```

For Arm64 MacOS users, installation can be executed with:

``` bash
brew install aria2
```

## BHDS

For examples of using BHDS, please refer to `binance_data.sh`.

### Download Candlestick Data and Verify Checksum

```bash
# Download 1-hour candlestick data for all USDT perpetual symbols
python cli.py bhds get_aws_all_usdt_perpetual 1h
# Verify 1-hour USDT perpetual candlestick data and remove corrupted files
python cli.py bhds verify_aws_candle usdt_futures 1h

# Download 1-hour candlestick data for all coin perpetual symbols
python cli.py bhds get_aws_all_coin_perpetual 1h
# Verify 1-hour coin perpetual candlestick data and remove corrupted files
python cli.py bhds verify_aws_candle coin_futures 1h

# Download 1-hour candlestick data for all spot symbols
python cli.py bhds get_aws_all_usdt_spot 1h
# Verify 1-hour spot candle data and remove corrupted files
python cli.py bhds verify_aws_candle spot 1h
```

### Download Missing Candlestick Data from Market Data API

```bash
# Download missing 1-hour USDT perpetual candlestick data from market data API 
python cli.py bhds download_aws_missing_candle usdt_futures 1h

# Download missing 1-hour spot candlestick data from market data API
python cli.py bhds download_aws_missing_candle spot 1h

# Download missing 1-hour coin perpetual candlestick data from market data API
python cli.py bhds download_aws_missing_candle coin_futures 1h
```

### Merge Downloaded Candlestick CSV Files and Convert to Pandas Parquet Format

```bash
# Convert 1-hour USDT perpetual candlestick data to Pandas Parquet
python cli.py bhds convert_aws_candle_csv usdt_futures 1h

# Convert 1-hour coin perpetual candlestick data to Pandas Parquet
python cli.py bhds convert_aws_candle_csv coin_futures 1h

# Convert 1-hour spot candlestick data to Pandas Parquet
python cli.py bhds convert_aws_candle_csv spot 1h
```

### Split Candlestick Data and Fill Gaps

Split the delisted and relisted symbols like LUNA

```bash
# Split 1-hour USDT perpetual candlestick data and fill gaps
python cli.py bhds fix_candle aws usdt_futures 1h

# Split 1-hour spot candlestick data and fill gaps
python cli.py bhds fix_candle aws spot 1h

# Split 1-hour coin perpetual candlestick data and fill gaps
python cli.py bhds fix_candle aws coin_futures 1h
```

### Download Aggregated Trades for Recent Days and Verify Checksum

```bash
# Download aggregated trades data for the recent 30 days for specified symbols
python cli.py bhds get_aws_aggtrades usdt_futures --recent=30 BTCUSDT ETHUSDT
# Verify aggregated trades data and remove corrupted files
python cli.py bhds verify_aws_aggtrades usdt_futures
```

Upon successful completion of the download process, the structure under `$CRYPTO_BASE_DIR` should look like:

```
CRYPTO_BASE_DIR
└── ./binance_data
    ├── api_data                Downloaded from market api
    │   ├── coin_futures
    │   │   └── 1h
    │   ├── spot
    │   │   └── 1h
    │   └── usdt_futures
    │       └── 1h
    ├── aws_data                Downloaded from aws data center
    │   └── data
    │       ├── futures
    │       └── spot
    ├── candle_parquet          Merged parquet data
    │   ├── coin_futures
    │   │   └── 1h
    │   ├── spot
    │   │   └── 1h
    │   └── usdt_futures
    │       └── 1h
    └── candle_parquet_fixed    Splited parquet data with gaps filled
        ├── coin_futures
        │   └── 1h
        ├── spot
        │   └── 1h
        └── usdt_futures
            └── 1h
```
