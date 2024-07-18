# Binance DataTool

Binance DataTool is a Comprehensive data toolbox for Binance quantitative trading, created by lostleaf.eth and contributed by community.

The suite currently includes:
- [BHDS](#bhds): Binance Historical Data Service. BHDS retrieves historical market data from the [Binance AWS data center](https://data.binance.vision/) and the market data API, transforming raw data into the Pandas Parquet format.
- [BMAC](#bmac): Binance Marketdata Async Client. An asnyc marketdata client powered by both Websocket and REST API.

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

## BMAC

Unlike BHDS, BMAC does not rely on `aria2`.

### Configuration

To use BMAC, first, Create a new folder as the base directory, for example, `~/udeli_1m`.

Then, in the newly created folder, create a configuration file named `config.json`. A minimal configuration example is as follows:

```json
{
    "interval": "1m",
    "trade_type": "usdt_deli"
}
```

BMAC will receive 1-minute K-line data for USDT delivery contracts based on this configuration.

## Running

The entry point for Binance DataTool is unified as `cli.py`, and the entry point for BMAC2 is `python cli.py bmac start`. For example:

```bash
python cli.py bmac start ~/udeli_1m
```

BMAC will first initialize historical data through the REST API and then update the data by subscribing to the websocket.

The directory structure during operation is as follows:

```
udeli_1m
├── config.json
├── exginfo_1m
│   ├── exginfo.pqt
│   └── exginfo_20240717_193700.ready
└── usdt_deli_1m
    ├── BTCUSDT_240927.pqt
    ├── BTCUSDT_240927_20240717_193700.ready
    ├── BTCUSDT_241227.pqt
    ├── BTCUSDT_241227_20240717_193700.ready
    ├── ETHUSDT_240927.pqt
    ├── ETHUSDT_240927_20240717_193700.ready
    ├── ETHUSDT_241227.pqt
    └── ETHUSDT_241227_20240717_193700.ready
```

### Core Parameters

BMAC2 mainly includes two core parameters, `interval` and `trade_type`, which represent the K-line time interval and the type of trading instrument, respectively.

The `interval` can be `1m`, `5m`, `1h`, `4h`, etc., as supported by Binance.

The `trade_type` has several options, as defined below, including different types of spot, USDT-margined contracts, and coin-margined contracts.

```python
DELIVERY_TYPES = ['CURRENT_QUARTER', 'NEXT_QUARTER']

{
    # spot
    'usdt_spot': (TradingSpotFilter(quote_asset='USDT', keep_stablecoins=False), 'spot'),
    'usdc_spot': (TradingSpotFilter(quote_asset='USDC', keep_stablecoins=False), 'spot'),
    'btc_spot': (TradingSpotFilter(quote_asset='BTC', keep_stablecoins=False), 'spot'),

    # usdt_futures
    'usdt_perp': (TradingUsdtFuturesFilter(quote_asset='USDT', types=['PERPETUAL']), 'usdt_futures'),
    'usdt_deli': (TradingUsdtFuturesFilter(quote_asset='USDT', types=DELIVERY_TYPES), 'usdt_futures'),
    'usdc_perp': (TradingUsdtFuturesFilter(quote_asset='USDC', types=['PERPETUAL']), 'usdt_futures'),

    # Only includes ETHBTC perpetual contract, a USDT-margined contract
    'btc_perp': (TradingUsdtFuturesFilter(quote_asset='BTC', types=['PERPETUAL']), 'usdt_futures'),

    # coin_futures
    'coin_perp': (TradingCoinFuturesFilter(types=['PERPETUAL']), 'coin_futures'),
    'coin_deli': (TradingCoinFuturesFilter(types=DELIVERY_TYPES), 'coin_futures'),
}
```

### Optional Parameters

BMAC2 includes multiple optional parameters. Refer to the definitions in `handler.py` as follows:

```python
# Optional parameters

# Number of K-line data to retain, default is 1500
self.num_candles = cfg.get('num_candles', 1500)
# Whether to fetch funding rates, default is False
self.fetch_funding_rate = cfg.get('funding_rate', False)
# HTTP timeout in seconds, default is 5 seconds
self.http_timeout_sec = int(cfg.get('http_timeout_sec', 5))
# K-line close timeout in seconds, default is 15 seconds
self.candle_close_timeout_sec = int(cfg.get('candle_close_timeout_sec', 15))
# Symbol whitelist, if present, only fetches symbols in the whitelist, default is None
self.keep_symbols = cfg.get('keep_symbols', None)
# K-line data storage format, default is parquet, can also be feather
save_type = cfg.get('save_type', 'parquet')
# Dingding configuration, default is None
self.dingding = cfg.get('dingding', None)
# Number of REST fetchers
self.num_rest_fetchers = cfg.get('num_rest_fetchers', 8)
# Number of websocket listeners
self.num_socket_listeners = cfg.get('num_socket_listeners', 8)
```

You can also refer to the examples in the `bmac_example` directory for configuration, such as `bmac_example/usdt_perp_5m_all/config.json.example`.
