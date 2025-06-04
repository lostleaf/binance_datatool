# Architecture Document: BHDS (Binance Historical Data Service)

## 1. Introduction

This document provides a more detailed look into the architecture of the Binance Historical Data Service (BHDS), expanding on the System Design Document. It focuses on the internal structure of key modules, their interactions, data handling specifics, and design choices.

## 2. Core Architectural Principles

*   **Modularity:** Components are organized into distinct Python packages (`aws`, `api`, `generate`, `util`, `config`) with clear responsibilities.
*   **Pipeline Processing:** Data flows through a series of stages: download, verify, parse, generate, and resample.
*   **CLI-Driven:** All operations are initiated via a Typer-based command-line interface.
*   **Efficiency:** Asynchronous operations (`asyncio`) are used for I/O-bound tasks (downloads, API calls), and parallel processing (`multiprocessing`) is used for CPU-bound tasks (parsing, data generation).
*   **Configuration over Hardcoding:** System behavior (paths, parallelism, etc.) is largely controlled by `config.py` and environment variables.
*   **Data Format Standardization:** Polars DataFrames are the primary in-memory data structure, and Parquet is the standard storage format for processed data.

## 3. Module Interaction Overview

### 3.1. AWS Data Pipeline (Conceptual Flow for K-lines)

```
User CLI Command (e.g., `bhds aws_kline download ...`)
    -> aws.kline.app.py (Typer command handler)
        -> aws.kline.download.py (e.g., `download_spot_klines`)
            -> util.symbol_filter.py (to select symbols)
            -> aws.client_async.AwsKlineClient
                -> Lists symbols/files on AWS S3 (async HTTP via aiohttp)
                -> Invokes `aria2c` (subprocess) for file downloads (.zip, .CHECKSUM)
                    -> Stores raw data in `aws_data/...`

User CLI Command (e.g., `bhds aws_kline verify ...`)
    -> aws.kline.app.py
        -> aws.kline.verify.py (e.g., `verify_klines`)
            -> aws.checksum.get_verified_aws_data_files
                -> Reads .CHECKSUM files and compares with downloaded .zip files.

User CLI Command (e.g., `bhds aws_kline parse ...`)
    -> aws.kline.app.py
        -> aws.kline.parse.py (e.g., `parse_klines`)
            -> Uses ProcessPoolExecutor for parallel symbol processing
            -> For each symbol: `run_parse_symbol_kline`
                -> aws.checksum.get_verified_aws_data_files
                -> Reads .zip files, extracts CSVs
                -> `read_kline_csv`: Parses CSV into Polars DataFrame
                -> util.ts_manager.TSManager
                    -> Writes DataFrame to partitioned Parquet files in `parsed_data/...`
```

### 3.2. API Data Integration (Conceptual Flow for K-lines)

```
User CLI Command (e.g., `bhds api_data download_aws_missing_kline ...`)
    -> api.app.py (Typer command handler)
        -> api.kline.py (e.g., `api_download_missing_kline_for_symbols`)
            -> Identifies missing data ranges by comparing local AWS data against expected full range.
            -> api.binance_market_async.BinanceMarketAsync (or similar client)
                -> Makes async calls to Binance API endpoints.
            -> util.ts_manager.TSManager
                -> Stores fetched API data as Parquet files in `api_data/...`
```

### 3.3. Data Generation Pipeline (Conceptual Flow for K-lines)

```
User CLI Command (e.g., `bhds generate kline ...`)
    -> generate.app.py (Typer command handler)
        -> generate.kline.py (e.g., `gen_kline_type` -> `gen_kline` for each symbol)
            -> Uses ProcessPoolExecutor for parallel symbol processing
            -> For each symbol (`gen_kline`):
                1. generate.merge.merge_klines
                   -> util.ts_manager.TSManager (reads `parsed_data` and `api_data`)
                   -> Merges AWS and API K-line DataFrames.
                2. (Optional) Calculates VWAP.
                3. (Optional) generate.merge.merge_funding_rates
                   -> util.ts_manager.TSManager (reads funding rate data)
                   -> Joins funding rates to K-line DataFrame.
                4. (Optional) generate.kline_gaps.scan_gaps
                   -> Identifies significant gaps.
                5. (Optional) generate.kline_gaps.split_by_gaps
                   -> Splits DataFrame if gaps found.
                6. For each DataFrame segment:
                   -> generate.kline_gaps.fill_kline_gaps
                      -> Fills missing timestamps.
                   -> Writes final DataFrame to `results_data/...` as Parquet.
```

## 4. Detailed View of Key Modules

### 4.1. `aws.kline` Module
*   **`app.py`:** Defines Typer CLI commands for AWS K-line operations (download, verify, parse). Imports and calls functions from `download.py`, `verify.py`, `parse.py`.
*   **`download.py`:**
    *   `download_klines()`: Orchestrates download for a list of symbols.
    *   `download_spot_klines()`, `download_um_futures_klines()`, etc.: Specific downloaders that first list all symbols for a market type, filter them using `util.symbol_filter`, then call `download_klines()`.
    *   Relies on `aws.client_async.AwsKlineClient` for actual AWS interaction and `aria2c` execution.
*   **`parse.py`:**
    *   `parse_klines()`: Main entry point, uses `ProcessPoolExecutor` to parallelize `run_parse_symbol_kline()` across symbols.
    *   `run_parse_symbol_kline()`: Handles parsing for one symbol. It identifies relevant raw files, checks against already parsed data (via `TSManager`), reads CSVs using `read_kline_csv()`, and updates stored Parquet data via `TSManager`.
    *   `read_kline_csv()`: Reads a single zipped K-line CSV into a Polars DataFrame, applying the correct schema and performing initial datetime conversions.
*   **`verify.py`:** Uses `aws.checksum.get_verified_aws_data_files` to compare local file checksums with those from Binance.
*   **`util.py`:** Contains utility functions specific to AWS K-line data, like `local_list_kline_symbols`.

### 4.2. `api.kline` Module (Illustrative)
*   **`app.py` (in `api/`):** Defines Typer CLI commands for API K-line operations.
*   **`kline.py`:**
    *   `api_download_kline()`: Fetches K-lines for specific symbols and date ranges.
    *   `api_download_aws_missing_kline_for_symbols()`: Identifies date ranges for which AWS data is missing (by checking existing `parsed_data`) and fetches them via API.
    *   These functions use an API client (e.g., `BinanceMarketAsync`) to make HTTP requests to Binance API endpoints. Data is then typically stored using `TSManager`.

### 4.3. `generate.kline` Module
*   **`app.py` (in `generate/`):** Defines Typer CLI commands for generating final K-line datasets.
*   **`kline.py`:**
    *   `gen_kline_type()`: Main entry point for processing all symbols of a trade type. Uses `ProcessPoolExecutor` to parallelize `gen_kline()` across symbols.
    *   `gen_kline()`: The core function for processing a single symbol:
        *   Calls `generate.merge.merge_klines()` to combine AWS and API data.
        *   Performs enrichments (VWAP, funding rates by calling `generate.merge.merge_funding_rates()`).
        *   Handles gaps using functions from `generate.kline_gaps` (`scan_gaps`, `split_by_gaps`, `fill_kline_gaps`).
        *   Writes the final DataFrame(s) to Parquet in the `results_data` directory.
*   **`merge.py`:** Contains `merge_klines()` and `merge_funding_rates()`, which use `TSManager` to load necessary data and perform joins/concatenations.
*   **`kline_gaps.py`:** Contains logic for identifying data gaps based on time duration and price changes, splitting DataFrames at these gaps, and filling missing rows within continuous data segments.

## 5. Data Structures

*   **Primary In-Memory Structure:** `polars.DataFrame`.
*   **K-line Data Schema (Core Columns in Parquet files):**
    *   `candle_begin_time`: `pl.Datetime` (UTC, milliseconds resolution) - Start time of the candle.
    *   `open`: `pl.Float64` - Opening price.
    *   `high`: `pl.Float64` - Highest price.
    *   `low`: `pl.Float64` - Lowest price.
    *   `close`: `pl.Float64` - Closing price.
    *   `volume`: `pl.Float64` - Base asset volume.
    *   `quote_volume`: `pl.Float64` - Quote asset volume.
    *   `trade_num`: `pl.Int64` - Number of trades.
    *   `taker_buy_base_asset_volume`: `pl.Float64` - Volume of base asset bought by takers.
    *   `taker_buy_quote_asset_volume`: `pl.Float64` - Volume of quote asset bought by takers.
    *   *Generated K-lines may also include:*
        *   `avg_price_<interval>`: `pl.Float64` (VWAP).
        *   `funding_rate`: `pl.Float64`.
*   **Storage Format:** Apache Parquet for `parsed_data`, `api_data`, and `results_data`. Raw `aws_data` is stored as downloaded (zipped CSVs).

## 6. Concurrency Model

*   **Asynchronous I/O (`asyncio`):**
    *   Used for network operations:
        *   Listing files on AWS S3.
        *   Making calls to the Binance API.
    *   Implemented using `aiohttp` for HTTP clients and `async/await` syntax.
    *   Allows the system to handle many network requests concurrently without blocking, improving responsiveness for I/O-bound tasks.
*   **Parallel Processing (`multiprocessing`):**
    *   Used for CPU-bound tasks:
        *   Parsing raw CSV files (`aws.kline.parse.py`).
        *   Generating final K-line datasets (`generate.kline.py`).
        *   Resampling data (`generate.resample.py`).
    *   Implemented using `concurrent.futures.ProcessPoolExecutor`.
    *   The number of worker processes is configurable via `N_JOBS` in `config.py`.
*   **Aria2 for Downloads:** `aria2c` is an external tool invoked as a subprocess. It handles its own concurrent downloads internally.

## 7. Error Handling and Logging

*   **Error Handling:**
    *   Standard Python `try-except` blocks are used for expected errors (e.g., file not found, network issues).
    *   Checksum verification helps detect corrupted downloads.
    *   Failures in `ProcessPoolExecutor` tasks are generally propagated.
    *   More robust error handling for retries or partial failures could be an area for future enhancement.
*   **Logging (`util/log_kit.py`):**
    *   Uses Python's `logging` module.
    *   Provides console output with timestamps and log levels (INFO, DEBUG, WARNING, ERROR).
    *   `divider()` function is used to visually separate sections in logs.
    *   Log messages indicate the start/end of major operations and any significant events or errors.

## 8. Configuration System

*   **`config/config.py`:** Central point for defining application-wide constants and settings.
    *   Defines default paths (e.g., `_DEFAULT_BASE_DIR`).
    *   Reads environment variables (e.g., `CRYPTO_BASE_DIR`, `HTTP_PROXY`, `CRYPTO_NJOBS`) to allow runtime customization.
    *   Defines enums (`TradeType`, `ContractType`, `DataFrequency`) for type safety and clarity.
*   **Environment Variables:** Provide a flexible way to override default configurations without code changes.
*   **CLI Arguments:** Typer commands accept arguments and options that further control behavior for specific operations (e.g., symbol lists, time intervals, force flags).

This architectural design balances performance, modularity, and usability for its intended purpose of providing historical market data for quantitative research.
