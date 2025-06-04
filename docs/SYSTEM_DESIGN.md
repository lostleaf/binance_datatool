# System Design Document: BHDS (Binance Historical Data Service)

## 1. Introduction

This document outlines the system design for BHDS (Binance Historical Data Service). BHDS is a Python application designed to download, process, and store historical market data from Binance for quantitative analysis and trading research. It operates as a CLI-driven toolset with a modular architecture.

## 2. System Architecture Overview

BHDS employs a modular, pipeline-oriented architecture. Users interact with the system via a Command Line Interface (CLI) built using the Typer library. The system is organized into several core packages, each responsible for a specific stage of data handling:

*   **Configuration (`config`):** Manages global settings, paths, and type definitions.
*   **AWS Data (`aws`):** Handles interaction with Binance's AWS public data repository. This includes downloading, verifying, and parsing data.
*   **API Data (`api`):** Manages interaction with the official Binance API, primarily for fetching recent data or filling gaps.
*   **Data Generation (`generate`):** Processes and transforms data from AWS and API sources into final, usable datasets. This includes merging, feature enrichment, gap handling, and resampling.
*   **Utilities (`util`):** Provides common functionalities like logging, time manipulation, network operations, symbol filtering, and a Time Series Manager for handling partitioned Parquet data.

The system is designed for batch processing, with many operations capable of running in parallel using Python's multiprocessing features for CPU-bound tasks and asyncio for I/O-bound tasks.

## 3. Major Components

### 3.1. CLI (`bhds.py`, `*/app.py`)
*   **Interface:** Typer-based CLI.
*   **Function:** Provides user access to all system functionalities through commands and subcommands.
*   **Structure:** A main `bhds.py` application aggregates Typer sub-applications from various modules (e.g., `aws.kline.app`, `api.app`, `generate.app`).

### 3.2. Configuration Manager (`config/config.py`)
*   **Function:** Defines and manages global configurations:
    *   Data storage paths (e.g., `BINANCE_DATA_DIR`).
    *   Environment variable access (e.g., `CRYPTO_BASE_DIR`, `HTTP_PROXY`, `N_JOBS`).
    *   Enumerations for `TradeType`, `ContractType`, `DataFrequency`.
    *   Constants like `HTTP_TIMEOUT_SEC`.

### 3.3. AWS Data Module (`aws/`)
*   **Sub-modules:** `kline`, `funding`, `liquidation`. Each contains `app.py` (CLI commands), `download.py`, `parse.py`, and often `verify.py` and `util.py`.
*   **Client (`aws/client_async.py`):** `AwsDataClient`, `AwsKlineClient`, `AwsFundingRateClient`, `AwsLiquidationClient`. These clients abstract the specifics of interacting with the Binance AWS S3 structure and utilize `aria2c` (via subprocess) for downloads.
*   **Functionality:**
    *   Listing available data on AWS.
    *   Downloading raw data files (zipped CSVs).
    *   Verifying checksums of downloaded files against Binance-provided checksums.
    *   Parsing raw CSV data into structured Polars DataFrames and saving as Parquet files, managed by `TSManager`.

### 3.4. API Data Module (`api/`)
*   **Sub-modules:** `kline`, `funding`, `binance.py` (Binance API client wrapper).
*   **Client (`api/binance.py`, `api/binance_market_async.py`):** Wrappers around a Python Binance API library to fetch data.
*   **Functionality:**
    *   Fetching K-lines for specific date ranges or to fill gaps.
    *   Fetching recent funding rates.
    *   Data is typically processed and stored directly in Parquet format by the `TSManager`.

### 3.5. Data Generation Module (`generate/`)
*   **Sub-modules:** `kline.py`, `resample.py`, `merge.py`, `kline_gaps.py`.
*   **Functionality:**
    *   **Merging:** Combining data from AWS (parsed) and API sources (`merge_klines`, `merge_funding_rates`).
    *   **Enrichment:** Adding calculated columns like VWAP and joining funding rates to K-line data.
    *   **Gap Handling:** Identifying time-series gaps (`scan_gaps`), optionally splitting data files based on gaps (`split_by_gaps`), and filling missing timestamps within segments (`fill_kline_gaps`).
    *   **Resampling:** Converting K-line data from a base interval to higher intervals (e.g., 1m to 1h, 4h).
*   **Output:** Produces the final "results" datasets in Parquet format.

### 3.6. Utility Modules (`util/`)
*   **`TSManager` (`util/ts_manager.py`):** Manages time-series data stored in partitioned Parquet files (e.g., by month or year). Handles reading, writing, and updating these datasets.
*   **`log_kit.py`:** Logging setup and utilities.
*   **`time.py`:** Time and date related utilities.
*   **`network.py`:** Network utilities, including `aiohttp` session creation.
*   **`concurrent.py`:** Multiprocessing utilities.
*   **`symbol_filter.py`:** Logic for filtering lists of trading symbols.
*   **`checksum.py` (in `aws/`):** Handles checksum verification.

## 4. Data Flow

The data flows through BHDS in several stages:

1.  **Initiation (User via CLI):** User issues a command, e.g., `python bhds.py aws_kline download ...` or `python bhds.py generate kline ...`.

2.  **Raw Data Acquisition:**
    *   **AWS:**
        *   `aws/.../download.py` functions are called.
        *   `AwsDataClient` lists files on Binance S3, then uses `aria2c` to download raw `.zip` files (containing CSVs) and corresponding `.CHECKSUM` files to a local `aws_data/<data_type>/<frequency>/...` directory.
    *   **API:**
        *   `api/.../.py` functions are called.
        *   The Binance API client fetches data (e.g., K-lines, funding rates).
        *   Data is typically saved by `TSManager` into the `api_data/...` directory as Parquet files.

3.  **Verification (AWS Data):**
    *   `aws/.../verify.py` functions compare checksums of downloaded files with `.CHECKSUM` files. Corrupted files may be deleted or reported.

4.  **Parsing (AWS Data):**
    *   `aws/.../parse.py` functions read the raw (verified) zipped CSV files.
    *   Data is converted into Polars DataFrames.
    *   `TSManager` writes these DataFrames into partitioned Parquet files in the `parsed_data/<trade_type>/<data_type>/...` directory.

5.  **Data Generation (primarily K-lines):**
    *   `generate/kline.py` functions are invoked.
    *   `merge_klines` reads parsed AWS data and relevant API data (both from Parquet via `TSManager`).
    *   Data is merged, potentially enriched (VWAP, funding rates), and gaps are handled (scanned, split, filled).
    *   The final processed DataFrames are written as Parquet files to the `results_data/<trade_type>/klines/<time_interval>/...` directory.

6.  **Resampling (Optional):**
    *   `generate/resample.py` functions read generated K-line data (from `results_data`).
    *   Data is resampled to new timeframes.
    *   Resampled DataFrames are written to `results_data/<trade_type>/resampled_klines/<new_interval>/...`.

## 5. Data Storage Strategy

BHDS uses a structured directory layout within the main data directory (`<CRYPTO_BASE_DIR>/binance_data/`):

*   **`aws_data/`:** Stores raw data downloaded from AWS.
    *   Structure: `aws_data/<data_category (e.g., um_futures)> /<frequency (e.g., daily, monthly)>/<data_type (e.g., klines, fundingRate)>/<symbol>/<time_interval (for klines)>/<files.zip>`
    *   Format: ZIP archives containing CSV files, and `.CHECKSUM` files.

*   **`parsed_data/`:** Stores data parsed from `aws_data/`, organized for efficient access.
    *   Structure: `parsed_data/<trade_type>/<data_type (e.g., klines, funding)>/<symbol>/<time_interval (for klines)>/<partitioned_parquet_files>`
    *   Format: Parquet files, typically partitioned by `TSManager` (e.g., monthly).

*   **`api_data/`:** Stores data downloaded directly from the Binance API.
    *   Structure: `api_data/<trade_type>/<data_type (e.g., klines, funding_rate)>/<symbol>/<time_interval (for klines)>/<partitioned_parquet_files>`
    *   Format: Parquet files, managed by `TSManager`.

*   **`results_data/`:** Stores the final, processed datasets ready for use.
    *   Structure: `results_data/<trade_type>/<data_type (e.g., klines, resampled_klines)>/<time_interval_or_resample_rule>/<symbol_or_split_symbol_files.pqt>`
    *   Format: Parquet files.

## 6. Key Technologies & Libraries

*   **Python:** Core programming language.
*   **Typer:** For building the CLI application.
*   **Polars:** High-performance DataFrame library for data manipulation and Parquet I/O.
*   **Aria2:** External command-line utility for fast parallel downloads from AWS.
*   **aiohttp:** Asynchronous HTTP client/server framework (used for API calls and AWS file listing).
*   **asyncio:** Standard Python library for asynchronous programming.
*   **multiprocessing:** Standard Python library for parallel execution of CPU-bound tasks (parsing, generation).
*   **Conda:** For environment and dependency management (via `environment.yml`).

This design promotes modularity and a clear separation of concerns, facilitating maintenance and potential future extensions.
