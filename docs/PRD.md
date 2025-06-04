# Product Requirements Document: BHDS (Binance Historical Data Service)

## 1. Introduction

BHDS (Binance Historical Data Service) is an open-source Python-based application designed to download, store, and manage historical market data from the Binance cryptocurrency exchange. Its primary purpose is to provide a reliable and efficient local data source for quantitative trading research, backtesting trading strategies, and market analysis.

**Target Audience:**
*   Cryptocurrency quantitative traders
*   Financial data analysts
*   Researchers studying market dynamics

## 2. Goals

*   To provide a comprehensive local copy of historical Binance market data.
*   To ensure data integrity through verification processes.
*   To offer data in an efficient queryable format (Parquet).
*   To automate the process of downloading, parsing, and updating data.
*   To allow users to fill data gaps using Binance API for more complete datasets.
*   To enable the generation of customized datasets (e.g., resampled timeframes, merged data with funding rates).

## 3. Key Features

*   Download historical market data from Binance's AWS public data repository.
*   Download recent/missing market data from Binance's official API.
*   Verify integrity of downloaded data (e.g., checksums).
*   Parse raw data (CSV) into structured Parquet files.
*   Merge data from different sources (AWS, API).
*   Generate enhanced K-line data (e.g., with VWAP, funding rates).
*   Identify and handle gaps in time-series data.
*   Resample K-line data to different time frequencies.
*   Provide a Command Line Interface (CLI) for all functionalities.

## 4. Data Types Handled

The system primarily handles the following types of market data:

*   **K-lines (Candlesticks):** Open, High, Low, Close, Volume (OHLCV) data for various time intervals (e.g., 1m, 5m, 1h, 1d).
*   **Funding Rates:** For perpetual futures contracts.
*   **Liquidation Data:** Snapshots of liquidation orders from AWS.

## 5. Data Sources

*   **Binance AWS S3 Buckets:** For bulk historical data (K-lines, Funding Rates, Liquidations). URL: [https://data.binance.vision/](https://data.binance.vision/)
*   **Binance Official API:** For recent data, filling gaps in historical data, and fetching data types not available in bulk (e.g., certain K-line intervals or very recent funding rates).

## 6. Functional Requirements

### 6.1. Data Downloading (AWS)
*   FR1.1: System must be able to download K-line data for specified symbols, trade types (Spot, USDⓈ-M Futures, COIN-M Futures), and time intervals from Binance AWS.
*   FR1.2: System must be able to download Funding Rate data for specified symbols and trade types from Binance AWS.
*   FR1.3: System must be able to download Liquidation snapshot data for specified symbols and trade types from Binance AWS.
*   FR1.4: System should allow filtering symbols for download (e.g., by quote currency, contract type).
*   FR1.5: System should utilize Aria2 for efficient downloading from AWS.
*   FR1.6: System should support HTTP proxy for downloads.

### 6.2. Data Downloading (API)
*   FR2.1: System must be able to download K-line data for specified symbols, trade types, time intervals, and specific dates/date ranges from Binance API.
*   FR2.2: System must be able to identify and download K-line data missing from the AWS dataset for specified symbols or entire trade types.
*   FR2.3: System must be able to download recent Funding Rate data for specified symbols or entire trade types from Binance API.
*   FR2.4: System should support HTTP proxy for API requests.

### 6.3. Data Verification
*   FR3.1: System must be able to verify the integrity of downloaded AWS K-line data using checksums provided by Binance.
*   FR3.2: System must be able to verify the integrity of downloaded AWS Funding Rate data.
*   FR3.3: System should identify and report/delete corrupted data files.

### 6.4. Data Parsing
*   FR4.1: System must parse downloaded raw K-line data (CSV format from zip files) into a structured format.
*   FR4.2: System must parse downloaded raw Funding Rate data (CSV format) into a structured format.
*   FR4.3: Parsed data must be stored in Parquet file format.
*   FR4.4: Parsed K-line data should include: candle begin time (UTC), open, high, low, close, volume, quote volume, trade count, taker buy base/quote asset volume.
*   FR4.5: System should allow forcing re-parsing of data even if previously parsed data exists.
*   FR4.6: System should use a time-series manager (`TSManager`) for efficient partitioned storage of parsed data (e.g., monthly Parquet files).

### 6.5. Data Generation & Processing
*   FR5.1: System must be able to merge K-line data obtained from AWS and API sources for a given symbol and time interval.
*   FR5.2: System should allow calculation and inclusion of Volume Weighted Average Price (VWAP) in generated K-line data.
*   FR5.3: System should allow inclusion of Funding Rates into the K-line data for perpetual futures.
*   FR5.4: System must be able to identify gaps in K-line data based on configurable time duration and/or price change thresholds.
*   FR5.5: System should offer an option to split K-line data into separate files based on identified gaps.
*   FR5.6: System must fill missing timestamps within K-line data segments (e.g., by forward-filling or creating empty candles).
*   FR5.7: System must be able to resample K-line data from a base interval (e.g., 1m) to a target interval (e.g., 1h, 4h, 1d) with specified offset.

### 6.6. CLI Operations
*   FR6.1: All major functionalities (download, verify, parse, generate, resample) must be accessible via a CLI.
*   FR6.2: The CLI should provide clear help messages and options for each command.
*   FR6.3: The CLI should support operations on individual symbols or all symbols of a given trade type.

## 7. Data Pipeline Overview

1.  **Download (AWS/API):** Fetch raw data (zip/CSV) using `aws_*` or `api_data` commands.
2.  **Verify (AWS):** Check integrity of downloaded AWS files using `aws_* verify` commands.
3.  **Parse (AWS):** Convert raw AWS CSVs to structured Parquet files using `aws_* parse` commands. API data is typically fetched in a more structured manner and may not require a separate parsing step in the same way.
4.  **Generate K-lines:**
    *   Merge AWS and API K-line data.
    *   Add VWAP and Funding Rates.
    *   Handle gaps (scan, split, fill).
    *   Store as final Parquet K-line series using `generate kline` commands.
5.  **Resample K-lines (Optional):** Create different timeframes from generated K-lines using `generate resample` commands.

## 8. Non-Functional Requirements

*   **NFR1 (Performance):** The system should utilize parallel processing (multiprocessing) for CPU-bound tasks like parsing and data generation to improve performance. Asynchronous operations should be used for I/O-bound tasks like downloading.
*   **NFR2 (Configurability):**
    *   Base data storage directory must be configurable via an environment variable (`CRYPTO_BASE_DIR`).
    *   Number of parallel jobs (`N_JOBS`) must be configurable.
    *   HTTP proxy must be configurable.
    *   Gap detection parameters (min_days, min_price_chg) must be configurable.
*   **NFR3 (Data Storage):** Final processed data should be stored in Parquet format for efficient querying and storage. Raw downloaded data is kept in its original format (zip/CSV).
*   **NFR4 (Dependencies):** Key dependencies include Python, Typer (CLI), Polars (DataFrame manipulation), Aria2 (downloader), aiohttp (async HTTP). Environment managed by Conda (`environment.yml`).
*   **NFR5 (Logging):** The system should provide informative logging for operations, errors, and progress.
*   **NFR6 (Modularity):** The codebase should be organized into logical modules (e.g., aws, api, generate, util).
*   **NFR7 (Extensibility):** The system should be designed to potentially support new data types or exchanges in the future with reasonable effort (though not an immediate requirement).
*   **NFR8 (Data Integrity):** Data timestamps should be consistently handled in UTC.
