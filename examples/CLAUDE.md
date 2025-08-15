# examples/CLAUDE.md

This file is part of the parent CLAUDE.md system. It contains library usage examples and custom workflow documentation for the Binance Historical Data Service (BHDS).

## Overview

The examples directory contains complete working examples demonstrating how to use the BHDS library API for custom data workflows beyond the CLI interface.

## Example Files

### Core Examples
- **`kline_download_task.py`**: Complete library-based download workflow example - Demonstrates AWS data downloading using the library API
- **`cm_futures_holo.py`**: Batch processing example for cm_futures 1m holo klines with gap detection - Demonstrates `generate_all()` and `execute_polars_batch` usage

## Example Usage Patterns

### AWS Data Download (`kline_download_task.py`)
Uses `AwsKlineClient`, `AwsDownloader`, and `ChecksumVerifier` to download spot 1m klines for USDT symbols with verification.

### Local File Management
Uses `LocalAwsClient` and `AwsKlinePathBuilder` to manage and verify downloaded AWS data files.

### Holo 1m Kline Processing (`cm_futures_holo.py`)
Uses `Holo1mKlineMerger`, `HoloKlineGapDetector`, and `HoloKlineSplitter` for:
- Batch generation of cm_futures 1m holo klines with VWAP and funding rates
- Gap detection across all generated files
- Automatic splitting of kline data based on detected gaps
- Summary statistics of gaps and splits
