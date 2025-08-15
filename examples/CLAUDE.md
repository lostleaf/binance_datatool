# examples/CLAUDE.md

This file is part of the parent CLAUDE.md system. It contains library usage examples and custom workflow documentation for the Binance Historical Data Service (BHDS).

## Overview

The examples directory contains complete working examples demonstrating how to use the BHDS library API for custom data workflows beyond the CLI interface.

## Example Files

### Core Examples
- **`kline_download_task.py`**: Complete library-based download workflow example - Demonstrates AWS data downloading using the library API

## Library Usage Patterns

### AWS Data Download
```python
from bdt_common.enums import DataFrequency, TradeType
from bdt_common.symbol_filter import SpotFilter
from bhds.aws.client import AwsKlineClient
from bhds.aws.downloader import AwsDownloader

# Custom workflow using library API
async def custom_download():
    client = AwsKlineClient(
        trade_type=TradeType.spot,
        data_freq=DataFrequency.daily,
        time_interval="1m"
    )
    downloader = AwsDownloader(local_dir="/path/to/data")
    # ... custom logic
```

### Local File Management
```python
from pathlib import Path
from bdt_common.enums import DataFrequency, TradeType
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder

# Manage local downloaded data
path_builder = AwsKlinePathBuilder(
    trade_type=TradeType.spot,
    data_freq=DataFrequency.daily,
    time_interval="1m"
)
local_client = LocalAwsClient(
    base_dir=Path("/path/to/crypto_data"),
    path_builder=path_builder
)

# Get verification status
symbols = local_client.list_symbols()
status = local_client.get_all_symbols_status()
summary = local_client.get_summary()
```

## Running Examples

### Basic Usage
```bash
# Set HTTP_PROXY environment variable if needs HTTP proxy for downloads

# Run example with custom data directory
uv run python examples/kline_download_task.py /path/to/crypto_data

# Or set CRYPTO_BASE_DIR environment variable (defaults to `~/crypto_data`)
export CRYPTO_BASE_DIR=/path/to/crypto_data
uv run python examples/kline_download_task.py
```

### Environment Setup
Ensure your environment is properly configured:
```bash
# Setup environment
uv sync
source .venv/bin/activate
