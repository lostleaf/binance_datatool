# examples/CLAUDE.md

This file is part of the parent CLAUDE.md system. It contains library usage examples and custom workflow documentation for the Binance Historical Data Service (BHDS).

## Overview

The examples directory contains complete working examples demonstrating how to use the BHDS library API for custom data workflows beyond the CLI interface.

## Example Files

### Core Examples
- **`kline_download_task.py`**: Complete library-based download workflow example - Demonstrates AWS data downloading using the library API
- **`cm_futures_holo_gap.py`**: Batch processing example for cm_futures 1m holo klines with gap detection - Demonstrates `generate_all()` and `execute_polars_batch` usage

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

### Holo 1m Kline Generation with Gap Detection
```python
import tempfile
from pathlib import Path
from bdt_common.enums import TradeType
from bdt_common.polars_utils import execute_polars_batch
from bhds.holo_kline.merger import Holo1mKlineMerger
from bhds.holo_kline.gap_detector import HoloKlineGapDetector

# Generate cm_futures 1m holo klines with gap detection
with tempfile.TemporaryDirectory() as temp_dir:
    merger = Holo1mKlineMerger(
        trade_type=TradeType.cm_futures,
        base_dir=Path("/path/to/crypto_data/binance_data/parsed_data"),
        include_vwap=True,
        include_funding=True,
    )
    
    # Generate all symbols
    lazy_frames = merger.generate_all(Path(temp_dir))
    execute_polars_batch(lazy_frames, "Collecting kline data")
    
    # Detect gaps in all generated files
    detector = HoloKlineGapDetector(min_days=1, min_price_chg=0.1)
    generated_files = list(Path(temp_dir).glob("*.parquet"))
    
    gap_tasks = [detector.detect(file_path) for file_path in generated_files]
    gap_results = execute_polars_batch(gap_tasks, "Detecting gaps", return_results=True)
    
    # Process results
    for file_path, gaps_df in zip(generated_files, gap_results):
        if len(gaps_df) > 0:
            symbol = file_path.stem
            print(f"{symbol}: {len(gaps_df)} gaps detected")
```

## Running Examples

### Basic Usage
```bash
# Set HTTP_PROXY environment variable if needs HTTP proxy for downloads

# Run kline download example
uv run python examples/kline_download_task.py /path/to/crypto_data

# Run cm_futures gap detection example
uv run python examples/cm_futures_holo_gap.py

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
