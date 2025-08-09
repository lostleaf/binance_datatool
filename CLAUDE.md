# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Binance Historical Data Service (BHDS) - Python CLI tool for downloading, processing, and maintaining Binance cryptocurrency market data using AWS historical archives and Binance APIs. Outputs optimized Parquet datasets for quantitative research.

## Environment Setup

### Prerequisites
- Python >=3.12
- [uv](https://docs.astral.sh/uv/) package manager
- aria2 download utility

### Installation
```bash
# Install uv if needed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Setup environment
uv venv && source .venv/bin/activate
uv sync

# Install aria2 (Ubuntu/Debian)
sudo apt install aria2

# Or macOS
brew install aria2
```

### Configuration
- `CRYPTO_BASE_DIR`: Data storage (default: $HOME/crypto_data)
- `CRYPTO_NJOBS`: Parallel jobs (default: CPU-2)
- `HTTP_PROXY`: HTTP proxy for downloads

## Architecture

### Directory Structure
```
bhds.py                 # CLI entry (Typer)
api/                   # Real-time Binance API
├── binance.py         # API client
├── kline.py          # K-line endpoints
└── funding.py        # Funding rate endpoints

aws/                   # AWS historical data
├── kline/            # Candlestick data
│   ├── download.py   # AWS S3 downloads
│   ├── parse.py      # CSV → Parquet
│   └── verify.py     # Checksum validation
├── funding/          # Funding rates
└── liquidation/      # Liquidation snapshots

generate/             # Data processing
├── kline.py          # Dataset generation
├── resample.py       # LazyFrame resampling
├── kline_gaps.py     # Gap detection
└── merge.py          # AWS+API merging

util/                 # Shared utilities
├── log_kit.py        # Logging
├── time.py           # Time handling
└── network.py        # HTTP utilities
```

### Data Flow
```
AWS S3 → Download → Parse → Merge → Generate → Resample → Results
API    → Download → Parse →  ↑
```

### Storage Structure
```
$CRYPTO_BASE_DIR/binance_data/
├── aws_data/           # Raw AWS downloads
├── parsed_data/        # Processed AWS data
├── api_data/           # Recent API data
├── results_data/       # Final datasets
│   ├── klines/         # Merged kline data
│   └── resampled_klines/ # Higher timeframes
```

## Key Commands

### AWS Data Management
```bash
# K-lines
python bhds.py aws_kline download-spot 1m
python bhds.py aws_kline verify-type-all um_futures 1m
python bhds.py aws_kline parse-type-all cm_futures 1m

# Funding rates
python bhds.py aws_funding download-um-futures --quote USDT
python bhds.py aws_funding verify-type-all um_futures
```

### API Data Management
```bash
python bhds.py api_data download-kline-spot BTCUSDT 1m
python bhds.py api_data download-funding-um-futures BTCUSDT
```

### Dataset Generation
```bash
# Single symbol
python bhds.py generate kline BTCUSDT um_futures 1m --with-vwap --with-funding-rates

# All symbols of type
python bhds.py generate kline-type um_futures 1m --split-gaps

# Resample
python bhds.py generate resample-type um_futures 1m 5m
```

### Complete Pipeline
```bash
./aws_download.sh 1m      # Download AWS historical data
./aws_parse.sh            # Parse to parquet
./api_download.sh         # Download recent API data
./gen_kline.sh 1m         # Generate merged datasets
./resample.sh             # Create higher timeframes
```

## Development

### Code Quality
```bash
black . && isort .
uv run python -m mypy .
```

### Dependencies
```bash
uv sync --dev        # Install dev dependencies
uv add package       # Add new package
uv lock && uv sync   # Update lockfile
```

### Common Patterns

#### LazyFrame Processing
```python
import polars as pl

# Efficient processing with streaming
pl.scan_parquet(input_path)
  .filter(pl.col("volume") > 0)
  .group_by(pl.col("candle_begin_time").dt.date())
  .agg([pl.col("close").last()])
  .sink_parquet(output_path)
```

#### Time Handling
```python
from util.time import TimeUtil

# Convert timestamps
ts = TimeUtil.convert_ms_to_datetime(ms_timestamp)
start_date = TimeUtil.get_start_date(days_back=30)
```

#### Logging
```python
from util.log_kit import logger

logger.info("Processing started")
logger.ok("Download completed")
logger.warning("Missing data detected")
logger.error("Validation failed")
```

### Trade Types
- `spot`: Spot trading pairs
- `um_futures`: USD-margined futures
- `cm_futures`: Coin-margined futures

### Data Formats
- **Input**: CSV (from AWS), JSON (from API)
- **Output**: Parquet with optimized compression
- **Partitioning**: Daily files (YYYYMMDD.parquet)
- **Columns**: OHLCV + enhanced fields (VWAP, funding rates)

## Performance Tips

- Use `CRYPTO_NJOBS` to control parallelism
- Leverage LazyFrame for large datasets
- Monitor memory usage during resampling
- Use gap splitting for continuous periods
- Enable HTTP proxy for better download speeds