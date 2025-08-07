# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**BHDS (Binance Historical Data Service)** is a Python-based tool for downloading and maintaining historical market data from Binance. It uses Aria2 for downloading from Binance's AWS repository and Polars for data processing, storing data in Parquet format for quantitative trading research.

## Environment Setup

### Prerequisites
- **Python**: >=3.12
- **Package Manager**: uv (Astral's Python package manager)
- **Downloader**: aria2 (cross-platform command-line download utility)

### Quick Setup
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
uv sync

# Install aria2 (Ubuntu/Debian)
sudo apt update && sudo apt install aria2
```

### Configuration
- **Data Directory**: Defaults to `$HOME/crypto_data`
- **Custom Directory**: Set `CRYPTO_BASE_DIR` environment variable
- **Parallel Jobs**: Set `CRYPTO_NJOBS` (defaults to CPU count - 2)
- **HTTP Proxy**: Set `HTTP_PROXY` or `http_proxy` for downloads

## Architecture

### Core Components

```
bhds.py                 # CLI entry point (Typer-based)
├── api/                # Real-time Binance API data fetching
├── aws/                # AWS historical data processing
│   ├── kline/          # K-line (candlestick) data
│   ├── funding/        # Funding rates data
│   └── liquidation/    # Liquidation snapshots
├── generate/           # Data merging and processing
├── util/               # Shared utilities
├── config/             # Configuration and constants
└── notebook/           # Jupyter notebooks for analysis
```

### Data Flow
1. **Download**: Fetch historical data from Binance AWS using aria2
2. **Verify**: Check checksums and validate downloaded files
3. **Parse**: Convert raw CSV data to Polars DataFrames in Parquet format
4. **Generate**: Merge AWS and API data, handle gaps, add features (VWAP, funding rates)
5. **Resample**: Create higher timeframe data from 1-minute base data

## CLI Commands

### Main CLI Structure
```bash
python bhds.py [COMMAND] [SUBCOMMAND] [OPTIONS]
```

### Command Groups
- `aws_kline`: Download/verify/parse K-line data from AWS
- `aws_funding`: Download/verify/parse funding rate data
- `aws_liquidation`: Download liquidation snapshot data
- `api_data`: Download recent data from Binance API
- `generate`: Merge data and create final datasets

### Common Workflows

#### Full Data Pipeline (via shell scripts)
```bash
# Sequential workflow
./aws_download.sh 1m    # Download all data
./aws_parse.sh          # Parse to parquet
./api_download.sh       # Fill recent gaps
./gen_kline.sh 1m       # Generate merged datasets
./resample.sh           # Create higher timeframes (optional)
```

#### Individual Commands
```bash
# Download spot 1-minute klines for USDT pairs
python bhds.py aws_kline download-spot 1m --quote USDT

# Download USD-M futures funding rates
python bhds.py aws_funding download-um-futures

# Generate merged kline data with VWAP and funding rates
python bhds.py generate kline-type um_futures 1m --split-gaps --with-vwap --with-funding-rates

# Resample to 1-hour data with 5-minute offset
python bhds.py generate resample-type um_futures 1h 5m
```

## Data Structure

### Trade Types
- `spot`: Spot trading pairs
- `um_futures`: USD-Margined futures (USDT/USDC settled)
- `cm_futures`: Coin-Margined futures (crypto settled)

### Storage Format
- **Format**: Parquet files with monthly partitioning
- **Location**: `$CRYPTO_BASE_DIR/binance_data/[trade_type]/[data_type]/[time_interval]/[symbol]/`
- **Partitioning**: Monthly (YYYYMM) by default
- **Columns**: Standard OHLCV + additional computed features

### Key Utilities

#### TSManager (util/ts_manager.py)
Time-series data manager for partitioned storage:
- `read_partition()`: Read specific month/year
- `write_partition()`: Write data to partition
- `update()`: Update partitions with new data
- `read_all()`: Read and merge all partitions

#### Logging (util/log_kit.py)
Custom logging with colors and emojis:
- `logger.debug()`: Plain output
- `logger.info()`: Blue with spinner
- `logger.ok()`: Green checkmark for success
- `logger.warning()`: Yellow bell
- `logger.error()`: Red X
- `divider()`: Section separators with timestamps

## Development Commands

### Code Quality
```bash
# Format code
black .
isort .

# Run type checking (if configured)
python -m mypy .

# Run tests (if configured)
pytest
```

### Environment Management
```bash
# Install dev dependencies
uv sync --dev

# Update dependencies
uv lock
uv sync

# Add new dependency
uv add package_name
uv add --dev dev_package_name
```

## Key Patterns

### Async Operations
Most AWS operations use asyncio for concurrent downloads via `aiohttp`.

### Data Processing Pipeline
1. Download raw ZIP files with aria2
2. Verify checksums (SHA256)
3. Parse CSV → Polars DataFrame
4. Clean and enhance data
5. Save as partitioned Parquet

### Gap Detection
When generating merged datasets, the system detects gaps based on:
- Time gaps > minimum days threshold
- Price changes > minimum percentage threshold

### Feature Engineering
- **VWAP**: Volume-weighted average price
- **Funding Rates**: For perpetual futures contracts
- **Gap Splitting**: Separate continuous trading periods