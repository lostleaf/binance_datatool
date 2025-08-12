# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Binance Historical Data Service (BHDS) - Python CLI tool for downloading, processing, and maintaining Binance cryptocurrency market data using AWS historical archives and Binance APIs. Outputs optimized Parquet datasets for quantitative research.

**⚠️ UNDER REFACTORING**: This project is currently being refactored from a legacy structure to a modern `src/` layout. All code under `legacy/` will eventually be moved to `src/`. The new structure includes:
- `src/bhds/` - New modular CLI (in development)
- `src/bdt_common/` - Shared utilities (modernized)
- `legacy/` - Legacy code (still functional, to be migrated)

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
├── legacy/                    # Legacy CLI implementation (primary, to be migrated)
│   ├── bhds.py               # Main CLI entry point
│   ├── api/                  # Real-time Binance API
│   ├── aws/                  # AWS historical data
│   ├── generate/             # Data processing
│   └── util/                 # Shared utilities
├── src/                      # New refactored structure
│   ├── bhds/                 # New modular CLI (in development)
│   │   ├── cli.py            # New CLI entry
│   │   ├── aws/              # AWS client/downloader
│   │   └── config.py
│   └── bdt_common/           # Shared utilities (modernized)
├── scripts/                  # Shell scripts for workflows
├── tests/                    # Test files
└── notebook/                 # Jupyter notebooks
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
./scripts/aws_download.sh 1m      # Download AWS historical data
./scripts/aws_parse.sh            # Parse to parquet
./scripts/api_download.sh         # Download recent API data
./scripts/gen_kline.sh 1m         # Generate merged datasets
./scripts/resample.sh             # Create higher timeframes
```

## Development

### Migration Status
- **Legacy structure**: All functional code currently in `legacy/` directory
- **New structure**: Modular code being developed in `src/` directory
- **Migration target**: Move all legacy components to `src/bhds/` with improved modularity

### Code Quality
```bash
# Format code
uv run black . && uv run isort .

# Type checking
uv run python -m mypy .

# Run tests
uv run python tests/aws_downloader.py
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

#### Logging
```python
# Legacy (from legacy/util/)
from util.log_kit import logger

# New (from src/bdt_common/)
from bdt_common.log_kit import logger

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