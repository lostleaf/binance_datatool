# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Binance Historical Data Service (BHDS) - Python CLI tool for downloading, processing, and maintaining Binance cryptocurrency market data using AWS historical archives and Binance APIs. Outputs optimized Parquet datasets for quantitative research.

**⚠️ UNDER REFACTORING**: Legacy code in `legacy/` is being migrated to modern `src/` structure. New development should use the `src/` layout.

## Quick Start

```bash
# Setup environment
uv venv && source .venv/bin/activate
uv sync
sudo apt install aria2  # or brew install aria2 on macOS

# Method 1: CLI with YAML config
uv run bhds aws-download configs/download_spot_kline.yaml

# Method 2: Library-based approach (see examples/kline_download_task.py)
uv run python examples/kline_download_task.py /path/to/data

# Method 3: Manual workflow with modern tools
uv run python -c "from bhds.aws.downloader import AwsDownloader; ..."
```

## Architecture

### Directory Structure
```
├── legacy/                     # Legacy code (deprecated, moved from scripts/)
├── src/                        # Modern refactored structure
│   ├── bhds/                   # New modular CLI
│   │   ├── cli.py              # CLI entry point (uv run bhds)
│   │   ├── aws/                # AWS client/downloader
│   │   └── config.py
│   └── bdt_common/             # Shared utilities
├── configs/                    # YAML task configurations
├── examples/                   # Library usage examples
├── scripts/                    # Minimal shell scripts (backup only)
├── tests/                      # Test files
└── notebook/                   # Jupyter analysis notebooks
```

### Storage Structure
```
$CRYPTO_BASE_DIR/binance_data/
├── aws_data/           # Raw AWS downloads (.zip)
├── parsed_data/        # Processed AWS data (.parquet)
├── api_data/           # Recent API data (.parquet)
├── results_data/       # Final datasets
│   ├── klines/         # Merged kline data
│   └── resampled_klines/ # Higher timeframes
```

## Environment Setup

### Prerequisites
- Python >=3.12
- [uv](https://docs.astral.sh/uv/) package manager
- aria2 download utility

### Commands
```bash
uv sync              # Install dependencies
uv sync --dev        # Install dev dependencies
uv add package       # Add new package
uv lock && uv sync   # Update lockfile
```

### Configuration
- `CRYPTO_BASE_DIR`: Data storage (default: $HOME/crypto_data)
- `CRYPTO_NJOBS`: Parallel jobs (default: CPU-2)
- `HTTP_PROXY`: HTTP proxy for downloads

## Usage Patterns

### Modern CLI Usage
```bash
# YAML-based task execution (recommended)
uv run bhds aws-download configs/download_spot_kline.yaml

# Available CLI commands
uv run bhds --help                # Show all commands
uv run bhds version               # Show version
uv run bhds aws-download <config> # Download with YAML config
```

### Library-Based Approach (Recommended)
```python
# See examples/kline_download_task.py for complete example
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

### Running Examples
```bash
# Run example with custom data directory
uv run python examples/kline_download_task.py /path/to/crypto_data

# Or set CRYPTO_BASE_DIR environment variable
export CRYPTO_BASE_DIR=/path/to/crypto_data
uv run python examples/kline_download_task.py
```

## Development

### Code Quality
```bash
uv run black . && uv run isort .  # Format code
uv run python tests/aws_downloader.py  # Run specific test
```

### Key Patterns

#### YAML Configuration
```yaml
# configs/download_spot_kline.yaml
data_type: "klines"
trade_type: "spot"
aws_client:
  data_freq: "daily"
  time_interval: "1m"
symbol_filter:
  quote: "USDT"
  stable_pairs: false
```

#### Polars LazyFrame Processing
```python
import polars as pl

pl.scan_parquet(input_path)
  .filter(pl.col("volume") > 0)
  .group_by(pl.col("candle_begin_time").dt.date())
  .agg([pl.col("close"last())])
  .sink_parquet(output_path)
```

#### Logging
```python
# Legacy
from util.log_kit import logger

# New
from bdt_common.log_kit import logger

logger.info("Processing started")
logger.ok("Download completed")
```

## Data Types & Trade Types

### Data Types
- `klines`: Candlestick data (OHLCV)
- `fundingRate`: Funding rates
- `metrics`: Market metrics
- `aggTrades`: Aggregated trades
- `liquidationSnapshot`: Liquidation data

### Trade Types
- `spot`: Spot trading pairs
- `futures/um`: USD-margined futures
- `futures/cm`: Coin-margined futures

## Testing

Test files are in `tests/` directory:
- `aws_client.py`: AWS client tests
- `aws_downloader.py`: Downloader tests
- `infer_exginfo.py`: Exchange info tests
- `checksum.py`: Checksum verification tests
- `symbol_filter.py`: Symbol filtering tests
- `test_parser.py`: Unified CSV parser tests

Run individual tests:
```bash
uv run python tests/aws_downloader.py
uv run python tests/test_parser.py  # Test parser with actual data
```

## Migration Notes

- **Modern approach**: Use `uv run bhds` CLI or library-based workflows
- **Legacy code**: Deprecated scripts moved to `legacy/` directory
- **Current**: New CLI uses YAML configurations and library API
- **Recommendation**: Use examples/kline_download_task.py as template for custom workflows