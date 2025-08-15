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
│   │   │   ├── local.py        # Local file management (LocalAwsClient)
│   │   │   ├── checksum.py     # Checksum verification utilities
│   │   │   └── ...             # Other AWS modules
│   │   ├── holo_kline/         # Holographic 1-minute kline synthesis
│   │   │   └── merger.py       # Holo1mKlineMerger implementation
│   │   └── tasks/              # Task implementations
│   └── bdt_common/             # Shared utilities
├── configs/                    # YAML task configurations
│   ├── download/               # Download task configurations
│   └── parsing/                # Parsing task configurations
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
├── results_data/       # Final datasets
│   ├── klines/         # Merged kline data
│   ├── resampled_klines/ # Higher timeframes
│   └── holo_1m_klines/ # Holographic 1-minute klines (with VWAP & funding)
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
- `HTTP_PROXY`: HTTP proxy for downloads

## Usage Patterns

### Modern CLI Usage
```bash
# YAML-based task execution (recommended)
uv run bhds aws-download configs/download/spot_kline.yaml
uv run bhds parse-aws-data configs/parsing/spot_kline.yaml

# Available CLI commands
uv run bhds --help                    # Show all commands
uv run bhds version                   # Show version
uv run bhds aws-download <config>     # Download with YAML config
uv run bhds parse-aws-data <config>   # Parse AWS data to Parquet
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

# Local file management (offline analysis)
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
# configs/download/spot_kline.yaml
data_type: "klines"
trade_type: "spot"
aws_client:
  data_freq: "daily"
  time_interval: "1m"
symbol_filter:
  quote: "USDT"
  stable_pairs: false

# configs/parsing/spot_kline.yaml
data_type: "klines"
trade_type: "spot"
data_freq: "daily"
time_interval: "1m"
enable_completion: true
force_update: false

# configs/parsing/um_funding.yaml
data_type: "fundingRate"
trade_type: "um_futures"
data_freq: "monthly"
enable_completion: true
force_update: false
```

#### Data Completion API
```python
from bdt_common.enums import TradeType, ContractType
from bdt_common.rest_api.fetcher import BinanceFetcher
from bhds.api.completion.detector import create_detector
from bhds.api.completion.executor import DataExecutor

# Factory-based detector creation
detector = create_detector(
    data_type=DataType.kline,
    trade_type=TradeType.spot,
    base_dir="/path/to/data",
    interval="1m"
)

# Detect missing data
tasks = detector.detect(symbols=["BTCUSDT", "ETHUSDT"])

# Execute completion via DataExecutor
executor = DataExecutor(fetcher)
result = await executor.execute(tasks)
```

#### Holo1mKlineMerger (Holographic 1-Minute Kline)
```python
from bdt_common.enums import TradeType
from bhds.holo_kline.merger import Holo1mKlineMerger

# Generate holographic 1-minute klines with VWAP and funding rates
merger = Holo1mKlineMerger(
    trade_type=TradeType.um_futures,  # spot, um_futures, cm_futures
    base_dir=Path("/data/parsed"),
    include_vwap=True,      # Add volume-weighted average price
    include_funding=True    # Add funding rates (futures only)
)

# Single symbol processing
ldf = merger.generate("BTCUSDT", Path("output/BTCUSDT.parquet"))

# Batch processing for all symbols
results = merger.generate_all(Path("output/holo_1m_klines/"))
```

#### Polars LazyFrame Processing
```python
import polars as pl

pl.scan_parquet(input_path)
  .filter(pl.col("volume") > 0)
  .group_by(pl.col("candle_begin_time").dt.date())
  .agg([pl.col("close").last()])
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
- `futures/um`: USDⓈ-margined futures
- `futures/cm`: Coin-margined futures

## Testing

Test files are in `tests/` directory:
- `aws_client.py`: AWS client tests
- `aws_downloader.py`: Downloader tests
- `local_aws_client.py`: Local file management tests
- `infer_exginfo.py`: Exchange info tests
- `checksum.py`: Checksum verification tests
- `symbol_filter.py`: Symbol filtering tests
- `parser.py`: Unified CSV parser tests
- `kline_comp.py`: Kline detector + DataExecutor integration tests
- `funding_comp.py`: Funding rate detector + DataExecutor integration tests
- `holo_merger.py`: Holographic 1-minute kline synthesis tests

Run individual tests:
```bash
uv run python tests/aws_downloader.py
uv run python tests/local_aws_client.py  # Test local file management
uv run python tests/parser.py  # Test parser with actual data
uv run python tests/kline_comp.py  # Test kline detector + executor
uv run python tests/funding_comp.py  # Test funding detector + executor
uv run python tests/holo_merger.py  # Test holographic kline synthesis
```

## Migration Notes

- **Modern approach**: Use `uv run bhds` CLI or library-based workflows
- **Legacy code**: Deprecated scripts moved to `legacy/` directory
- **Current**: New CLI uses YAML configurations and library API
- **Architecture**: AWS path building now uses dedicated `path_builder.py` module
- **Recommendation**: Use examples/kline_download_task.py as template for custom workflows

## User Defined Notes

- Always write comments and logs in English, No other languages