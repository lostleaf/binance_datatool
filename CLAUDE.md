# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Binance Historical Data Service (BHDS) - Python CLI tool for downloading, processing, and maintaining Binance cryptocurrency market data using AWS historical archives and Binance APIs. Outputs optimized Parquet datasets for quantitative research.

## **⚠️ UNDER REFACTORING**:
Legacy code in `legacy/` is being migrated to modern `src/` structure. 
New development should use the `src/` layout.

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
│   │   │   ├── merger.py       # Holo1mKlineMerger implementation
│   │   │   └── gap_detector.py # Gap detection for kline data
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
│   └── holo_1m_klines/ # Holographic 1-minute klines (with VWAP & funding)
```

## Environment Setup

### Prerequisites
- Python >=3.12
- [uv](https://docs.astral.sh/uv/) package manager
- aria2 download utility, installed via package manager `brew install aria2` or `sudo apt install aria2`

### Commands

```bash
# Setup environment
uv sync
source .venv/bin/activate

# Method 1: CLI with YAML config
uv run bhds aws-download configs/download_spot_kline.yaml

# Method 2: Library-based approach (see examples/kline_download_task.py)
uv run python examples/kline_download_task.py /path/to/data
```

### Configuration
- `CRYPTO_BASE_DIR`: Data storage (default: $HOME/crypto_data)
- `HTTP_PROXY`: HTTP proxy for downloads

## Usage Patterns

### Modern CLI Usage (Recommended)
```bash
# YAML-based task execution (recommended)
uv run bhds aws-download configs/download/spot_kline.yaml
uv run bhds parse-aws-data configs/parsing/spot_kline.yaml

# Other available CLI commands
uv run bhds --help                    # Show all commands
uv run bhds version                   # Show version
```

### Library-Based Approach
See [`examples/CLAUDE.md`](examples/CLAUDE.md) 
for comprehensive library usage examples and custom workflow documentation.

## Development

### Code Quality
```bash
uv run black . && uv run isort .  # Format code
uv run python tests/aws_downloader.py  # Run specific test
```

### Key Patterns

#### YAML Configuration
See [`configs/CLAUDE.md`](configs/CLAUDE.md) for comprehensive YAML configuration documentation and examples.

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

#### Polars Usage Guidelines
1. Use LazyFrame/Lazy API whenever possible
2. Use `from bdt_common.polars_utils import execute_polars_batch` for batch collection of multiple LazyFrames

#### Logging
Use `from bdt_common.log_kit import logger` for all logging. 
See [`tests/log_kit.py`](tests/log_kit.py) for usage examples.

## Core Enums

Core enums are defined in [`src/bdt_common/enums.py`](src/bdt_common/enums.py):
- **TradeType**: `spot`, `futures/um`, `futures/cm`
- **DataType**: `klines`, `fundingRate`, `aggTrades`, `liquidationSnapshot`, `metrics`
- **DataFrequency**: `daily`, `monthly`
- **ContractType**: `PERPETUAL`, `DELIVERY`

## Testing

See [`tests/CLAUDE.md`](tests/CLAUDE.md) for comprehensive testing documentation.

## Migration Notes

- **Modern approach**: Use `uv run bhds` CLI or library-based workflows
- **Legacy code**: Deprecated scripts moved to `legacy/` directory
- **Current**: New CLI uses YAML configurations and library API
- **Architecture**: AWS path building now uses dedicated `path_builder.py` module
- **Recommendation**: Use examples/kline_download_task.py as template for custom workflows

## User Defined Notes

- Always write comments and logs in English, No other languages