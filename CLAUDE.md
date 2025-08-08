# CLAUDE.md

Guidance for Claude Code working with BHDS (Binance Historical Data Service).

## Overview
Python tool for downloading and maintaining Binance historical market data using Aria2 and Polars, stored in Parquet format for quantitative research.

## Setup

### Prerequisites
- Python >=3.12
- uv (Astral's package manager)
- aria2 (download utility)

### Install
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv && source .venv/bin/activate
uv sync
sudo apt install aria2  # Ubuntu/Debian
```

### Config
- `CRYPTO_BASE_DIR`: Data directory (default: $HOME/crypto_data)
- `CRYPTO_NJOBS`: Parallel jobs (default: CPU-2)
- `HTTP_PROXY`: HTTP proxy for downloads

## Architecture

```
bhds.py                 # CLI (Typer)
├── api/                # Real-time Binance API
├── aws/                # AWS historical data
│   ├── kline/          # Candlestick data
│   ├── funding/        # Funding rates
│   └── liquidation/    # Liquidation snapshots
├── generate/           # Data merging/processing
├── util/               # Shared utilities
├── config/             # Constants
└── notebook/           # Jupyter analysis
```

### Data Flow
1. Download AWS data via aria2
2. Verify checksums (SHA256)
3. Parse CSV → Polars DataFrame
4. Merge AWS+API data, handle gaps
5. Resample to higher timeframes

## CLI

### Structure
```bash
python bhds.py [COMMAND] [SUBCOMMAND] [OPTIONS]
```

### Commands
- `aws_kline`: Download/verify/parse k-lines
- `aws_funding`: Download/verify/parse funding rates
- `aws_liquidation`: Download liquidation snapshots
- `api_data`: Download recent API data
- `generate`: Merge data and create datasets

### Workflows

#### Pipeline Scripts
```bash
./aws_download.sh 1m    # Download
./aws_parse.sh          # Parse to parquet
./api_download.sh       # Fill gaps
./gen_kline.sh 1m       # Generate datasets
./resample.sh           # Higher timeframes
./backup_aws_data.sh    # Backup AWS data
```

#### Individual
```bash
python bhds.py aws_kline download-spot 1m
python bhds.py aws_funding download-um-futures
python bhds.py generate kline-type um_futures 1m --split-gaps --with-vwap --with-funding-rates
python bhds.py generate resample-type um_futures 1h 5m
```

## Data Structure

### Trade Types
- `spot`: Spot trading
- `um_futures`: USD-margined futures
- `cm_futures`: Coin-margined futures

### Storage
- **Format**: Parquet
- **Path**: `$CRYPTO_BASE_DIR/binance_data/[trade_type]/[data_type]/[time_interval]/[symbol]/`
- **Partitioning**:
  - K-line: Daily files (YYYYMMDD.parquet)
  - Other: Monthly (YYYYMM)

### Utilities

#### TSManager (util/ts_manager.py)
Monthly partitioned storage for non-kline data:
- `read_partition()`, `write_partition()`, `update()`, `read_all()`

#### K-line Storage
Daily file-based:
- Input: Daily CSV zips from AWS/API
- Output: YYYYMMDD.parquet
- Processing: Incremental (missing dates only)

#### Logging (util/log_kit.py)
- `logger.debug()`, `logger.info()`, `logger.ok()`, `logger.warning()`, `logger.error()`

## Development

### Quality
```bash
black . && isort .
uv run python -m mypy .
```

### Dependencies
```bash
uv sync --dev        # Dev deps
uv add package       # Add package
uv lock && uv sync   # Update
```

## Patterns

### Async
AWS operations use asyncio with aiohttp.

### Pipeline
1. Download ZIP with aria2
2. Verify SHA256 checksum
3. CSV → Polars DataFrame
4. Clean/enhance data
5. Save partitioned Parquet
6. Resample with LazyFrame streaming

### LazyFrame
- `pl.scan_parquet()` + `sink_parquet(lazy=True)` for streaming
- Single-process execution (no multiprocessing)
- Optional tqdm progress

### Gap Detection
- Time gaps > threshold days
- Price changes > threshold %

### Features
- VWAP (volume-weighted average price)
- Funding rates for perpetuals
- Gap splitting for continuous periods