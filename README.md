# Binance Datatool

Binance Datatool is an open-source project for cryptocurrency quantitative trading research, featuring **BHDS** (Binance Historical Data Service) as its core service.

BHDS efficiently downloads and maintains historical market data from Binance using [Aria2](https://aria2.github.io/) for parallel downloads from [Binance's AWS repository](https://data.binance.vision/). Data is processed with [Polars](https://pola.rs/) and stored in Parquet format for optimal quantitative research workflows.

The project uses [src layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/) with two main packages: 
- `bhds`: CLI and core services
- `bdt_common`: shared utilities

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture.

This project is released under the **MIT License**.

## Environment Setup

### Prerequisites

- **Python ≥ 3.12** (required for modern type hints and performance optimizations)
- **[uv](https://docs.astral.sh/uv/)** for fast Python package management
- **[aria2](https://aria2.github.io/)** for efficient parallel downloads from Binance AWS

### Quick Setup

```bash
# Setup project environment
uv sync && source .venv/bin/activate

# Install aria2
sudo apt install aria2  # Ubuntu/Debian
# brew install aria2    # macOS
```

### Environment Variables

Optional configuration for advanced users:

```bash
# Base Crypto data storage directory (default: ~/crypto_data)
export CRYPTO_BASE_DIR="/path/to/your/crypto/data"

# HTTP proxy if needed
export HTTP_PROXY="http://127.0.0.1:7893"
```

## Usage

BHDS provides two interfaces for quantitative traders. **CLI interface is recommended for most use cases**.

### 1. CLI Interface (Recommended)

Built with [Typer](https://typer.tiangolo.com/), the CLI provides a streamlined workflow using YAML configurations:

```bash
# Show available commands
uv run bhds --help

# Download historical data
uv run bhds aws-download configs/download/spot_kline.yaml

# Parse downloaded CSV to Parquet
uv run bhds parse-aws-data configs/parsing/spot_kline.yaml

# Generate holistic 1m klines
uv run bhds holo-1m-kline configs/holo_1m/spot.yaml

# Resample to higher timeframes
uv run bhds resample configs/resample/spot.yaml
```
See [configs/](configs/) for YAML configuration templates.

### 2. Library Interface (Advanced)

For advanced users requiring programmatic access and custom workflows:

``` python
import asyncio
import os

from bdt_common.enums import DataFrequency, TradeType
from bdt_common.network import create_aiohttp_session
from bhds.aws.client import AwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder

async def main():
    async with create_aiohttp_session(5) as session:
        http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        path_builder = AwsKlinePathBuilder(trade_type=TradeType.spot, data_freq=DataFrequency.daily, time_interval="1m")
        client = AwsClient(path_builder=path_builder, session=session, http_proxy=http_proxy)
        symbols = await client.list_symbols()
        print('First 5 symbols of spot daily 1m kline:', symbols[:5])

asyncio.run(main())
```

See [examples/](examples/) for complete usage patterns including data download and processing workflows.
