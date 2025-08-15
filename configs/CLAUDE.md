# configs/CLAUDE.md

This file is part of the parent CLAUDE.md system. It contains YAML configuration documentation for the Binance Historical Data Service (BHDS).

## Overview

YAML configuration files define data processing workflows in `configs/download/` and `configs/parsing/` directories.

## Directory Structure

```
configs/
├── download/           # Download task configurations
│   ├── spot_kline.yaml
│   ├── um_kline.yaml
│   ├── cm_kline.yaml
│   ├── um_funding.yaml
│   ├── cm_funding.yaml
│   └── um_metrics.yaml
├── parsing/            # Parsing task configurations
│   ├── spot_kline.yaml
│   ├── um_kline.yaml
│   ├── cm_kline.yaml
│   ├── um_funding.yaml
│   └── cm_funding.yaml
└── holo_1m/            # Holo 1-minute kline synthesis configurations
    ├── spot.yaml       # Spot trading holographic 1m klines
    ├── um.yaml         # USDⓈ-margined futures 1m klines
    └── cm.yaml         # Coin-margined futures 1m klines
```

## Configuration Examples

### Download Task
```yaml
data_type: "klines"
trade_type: "spot"
aws_client:
  data_freq: "daily"
  time_interval: "1m"
symbol_filter:
  quote: "USDT"
  stable_pairs: false
```

### Parsing Task
```yaml
data_type: "klines"
trade_type: "futures/um"
data_freq: "daily"
time_interval: "1m"
enable_completion: true
force_update: false
```

### Holo 1m K-line Synthesis Task
```yaml
# USDⓈ-margined futures 1m klines
trade_type: "futures/um"
features:
  include_vwap: true
  include_funding: true
symbol_filter:
  quote: "USDT"
  stable_pairs: false
  contract_type: "PERPETUAL"
gap_detection:
  min_days: 1
  min_price_change: 0.1
```

## Configuration Reference

### Core Fields
As defined in [`src/bdt_common/enums.py`](src/bdt_common/enums.py):
- `data_type`: `klines`, `fundingRate`, `aggTrades`, `liquidationSnapshot`, `metrics`
- `trade_type`: `spot`, `futures/um`, `futures/cm`
- `data_freq`: `daily`, `monthly`

### Kline Specific
For klines only:
- `time_interval`: `1m`, `5m`, `1h`, `1d`

### Symbol Filtering
- `quote`: Quote currency filter (`USDT`, `USDC`, `BTC`, `ETH`)
- `stable_pairs`: Include/exclude stablecoin pairs (boolean)
- `contract_type`: `PERPETUAL` or `DELIVERY` (futures only)
- `leverage_tokens`: Include/exclude leverage tokens (spot only)

### Environment Variables
- `CRYPTO_BASE_DIR`: Base directory (defaults to `~/crypto_data`)
- `HTTP_PROXY`: HTTP proxy for downloads

## Usage

```bash
# Download
uv run bhds aws-download configs/download/spot_kline.yaml

# Parse
uv run bhds parse-aws-data configs/parsing/um_kline.yaml

# Generate Holo 1m K-lines
uv run bhds holo-1m-kline configs/holo_1m/cm.yaml
```