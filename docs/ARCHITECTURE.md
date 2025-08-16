# Architecture

This document summarizes the main modules and data layout used by **BHDS**.

## Directory Structure
```
├── legacy/                     # Legacy code (deprecated, moved from scripts/)
├── src/                        # Modern refactored structure
│   ├── bhds/                   # New modular CLI and task implementations
│   │   ├── cli.py              # CLI entry point (`uv run bhds`)
│   │   ├── aws/                # AWS client and download helpers
│   │   ├── holo_kline/         # Holographic 1‑minute kline synthesis
│   │   └── tasks/              # Task implementations
│   └── bdt_common/             # Shared utilities (logging, network, Polars helpers)
├── configs/                    # YAML task configurations
├── examples/                   # Library usage examples
├── scripts/                    # Minimal shell scripts (backup only)
├── tests/                      # Test files and executable scripts
└── notebook/                   # Jupyter analysis notebooks
```

## Storage Structure
```
$CRYPTO_BASE_DIR/binance_data/
├── aws_data/           # Raw AWS downloads (.zip)
├── parsed_data/        # Processed AWS data (.parquet)
├── results_data/       # Final datasets
│   └── holo_1m_klines/ # Holographic 1‑minute klines (with VWAP & funding)
```
