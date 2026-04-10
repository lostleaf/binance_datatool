# Module Reference

This document describes each package and module in `binance-datatool`, its responsibilities, and
its public API surface. All import paths are relative to `binance_datatool`.

## Package Tree

```
src/binance_datatool/
├── __init__.py              # Package root — exports __version__
├── py.typed                 # PEP 561 type stub marker
│
├── common/                  # Shared types and constants
│   ├── __init__.py          # Re-exports public symbols
│   ├── constants.py         # S3_LISTING_PREFIX, S3_HTTP_TIMEOUT_SECONDS
│   └── enums.py             # TradeType, DataFrequency, DataType
│
└── bhds/                    # Binance Historical Data Service
    ├── __init__.py
    │
    ├── archive/             # S3 data access
    │   ├── __init__.py      # Re-exports ArchiveClient, list_symbols
    │   └── client.py        # HTTP client and XML parsing
    │
    ├── workflow/            # Business logic orchestration
    │   ├── __init__.py
    │   └── archive.py       # ArchiveListSymbolsWorkflow
    │
    └── cli/                 # Typer CLI layer
        ├── __init__.py      # App definition and sub-command registration
        └── archive.py       # list-symbols command
```

---

## `binance_datatool`

The root package. Exports only version metadata.

| Symbol | Type | Description |
|--------|------|-------------|
| `__version__` | `str` | Semantic version string (currently `"0.1.0"`). |

---

## `binance_datatool.common`

Shared types and constants imported by all other packages. The `__init__.py` re-exports every
public symbol so callers can write `from binance_datatool.common import TradeType`.

### `common.constants`

| Constant | Type | Value | Description |
|----------|------|-------|-------------|
| `S3_LISTING_PREFIX` | `str` | `"https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"` | Base URL for the S3 listing endpoint. |
| `S3_HTTP_TIMEOUT_SECONDS` | `int` | `15` | Default timeout per HTTP request. |

### `common.enums`

**`TradeType(StrEnum)`** — Binance market segment.

| Member | Value | `s3_path` | Description |
|--------|-------|-----------|-------------|
| `spot` | `"spot"` | `"spot"` | Spot trading market. |
| `um` | `"um"` | `"futures/um"` | USD-M perpetual and delivery futures. |
| `cm` | `"cm"` | `"futures/cm"` | COIN-M perpetual and delivery futures. |

**`DataFrequency(StrEnum)`** — Archive partition frequency.

| Member | Value | Description |
|--------|-------|-------------|
| `daily` | `"daily"` | Files partitioned by day. |
| `monthly` | `"monthly"` | Files partitioned by month. |

**`DataType(StrEnum)`** — Dataset type on data.binance.vision.

| Member | Value |
|--------|-------|
| `klines` | `"klines"` |
| `agg_trades` | `"aggTrades"` |
| `trades` | `"trades"` |
| `funding_rate` | `"fundingRate"` |
| `book_depth` | `"bookDepth"` |
| `book_ticker` | `"bookTicker"` |
| `index_price_klines` | `"indexPriceKlines"` |
| `mark_price_klines` | `"markPriceKlines"` |
| `premium_index_klines` | `"premiumIndexKlines"` |
| `metrics` | `"metrics"` |
| `liquidation_snapshot` | `"liquidationSnapshot"` |

> Member names use `snake_case`; values match the S3 path segment exactly.

---

## `binance_datatool.bhds.archive`

S3 listing client for data.binance.vision.

### `ArchiveClient`

The main class for communicating with the Binance public data archive.

```python
from binance_datatool.bhds.archive import ArchiveClient

client = ArchiveClient(timeout_seconds=15, trust_env=True)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `timeout_seconds` | `15` | Total timeout in seconds per HTTP request. |
| `trust_env` | `True` | Read proxy settings from environment variables (`http_proxy`, etc.). |

**Public methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `list_dir` | `async (session, prefix) → list[str]` | List child prefixes under an S3 prefix, handling pagination automatically. |
| `list_symbols` | `async (trade_type, data_freq, data_type) → list[str]` | List sorted symbol names for a given archive path. Creates its own session internally. |

### `list_symbols` (module-level convenience function)

```python
from binance_datatool.bhds.archive import list_symbols

symbols = await list_symbols(TradeType.spot, DataFrequency.daily, DataType.klines)
```

Creates a temporary `ArchiveClient` and delegates to `ArchiveClient.list_symbols`.

### Private Helpers

These are internal implementation details, listed here for maintainer reference.

| Function | Purpose |
|----------|---------|
| `_build_prefix` | Construct S3 key prefix from enum parameters. |
| `_build_listing_url` | Build full S3 listing URL with query parameters. |
| `_normalize_entries` | Handle xmltodict single-element dict/list ambiguity. |
| `_extract_prefixes_from_payload` | Extract `CommonPrefixes` entries from parsed XML. |
| `_is_truncated` | Check whether the S3 response indicates more pages. |
| `_next_marker` | Determine the marker for the next pagination request. |
| `_extract_symbol` | Extract symbol name from a trailing S3 prefix path. |

---

## `binance_datatool.bhds.workflow`

Business logic orchestration layer between the CLI and the archive client.

### `ArchiveListSymbolsWorkflow`

```python
from binance_datatool.bhds.workflow.archive import ArchiveListSymbolsWorkflow

workflow = ArchiveListSymbolsWorkflow(
    trade_type=TradeType.spot,
    data_freq=DataFrequency.daily,
    data_type=DataType.klines,
    client=None,  # uses default ArchiveClient
)
symbols: list[str] = await workflow.run()
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_type` | *(required)* | Market segment to query. |
| `data_freq` | *(required)* | Partition frequency. |
| `data_type` | *(required)* | Dataset type. |
| `client` | `None` | Optional pre-configured `ArchiveClient`. |

---

## `binance_datatool.bhds.cli`

Typer CLI application. The entry point is `binance_datatool.bhds.cli:app`, exposed as the `bhds`
console script via `pyproject.toml`.

### App Structure

```
bhds                          # Root Typer app
└── archive                   # Sub-command group
    └── list-symbols          # Command
```

### `archive list-symbols`

```
bhds archive list-symbols <TRADE_TYPE> [--freq FREQ] [--type TYPE]
```

| Argument / Option | Type | Default | Description |
|-------------------|------|---------|-------------|
| `TRADE_TYPE` | `TradeType` | *(required)* | Market segment (positional argument). |
| `--freq` | `DataFrequency` | `daily` | Partition frequency. |
| `--type` | `DataType` | `klines` | Dataset type. |

**Output:** one symbol name per line to stdout.
