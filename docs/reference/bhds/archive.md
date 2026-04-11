# binance_datatool.bhds.archive

S3 listing client for data.binance.vision.

## `ArchiveClient`

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
| `list_dir` | `async (session, prefix) -> list[str]` | List child prefixes under an S3 prefix, handling pagination automatically. |
| `list_files_in_dir` | `async (session, prefix) -> list[ArchiveFile]` | List files directly under an S3 prefix (no recursion), handling pagination automatically. |
| `list_symbols` | `async (trade_type, data_freq, data_type) -> list[str]` | List sorted symbol names for a given archive path. Creates its own session internally. |
| `list_symbol_files` | `async (trade_type, data_freq, data_type, symbol, interval=None, *, session=None) -> list[ArchiveFile]` | High-level helper that builds the S3 prefix and lists every file for one symbol. Reuses a caller-supplied session when provided (the caller owns its lifecycle); otherwise creates and closes a short-lived one internally. Raises `ValueError` when `interval` does not match `data_type.has_interval_layer`. |

## `ArchiveFile`

Immutable metadata record for a single file on the Binance public data archive.
Returned by `ArchiveClient.list_files_in_dir` and `ArchiveClient.list_symbol_files`.

```python
from binance_datatool.bhds.archive import ArchiveFile
```

| Field | Type | Description |
|-------|------|-------------|
| `key` | `str` | Full S3 object key, e.g. `data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-03.zip`. |
| `size` | `int` | File size in bytes, parsed from the S3 `Size` field. |
| `last_modified` | `datetime` | Last-modified timestamp, always tz-aware in UTC. Parsed from the S3 `LastModified` ISO 8601 value. |

Declared as `@dataclass(slots=True, frozen=True)` — instances are hashable and
immutable.

## `list_symbols` (module-level convenience function)

```python
from binance_datatool.bhds.archive import list_symbols

symbols = await list_symbols(TradeType.spot, DataFrequency.daily, DataType.klines)
```

Creates a temporary `ArchiveClient` and delegates to `ArchiveClient.list_symbols`.

## Symbol Filters

Typed filter dataclasses that operate on parsed `SymbolInfo` values, not raw symbol
strings. Each market segment has its own filter class exposing only the fields that make
sense for that market. Filters are `@dataclass(slots=True)` without `frozen`, and do not
import the `infer_*` helpers — callers infer first, then filter.

Each filter class supports two call shapes:

- `filter_.matches(info) -> bool` — single-symbol predicate.
- `filter_(infos) -> list[...]` — batch filter that preserves input order.

### `SpotSymbolFilter`

```python
from binance_datatool.bhds.archive import SpotSymbolFilter

spot_filter = SpotSymbolFilter(
    quote_assets=frozenset({"USDT"}),
    exclude_leverage=True,
    exclude_stable_pairs=True,
)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `quote_assets` | `frozenset[str] \| None` | `None` | Quote-asset allowlist. `None` disables the check. |
| `exclude_leverage` | `bool` | `False` | Reject symbols where `info.is_leverage` is true. |
| `exclude_stable_pairs` | `bool` | `False` | Reject symbols where `info.is_stable_pair` is true. |

### `UmSymbolFilter`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `quote_assets` | `frozenset[str] \| None` | `None` | Quote-asset allowlist. |
| `contract_type` | `ContractType \| None` | `None` | Restrict to `perpetual` or `delivery` contracts. |
| `exclude_stable_pairs` | `bool` | `False` | Reject stablecoin pairs. |

### `CmSymbolFilter`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `contract_type` | `ContractType \| None` | `None` | Restrict to `perpetual` or `delivery` contracts. |

### `SymbolFilter`

Union alias `SpotSymbolFilter | UmSymbolFilter | CmSymbolFilter`.

### `build_symbol_filter()`

```python
from binance_datatool.bhds.archive import build_symbol_filter

symbol_filter = build_symbol_filter(
    trade_type=TradeType.spot,
    quote_assets=frozenset({"USDT"}),
    exclude_leverage=True,
    exclude_stable_pairs=False,
    contract_type=None,
)
```

Constructs the market-specific filter class for a given `trade_type` using only the
arguments that apply to that market. Arguments that do not apply are silently ignored
(e.g. `exclude_leverage` for USD-M and COIN-M, `quote_assets` for COIN-M).

Returns `None` when every applicable argument is at its no-op default, so callers can pass
the result straight to `ArchiveListSymbolsWorkflow` and short-circuit filtering when no
constraints are active.

For S3 XML protocol details (request format, pagination, retry, proxy), see
[S3 Listing Protocol](s3-protocol.md).

---

See also: [S3 protocol](s3-protocol.md) | [Architecture](../../architecture.md) | [Workflow](workflow.md)
