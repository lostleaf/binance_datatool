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
| `list_symbols` | `async (trade_type, data_freq, data_type) -> list[str]` | List sorted symbol names for a given archive path. Creates its own session internally. |

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

## S3 Listing Protocol

The archive client communicates with data.binance.vision's S3-compatible XML listing API.

**Request format:**

```
GET https://s3-ap-northeast-1.amazonaws.com/data.binance.vision
    ?delimiter=/&prefix=data/spot/daily/klines/
```

**Response structure (simplified):**

```xml
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <NextMarker>...</NextMarker>
  <CommonPrefixes>
    <Prefix>data/spot/daily/klines/BTCUSDT/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>data/spot/daily/klines/ETHUSDT/</Prefix>
  </CommonPrefixes>
</ListBucketResult>
```

**Key behaviors:**

| Behavior | Detail |
|-----------|--------|
| **Pagination** | S3 returns at most 1000 entries per page. When `IsTruncated` is `"true"`, the client uses `NextMarker` (or falls back to the last prefix) to fetch the next page. |
| **xmltodict normalization** | When only one `CommonPrefixes` element exists, `xmltodict` returns a `dict` instead of a `list`. The `_normalize_entries()` helper handles this. |
| **Retry with backoff** | HTTP requests use `tenacity` with exponential backoff (up to 5 attempts), retrying only on `aiohttp.ClientError` and `asyncio.TimeoutError`. |

## Proxy Support

`ArchiveClient` creates `aiohttp.ClientSession` with `trust_env=True` by default. This means
the session reads proxy configuration from standard environment variables:

- `http_proxy` / `HTTP_PROXY`
- `https_proxy` / `HTTPS_PROXY`
- `no_proxy` / `NO_PROXY`

No additional proxy configuration is needed at the application level.

---

See also: [Architecture](../../architecture.md) | [Workflow](workflow.md)
