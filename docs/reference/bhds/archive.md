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

## Archive Downloader

Aria2-backed batch download helpers. Declared in `bhds/archive/downloader.py` and
re-exported from `bhds/archive/__init__`.

### `DownloadRequest`

Immutable `@dataclass(slots=True, frozen=True)` describing a single direct-download task.

| Field | Type | Description |
|-------|------|-------------|
| `url` | `str` | Full download URL (built from `S3_DOWNLOAD_PREFIX` + archive key). |
| `local_path` | `Path` | Target filesystem path. Parent directories are created automatically. |

### `download_archive_files()`

```python
from binance_datatool.bhds.archive import download_archive_files, DownloadRequest

result = download_archive_files(
    requests,
    inherit_proxy=False,
    batch_size=4096,
    max_tries=3,
    progress_callback=None,
)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `requests` | *(required)* | Sequence of `DownloadRequest` items to download. |
| `inherit_proxy` | *(required)* | When `False`, proxy env vars (`HTTP_PROXY`, `HTTPS_PROXY`, etc.) are stripped from the aria2c subprocess environment. When `True`, aria2c inherits the caller's proxy settings. |
| `batch_size` | `4096` | Maximum files per aria2c invocation. Larger sets are chunked. |
| `max_tries` | `3` | Maximum retry rounds for failed batches. Each retry re-runs the entire failed batch. |
| `progress_callback` | `None` | Optional `Callable[[BatchProgressEvent], None]` invoked at batch lifecycle events. |

Returns an `Aria2DownloadResult`.

Aria2 is invoked with `--allow-overwrite=true` and `--auto-file-renaming=false` to
ensure updated files replace existing ones without `.1.zip` renaming.

### `Aria2DownloadResult`

| Field / Property | Type | Description |
|------------------|------|-------------|
| `requested` | `int` | Total number of files requested. |
| `failed_requests` | `list[DownloadRequest]` | Requests that still failed after all retries. |
| `succeeded` | `int` *(property)* | `requested - len(failed_requests)`. |

### `BatchProgressEvent`

Frozen dataclass payload for the progress callback.

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `str` | One of `"start"`, `"success"`, `"retry"`, `"failed"`. |
| `batch_index` | `int` | 1-based index within the current retry round. |
| `total_batches` | `int` | Total batches in the current retry round. |
| `requested` | `int` | Number of files in this batch. |
| `attempt` | `int` | Current retry attempt (1-based). |
| `max_tries` | `int` | Maximum retry attempts configured. |

### `Aria2NotFoundError`

Custom `FileNotFoundError` subclass raised when `aria2c` is not available in `PATH`.

## Archive Checksum

SHA256 verification helpers for local archive files. Declared in
`bhds/archive/checksum.py` and re-exported from `bhds/archive/__init__`.

### `VerifyFileResult`

Mutable `@dataclass(slots=True)` describing the verification outcome for a single zip file.

| Field | Type | Description |
|-------|------|-------------|
| `zip_path` | `Path` | Path to the zip file that was verified. |
| `passed` | `bool` | `True` when the computed SHA256 matches the expected checksum. |
| `detail` | `str` | Empty on success; describes the failure otherwise (`"checksum mismatch"`, or the exception message for I/O errors). |

### `calc_sha256()`

```python
from binance_datatool.bhds.archive import calc_sha256

hex_digest = calc_sha256(file_path)
```

Computes the SHA256 hex digest of a file using `hashlib.file_digest`.

### `read_expected_checksum()`

```python
from binance_datatool.bhds.archive import read_expected_checksum

expected = read_expected_checksum(zip_path)
```

Reads the first whitespace-delimited token from the sibling `.CHECKSUM` file
(`{zip_path}.CHECKSUM`). Raises `FileNotFoundError` when the checksum file does not
exist and `ValueError` when the file is empty or has no parseable token.

### `verify_single_file()`

```python
from binance_datatool.bhds.archive import verify_single_file

result = verify_single_file(zip_path)
```

Verifies a single zip file against its sibling checksum file. Returns a
`VerifyFileResult` — never raises. Exceptions during checksum reading or
SHA256 computation are caught and reported via `result.detail`.

This function is the unit of work submitted to `ProcessPoolExecutor` by
`ArchiveVerifyWorkflow`.

For S3 XML protocol details (request format, pagination, retry, proxy), see
[S3 Listing Protocol](s3-protocol.md).

---

See also: [S3 protocol](s3-protocol.md) | [Architecture](../../architecture.md) | [Workflow](workflow.md)
