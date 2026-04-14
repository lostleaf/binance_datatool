# binance_datatool.bhds.workflow

Business logic orchestration layer between the CLI and the archive client.

## `ArchiveListSymbolsWorkflow`

Fetches raw symbols via `ArchiveClient`, infers typed metadata per market segment, and
optionally applies a typed symbol filter.

```python
from binance_datatool.bhds.archive import SpotSymbolFilter
from binance_datatool.bhds.workflow.archive import ArchiveListSymbolsWorkflow, ListSymbolsResult
from binance_datatool.common import DataFrequency, DataType, TradeType

workflow = ArchiveListSymbolsWorkflow(
    trade_type=TradeType.spot,
    data_freq=DataFrequency.daily,
    data_type=DataType.klines,
    symbol_filter=SpotSymbolFilter(
        quote_assets=frozenset({"USDT"}),
        exclude_leverage=True,
    ),
)
result: ListSymbolsResult = await workflow.run()
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_type` | *(required)* | Market segment to query. |
| `data_freq` | *(required)* | Partition frequency. |
| `data_type` | *(required)* | Dataset type. |
| `symbol_filter` | `None` | Optional typed filter applied to inferred metadata. `None` disables filtering. |
| `client` | `None` | Optional pre-configured `ArchiveClient`. A default client is created when omitted. |

### `run()`

```python
async def run(self) -> ListSymbolsResult
```

1. Calls `client.list_symbols()` for the configured market.
2. Dispatches each raw symbol through `infer_spot_info` / `infer_um_info` / `infer_cm_info`
   according to `trade_type`.
3. Splits the results into `inferred` and `unmatched` (raw strings that failed inference).
4. If `symbol_filter` is set, further splits `inferred` into `matched` and `filtered_out`
   via `symbol_filter.matches()`. Otherwise `matched == inferred` and `filtered_out` is empty.

## `ListSymbolsResult`

Structured return type for `ArchiveListSymbolsWorkflow.run()`. Declared as a `slots=True`
dataclass in the workflow module because it is the workflow's result shape, not a general
shared type.

| Field | Type | Description |
|-------|------|-------------|
| `matched` | `list[SymbolInfo]` | Inferred symbols that passed the filter (or all inferred symbols when no filter is set). |
| `unmatched` | `list[str]` | Raw symbols that could not be parsed by the per-market inference function. |
| `filtered_out` | `list[SymbolInfo]` | Inferred symbols rejected by the filter. Always empty when no filter is set. |

Input order is preserved across all three buckets.

## `ArchiveListFilesWorkflow`

Lists archive files for one or more symbols concurrently while preserving caller
input order and isolating per-symbol failures so that one bad symbol does not abort
the entire batch.

```python
from binance_datatool.bhds.workflow.archive import (
    ArchiveListFilesWorkflow,
    ListFilesResult,
)
from binance_datatool.common import DataFrequency, DataType, TradeType

workflow = ArchiveListFilesWorkflow(
    trade_type=TradeType.um,
    data_freq=DataFrequency.daily,
    data_type=DataType.klines,
    symbols=["BTCUSDT", "ETHUSDT"],
    interval="1m",
)
result: ListFilesResult = await workflow.run()
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_type` | *(required)* | Market segment to query. |
| `data_freq` | *(required)* | Partition frequency. |
| `data_type` | *(required)* | Dataset type. |
| `symbols` | *(required)* | Sequence of symbols to list. Caller order is preserved. |
| `interval` | `None` | Kline interval directory. Required when `data_type.has_interval_layer` is `True`; must be `None` otherwise. |
| `client` | `None` | Optional pre-configured `ArchiveClient`. A default client is created when omitted. |

The constructor validates the `interval` vs `data_type.has_interval_layer` invariant
and raises `ValueError` on mismatch. This is the workflow-layer line of defense in
the three-layer interval consistency check (CLI / Workflow / Client).

### `run()`

```python
async def run(self) -> ListFilesResult
```

1. Opens a single shared `aiohttp.ClientSession` configured with
   `S3_HTTP_TIMEOUT_SECONDS` and `trust_env=True`.
2. Creates one `client.list_symbol_files(...)` coroutine per symbol, passing the
   shared session so every concurrent request reuses one connection pool.
3. Awaits them through `asyncio.gather(*, return_exceptions=True)` — no semaphore,
   no explicit rate limiting; the underlying aiohttp connection pool caps concurrency.
4. Wraps successes into `SymbolListFilesResult(symbol, files, error=None)` and
   exceptions into `SymbolListFilesResult(symbol, files=[], error=str(exc))`,
   preserving caller input order.

Returns a `ListFilesResult` that always covers every requested symbol.

## `SymbolListFilesResult`

Per-symbol entry inside `ListFilesResult.per_symbol`. A successful listing of an
empty directory is represented by `files == []` with `error is None`; this is
**distinct** from a failed listing, which also carries an empty `files` list but sets
`error` to a non-empty description of the failure.

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | `str` | The symbol this entry describes, normalized to uppercase by the CLI. |
| `files` | `list[ArchiveFile]` | Files found under the symbol directory. Empty on both success-with-empty-directory and failure. |
| `error` | `str \| None` | `None` on success; a text description of the failure otherwise. |

## `ListFilesResult`

Aggregate result for `ArchiveListFilesWorkflow.run()`.

| Field / Property | Type | Description |
|------------------|------|-------------|
| `per_symbol` | `list[SymbolListFilesResult]` | One entry per requested symbol, in caller-provided input order. |
| `has_failures` | `bool` *(property)* | `True` when any `per_symbol` entry has a non-`None` `error`. Used by the CLI to set exit code 2. |
| `total_remote_files` | `int` *(property)* | Total number of successfully listed remote files across all symbols. |

## `ArchiveDownloadWorkflow`

Diffs remote archive listings against local files and optionally downloads new or
updated files via aria2c.

```python
from binance_datatool.bhds.workflow.archive import (
    ArchiveDownloadWorkflow,
    DiffResult,
    DownloadResult,
)
from binance_datatool.common import DataFrequency, DataType, TradeType

workflow = ArchiveDownloadWorkflow(
    trade_type=TradeType.um,
    data_freq=DataFrequency.monthly,
    data_type=DataType.funding_rate,
    symbols=["BTCUSDT"],
    bhds_home=Path("/data/bhds"),
)
result = await workflow.run()
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_type` | *(required)* | Market segment to query. |
| `data_freq` | *(required)* | Partition frequency. |
| `data_type` | *(required)* | Dataset type. |
| `symbols` | *(required)* | Symbols to download, preserving caller order. |
| `bhds_home` | *(required)* | Root directory for local BHDS data storage. |
| `interval` | `None` | Kline interval directory. Required for kline-class data types. |
| `dry_run` | `False` | When `True`, compute the diff without downloading. |
| `inherit_aria2_proxy` | `False` | Whether aria2c should inherit proxy env vars. |
| `show_progress` | `False` | Whether to display a tqdm progress bar on stderr. |
| `client` | `None` | Optional pre-configured `ArchiveClient`. |
| `download_func` | `None` | Optional download callable for dependency injection. Defaults to `download_archive_files`. |

### `run()`

```python
async def run(self) -> DiffResult | DownloadResult
```

1. Delegates to `ArchiveListFilesWorkflow` to fetch remote file metadata for all
   requested symbols concurrently.
2. Computes a diff by comparing each remote file against the local path under
   `bhds_home/aws_data/`. A file is skipped when its local copy exists and
   `local_mtime >= remote_last_modified`; otherwise it is classified as `"new"`
   or `"updated"`.
3. In dry-run mode, returns a `DiffResult` immediately.
4. Otherwise, invalidates stale `.verified` markers for updated zip/checksum files,
   then downloads via `download_archive_files` with batch retry and optional tqdm
   progress.

Returns `DiffResult` when `dry_run=True`, otherwise `DownloadResult`.

## `DiffEntry`

One file selected for download.

| Field / Property | Type | Description |
|------------------|------|-------------|
| `remote` | `ArchiveFile` | Remote file metadata. |
| `local_path` | `Path` | Target local path under `bhds_home/aws_data/`. |
| `reason` | `Literal["new", "updated"]` | Why this file was selected. |
| `url` | `str` *(property)* | Full download URL built from `S3_DOWNLOAD_PREFIX` + key. |

## `DiffResult`

Returned by `ArchiveDownloadWorkflow.run()` in dry-run mode.

| Field / Property | Type | Description |
|------------------|------|-------------|
| `to_download` | `list[DiffEntry]` | Files selected for download. |
| `skipped` | `int` | Files already up to date locally. |
| `total_remote` | `int` | Total remote files listed. |
| `listing_errors` | `list[SymbolListingError]` | Per-symbol listing failures. |
| `listing_failed_symbols` | `int` *(property)* | Count of failed symbol listings. |

## `DownloadResult`

Returned by `ArchiveDownloadWorkflow.run()` after a real download.

| Field / Property | Type | Description |
|------------------|------|-------------|
| `total_remote` | `int` | Total remote files listed. |
| `skipped` | `int` | Files already up to date locally. |
| `downloaded` | `int` | Files successfully downloaded. |
| `failed` | `int` | Files that failed after all retries. |
| `listing_errors` | `list[SymbolListingError]` | Per-symbol listing failures. |
| `listing_failed_symbols` | `int` *(property)* | Count of failed symbol listings. |

## `SymbolListingError`

Structured per-symbol listing error, used by both `DiffResult` and `DownloadResult`.

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | `str` | The symbol whose listing failed. |
| `error` | `str` | Text description of the failure. |

## `ArchiveVerifyWorkflow`

Scans local archive directories, classifies zip files into verify / skip / orphan
buckets, and optionally verifies SHA256 checksums in parallel using a process pool.

```python
from binance_datatool.bhds.workflow.archive import (
    ArchiveVerifyWorkflow,
    VerifyDiffResult,
    VerifyResult,
)
from binance_datatool.common import DataFrequency, DataType, TradeType

workflow = ArchiveVerifyWorkflow(
    trade_type=TradeType.um,
    data_freq=DataFrequency.daily,
    data_type=DataType.klines,
    symbols=["BTCUSDT"],
    bhds_home=Path("/data/bhds"),
    interval="1m",
)
result = workflow.run()
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_type` | *(required)* | Market segment. |
| `data_freq` | *(required)* | Partition frequency. |
| `data_type` | *(required)* | Dataset type. |
| `symbols` | *(required)* | Symbols to verify, preserving caller order. |
| `bhds_home` | *(required)* | Root directory for local BHDS data storage. |
| `interval` | `None` | Kline interval directory. Required for kline-class data types. |
| `keep_failed` | `False` | When `True`, retain failed zip and checksum files instead of deleting them. |
| `dry_run` | `False` | When `True`, scan and classify files without verifying or mutating the filesystem. |
| `n_workers` | `None` | Process pool size. Defaults to `max(1, cpu_count - 2)`. |
| `show_progress` | `False` | Whether to display a tqdm progress bar on stderr. |

### `run()`

```python
def run(self) -> VerifyDiffResult | VerifyResult
```

This is a **synchronous** method (no `async`) because the verify workflow is purely
local I/O and CPU work — no network access is needed.

1. **Scan** — walks each symbol's local directory under
   `bhds_home/aws_data/data/{s3_path}/{freq}/{type}/{symbol}[/{interval}]`,
   classifying each `.zip` file as pending verification, already verified (valid
   timestamped marker), orphan zip (missing `.CHECKSUM` sibling), or orphan checksum
   (missing `.zip` sibling).
2. In **dry-run mode**, returns a `VerifyDiffResult` immediately.
3. **Orphan cleanup** — orphan zips have their markers cleared (zip file is kept);
   orphan checksums and their markers are deleted.
4. **Parallel verification** — submits `verify_single_file` tasks to a
   `ProcessPoolExecutor` with `spawn` context. SHA256 is CPU-intensive, so process
   isolation avoids GIL contention.
5. **Post-verify** — for each result: clears old markers, writes a fresh timestamped
   marker on pass, or deletes the zip + checksum on failure (unless `keep_failed`).

Returns `VerifyDiffResult` when `dry_run=True`, otherwise `VerifyResult`.

### Timestamped marker protocol

Verified files are tracked by marker files named `{zip_name}.{ts}.verified`, where
`ts = max(now, ceil(max(zip_mtime, checksum_mtime)))`. A marker is valid when
`max(ts_markers) >= ceil(max(zip_mtime, checksum_mtime))`. Legacy markers without a
timestamp (`file.zip.verified`) are treated as invalid and force re-verification.

## `VerifyDiffResult`

Returned by `ArchiveVerifyWorkflow.run()` in dry-run mode.

| Field / Property | Type | Description |
|------------------|------|-------------|
| `to_verify` | `list[Path]` | Zip files pending SHA256 verification. |
| `skipped` | `int` | Zip files with a valid timestamped marker. |
| `orphan_zips` | `list[Path]` | Zip files without a sibling `.CHECKSUM`. |
| `orphan_checksums` | `list[Path]` | `.CHECKSUM` files without a sibling `.zip`. |
| `total_zips` | `int` *(property)* | `len(to_verify) + skipped + len(orphan_zips)`. |

## `VerifyResult`

Returned by `ArchiveVerifyWorkflow.run()` after a real verify run.

| Field / Property | Type | Description |
|------------------|------|-------------|
| `skipped` | `int` | Zip files with a valid marker (not re-verified). |
| `verified` | `int` | Zip files that passed verification. |
| `orphan_zips` | `int` | Orphan zips whose markers were cleared. |
| `orphan_checksums` | `int` | Orphan checksums that were deleted. |
| `failed_details` | `dict[Path, str]` | Failed zip paths mapped to failure descriptions. |
| `failed` | `int` *(property)* | `len(failed_details)`. |
| `total_zips` | `int` *(property)* | `skipped + verified + failed + orphan_zips`. |

---

See also: [Archive client](archive.md) | [CLI commands](cli/archive.md) | [Architecture](../../architecture.md)
