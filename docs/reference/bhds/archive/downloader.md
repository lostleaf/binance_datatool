# binance_datatool.bhds.archive.downloader

Aria2-backed archive download helpers.

The package-level `binance_datatool.bhds.archive` re-exports every public
downloader type and function, so most imports can use:

```python
from binance_datatool.bhds.archive import DownloadRequest, download_archive_files
```

## `DownloadRequest`

Immutable `@dataclass(slots=True, frozen=True)` describing a single
direct-download task.

| Field | Type | Description |
|-------|------|-------------|
| `url` | `str` | Full download URL built from `S3_DOWNLOAD_PREFIX` plus the archive key. |
| `local_path` | `Path` | Target filesystem path. Parent directories are created automatically. |

## `download_archive_files()`

```python
from binance_datatool.bhds.archive import DownloadRequest, download_archive_files

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
| `inherit_proxy` | *(required)* | When `False`, proxy env vars (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, etc.) are stripped from the aria2c subprocess environment. When `True`, aria2c inherits the caller's proxy settings. |
| `batch_size` | `4096` | Maximum files per aria2c invocation. Larger sets are chunked. |
| `max_tries` | `3` | Maximum retry rounds for failed batches. Each retry re-runs the entire failed batch. |
| `progress_callback` | `None` | Optional `Callable[[BatchProgressEvent], None]` invoked at batch lifecycle events. |

Returns an `Aria2DownloadResult`.

Aria2 is invoked with `--allow-overwrite=true` and
`--auto-file-renaming=false` so updated files replace existing ones instead of
producing `.1.zip`-style renamed duplicates.

## `Aria2DownloadResult`

| Field / Property | Type | Description |
|------------------|------|-------------|
| `requested` | `int` | Total number of files requested. |
| `failed_requests` | `list[DownloadRequest]` | Requests that still failed after all retries. |
| `succeeded` | `int` *(property)* | `requested - len(failed_requests)`. |

## `BatchProgressEvent`

Frozen dataclass payload for the progress callback.

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `str` | One of `"start"`, `"success"`, `"retry"`, `"failed"`. |
| `batch_index` | `int` | 1-based index within the current retry round. |
| `total_batches` | `int` | Total batches in the current retry round. |
| `requested` | `int` | Number of files in this batch. |
| `attempt` | `int` | Current retry attempt, 1-based. |
| `max_tries` | `int` | Maximum retry attempts configured. |

## `Aria2NotFoundError`

Custom `FileNotFoundError` subclass raised when `aria2c` is not available in
`PATH`.

---

See also: [Archive package](README.md) | [Workflow](../workflow.md) | [S3 protocol](../s3-protocol.md)
