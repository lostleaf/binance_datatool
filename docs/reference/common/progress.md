# binance_datatool.common.progress

Shared progress-reporting helpers used by archive workflows and the downloader.

```python
from binance_datatool.common.progress import make_reporter, ProgressEvent
```

This module is **not** re-exported from `binance_datatool.common` — import directly
from the defining module.

## `ProgressEvent`

Immutable payload for a single progress tick.

```python
from binance_datatool.common.progress import ProgressEvent

event = ProgressEvent(name="BTCUSDT", ok=True, count=1)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | *(required)* | Short label for the item that just completed (e.g. a symbol name or batch label). |
| `ok` | `bool` | *(required)* | Whether the item succeeded. |
| `count` | `int` | `1` | Number of logical units this event represents. |

Declared as `@dataclass(slots=True, frozen=True)`.

## `ProgressReporter` (Protocol)

Context-managed interface that all reporter implementations satisfy.

```python
class ProgressReporter(Protocol):
    def __enter__(self) -> ProgressReporter: ...
    def __exit__(self, exc_type, exc, tb) -> None: ...
    def tick(self, event: ProgressEvent) -> None: ...
```

Callers should use `make_reporter()` to obtain a concrete implementation rather
than constructing reporters directly.

## `LogReporter`

Emits sampled progress lines to loguru's `INFO` channel. Progress lines are
rate-limited to ~20 steps so high-volume workflows do not flood the log.

On context exit the reporter emits a final summary line
(`complete` or `aborted`) with cumulative ok/fail counts.

| Parameter | Description |
|-----------|-------------|
| `total` | Expected number of units. |
| `description` | Human-readable label (e.g. `"download"`, `"verify"`). |

## `TqdmReporter`

Interactive `tqdm`-based progress bar on stderr. While active, existing
loguru stderr sinks are temporarily redirected through `tqdm.write()` so
that log lines do not corrupt the progress bar.

| Parameter | Description |
|-----------|-------------|
| `total` | Expected number of units. |
| `description` | Human-readable label shown in the progress bar prefix. |

`TqdmReporter` is **not re-entrant** — nesting two reporters in the same
process raises `RuntimeError`.

## `make_reporter()`

Factory function that selects the appropriate reporter implementation.

```python
from binance_datatool.common.progress import make_reporter

with make_reporter(progress_bar=True, total=100, description="download") as reporter:
    for item in items:
        process(item)
        reporter.tick(ProgressEvent(name=item.name, ok=True))
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `progress_bar` | `bool` | `True` → `TqdmReporter`; `False` → `LogReporter`. |
| `total` | `int` | Expected number of progress units. |
| `description` | `str` | Human-readable label for the operation. |

Returns a `ProgressReporter`-compatible context manager.

---

See also: [Downloader](../archive/downloader.md) | [Archive client](../archive/client.md) | [Architecture](../../architecture.md)
