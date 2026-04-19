# binance_datatool.common.path

Local path helpers for archive-managed data.

## `resolve_archive_home()`

```python
from binance_datatool.common import resolve_archive_home

home = resolve_archive_home()                     # from $BINANCE_DATATOOL_ARCHIVE_HOME
home = resolve_archive_home("/custom/path")      # explicit override
```

Resolves the archive home directory using the following priority:

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | `override` argument | CLI-provided `--archive-home` value. |
| 2 | `$BINANCE_DATATOOL_ARCHIVE_HOME` env var | Environment variable fallback. |
| 3 | *(none)* | Raises `ArchiveHomeNotConfiguredError`. |

Both the override and the env-var value are expanded via `Path.expanduser()`.

`archive_home` is the local Binance archive root itself, so local paths resolve
directly under `archive_home / "data" / ...`.

## `ArchiveHomeNotConfiguredError`

Custom `ValueError` subclass raised when neither a CLI override nor the
`BINANCE_DATATOOL_ARCHIVE_HOME` environment variable is set. The error message
includes remediation advice pointing to `--archive-home` and the environment
variable.

## `ARCHIVE_HOME_ENV_VAR`

String constant `"BINANCE_DATATOOL_ARCHIVE_HOME"` — the environment variable
name used by `resolve_archive_home()`.

```python
from binance_datatool.common.path import ARCHIVE_HOME_ENV_VAR
```

This constant is documented here but is not re-exported from
`binance_datatool.common`.

---

See also: [constants](constants.md) | [CLI overview](../cli/) | [Architecture](../../architecture.md)
