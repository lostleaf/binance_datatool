# binance_datatool.common.path

Local path helpers for BHDS-managed data.

## `resolve_bhds_home()`

```python
from binance_datatool.common import resolve_bhds_home

home = resolve_bhds_home()                    # from $BHDS_HOME
home = resolve_bhds_home("/custom/path")      # explicit override
```

Resolves the BHDS home directory using the following priority:

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | `override` argument | CLI-provided `--bhds-home` value. |
| 2 | `$BHDS_HOME` env var | Environment variable fallback. |
| 3 | *(none)* | Raises `BhdsHomeNotConfiguredError`. |

Both the override and the env-var value are expanded via `Path.expanduser()`.

## `BhdsHomeNotConfiguredError`

Custom `ValueError` subclass raised when neither a CLI override nor the `BHDS_HOME`
environment variable is set. The error message includes remediation advice pointing
to `--bhds-home` and the environment variable.

## `BHDS_HOME_ENV_VAR`

String constant `"BHDS_HOME"` — the environment variable name used by
`resolve_bhds_home()`.

```python
from binance_datatool.common.path import BHDS_HOME_ENV_VAR
```

This constant is documented here but is not re-exported from
`binance_datatool.common`.

---

See also: [constants](constants.md) | [CLI overview](../bhds/cli/) | [Architecture](../../architecture.md)
