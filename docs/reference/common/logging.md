# binance_datatool.common.logging

Shared `loguru` configuration helper for CLI entry points.

```python
from binance_datatool.common import configure_cli_logging

configure_cli_logging(verbosity=1)  # 0 = WARNING, 1 = INFO, 2+ = DEBUG
```

`configure_cli_logging(verbosity: int) -> None` resets `loguru` and installs a
single `stderr` sink with a level and format chosen from the verbosity level:

| `verbosity` | `loguru` level | Format |
|-------------|----------------|--------|
| `0` | `WARNING` | `<level>{level}</level>: {message}` |
| `1` | `INFO` | `<level>{level}</level>: {message}` |
| `2` or more | `DEBUG` | `<green>{time:HH:mm:ss.SSS}</green> \| <level>{level: <8}</level> \| <cyan>{name}</cyan>:<cyan>{line}</cyan> - {message}` |

The sink uses `colorize=sys.stderr.isatty()` so ANSI colour escapes appear on
interactive terminals but are stripped from logs redirected to files or pipes.
Level names are emitted in uppercase (`INFO`, `ERROR`, `DEBUG`, `WARNING`),
matching the conventions used by `pip`, the stdlib `logging` module, and the
default `loguru` formatter.

This function is called by the root `bhds` Typer callback before any
sub-command runs; see [CLI — Root Callback and Verbosity](../bhds/cli/overview.md#root-callback-and-verbosity).
It is introduced as a shared helper so future CLI entry points (for example a
planned `bmds`) can reuse the same configuration without duplication.

---

See also: [CLI overview](../bhds/cli/overview.md) | [Architecture](../../architecture.md)
