# binance_datatool.bhds.cli

Typer CLI application. The entry point is `binance_datatool.bhds.cli:app`, exposed as the `bhds`
console script via `pyproject.toml`.

## App Structure

```
bhds [-v | -vv] [--bhds-home PATH]   # Root Typer app with shared options
└── archive                          # Sub-command group
    ├── list-symbols                 # Command
    ├── list-files                   # Command
    ├── download                     # Command
    └── verify                       # Command
```

## Root Callback

The root `bhds` app defines a callback that runs before any sub-command:

| Flag | Effect |
|------|--------|
| *(default)* | `loguru` level `WARNING`, unified `date \| level \| module - message` format |
| `-v` | `loguru` level `INFO`, same unified format |
| `-vv` | `loguru` level `DEBUG`, same unified format |
| `--bhds-home` | Override the BHDS data directory. Stored in `ctx.obj["bhds_home_override"]` and consumed by commands that need local archive storage (`download`, `verify`). See [`common.path`](../../common/path.md) for resolution priority. |

`-v` is `count`-based — pass `-v -v` or `-vv` for DEBUG. All CLI logging is written
to `stderr` via `configure_cli_logging()` from [`common.logging`](../../common/logging.md),
so sub-command stdout remains safe to pipe.

## Sub-command Groups

| Group | Description | Reference |
|-------|-------------|-----------|
| `archive` | S3 archive listing commands | [archive commands](archive.md) |

---

See also: [Architecture](../../../architecture.md) |
[Extending the Project — Adding a New CLI Command](../../../extending.md#adding-a-new-cli-command)
