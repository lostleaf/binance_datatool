# binance_datatool.bhds.cli

Typer CLI application. The entry point is `binance_datatool.bhds.cli:app`, exposed as the `bhds`
console script via `pyproject.toml`.

## App Structure

```
bhds [-v | -vv]               # Root Typer app with verbosity callback
└── archive                   # Sub-command group
    ├── list-symbols          # Command
    └── list-files            # Command
```

## Root Callback and Verbosity

The root `bhds` app defines a callback that runs before any sub-command:

| Flag | Effect |
|------|--------|
| *(default)* | `loguru` level `WARNING`, simple `LEVEL: message` format |
| `-v` | `loguru` level `INFO`, simple format |
| `-vv` | `loguru` level `DEBUG`, rich format with time, level, module, and line |

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
