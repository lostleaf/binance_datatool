# binance_datatool.bhds.cli

Typer CLI application. The entry point is `binance_datatool.bhds.cli:app`, exposed as the `bhds`
console script via `pyproject.toml`.

## App Structure

```
bhds                          # Root Typer app
└── archive                   # Sub-command group
    └── list-symbols          # Command
```

## `archive list-symbols`

```
bhds archive list-symbols <TRADE_TYPE> [--freq FREQ] [--type TYPE]
```

| Argument / Option | Type | Default | Description |
|-------------------|------|---------|-------------|
| `TRADE_TYPE` | `TradeType` | *(required)* | Market segment (positional argument). |
| `--freq` | `DataFrequency` | `daily` | Partition frequency. |
| `--type` | `DataType` | `klines` | Dataset type. |

**Output:** one symbol name per line to stdout.

---

See also: [Workflow layer](workflow.md) |
[Extending the Project — Adding a New CLI Command](../../extending.md#adding-a-new-cli-command)
