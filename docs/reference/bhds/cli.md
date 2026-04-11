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
bhds archive list-symbols <TRADE_TYPE>
    [--freq FREQ] [--type TYPE]
    [--quote QUOTE ...] [--exclude-leverage] [--exclude-stables]
    [--contract-type CONTRACT_TYPE]
```

| Argument / Option | Type | Default | Description |
|-------------------|------|---------|-------------|
| `TRADE_TYPE` | `TradeType` | *(required)* | Market segment (positional argument). |
| `--freq` | `DataFrequency` | `daily` | Partition frequency. |
| `--type` | `DataType` | `klines` | Dataset type. |
| `--quote` | `list[str]` | `None` | Filter by quote asset. Repeat to allow multiple values. Normalized to uppercase and deduplicated. |
| `--exclude-leverage` | `bool` | `False` | Exclude leveraged spot tokens. Ignored for USD-M and COIN-M. |
| `--exclude-stables` | `bool` | `False` | Exclude stablecoin pairs. Ignored for COIN-M. |
| `--contract-type` | `ContractType` | `None` | Restrict futures symbols to `perpetual` or `delivery`. Ignored for spot. |

Flags that do not apply to the chosen market are silently ignored. When every filter flag is
at its default, no filter is constructed and the workflow runs in unfiltered mode.

**Output:** one matched symbol per line to stdout. The CLI only prints
`ListSymbolsResult.matched`; raw symbols that failed inference and symbols that were
filtered out are not printed, regardless of whether any filter flag was passed.

**Examples:**

```
bhds archive list-symbols spot --quote USDT --exclude-leverage --exclude-stables
bhds archive list-symbols um --quote USDT --quote USDC --contract-type perpetual
bhds archive list-symbols cm --contract-type delivery
```

---

See also: [Workflow layer](workflow.md) |
[Extending the Project — Adding a New CLI Command](../../extending.md#adding-a-new-cli-command)
