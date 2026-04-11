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
to `stderr` via `configure_cli_logging()` from [`common.logging`](../common.md#commonlogging),
so sub-command stdout remains safe to pipe.

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

## `archive list-files`

```
bhds archive list-files <TRADE_TYPE> [SYMBOLS...]
    [--freq FREQ] [--type TYPE] [--interval INTERVAL]
    [-l | --long]
    [--only-zip | --only-checksum]
```

Lists every file under one or more symbol directories on `data.binance.vision`,
preserving the caller-provided symbol order.

| Argument / Option | Type | Default | Description |
|-------------------|------|---------|-------------|
| `TRADE_TYPE` | `TradeType` | *(required)* | Market segment (positional argument). |
| `SYMBOLS` | `list[str]` | *(see below)* | Symbols to list, as variadic positional arguments. Accepts lowercase input; every symbol is normalized to uppercase before being passed to the workflow. |
| `--freq` | `DataFrequency` | `daily` | Partition frequency. |
| `--type` | `DataType` | `klines` | Dataset type. |
| `--interval` | `str \| None` | `None` | Kline interval directory. **Required** when `data_type.has_interval_layer` is `True` (the four kline-class data types); **rejected** otherwise. |
| `-l` / `--long` | `bool` | `False` | Switch from short relative-path output to a three-column TSV: `size_bytes<TAB>last_modified_iso<TAB>relative_path`. |
| `--only-zip` | `bool` | `False` | Print only `.zip` files. Mutually exclusive with `--only-checksum`. |
| `--only-checksum` | `bool` | `False` | Print only `.zip.CHECKSUM` files. Mutually exclusive with `--only-zip`. |

### Symbol input resolution

| State | Behavior |
|-------|----------|
| Positional args present | Use positional args, ignoring stdin (same convention as `grep` / `cat`). |
| No positional args and stdin is piped (`isatty()` is `False`) | Read `sys.stdin`, split on newlines, drop empty lines. |
| No positional args and stdin is a tty | Raise `typer.BadParameter` → exit 2. |

Every resolved symbol is stripped and uppercased before the workflow sees it, so
`btcusdt` and `BTCUSDT` behave identically.

### Output formats

**Short (default):** one relative path per line, `{symbol}[/{interval}]/{filename}`:

```
BTCUSDT/BTCUSDT-fundingRate-2026-03.zip
BTCUSDT/BTCUSDT-fundingRate-2026-03.zip.CHECKSUM
```

For kline-class data types the interval directory appears between symbol and
filename:

```
BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip
```

**Long (`-l` / `--long`):** three-column TSV, **no header row**, suitable for
piping into `awk` / `cut` / Polars:

```
1048	2026-04-01T08:06:34Z	BTCUSDT/BTCUSDT-fundingRate-2026-03.zip
105	2026-04-01T08:06:34Z	BTCUSDT/BTCUSDT-fundingRate-2026-03.zip.CHECKSUM
```

Columns are:

| # | Field | Format |
|---|-------|--------|
| 1 | `size_bytes` | Raw integer byte count. No human-readable suffix (so `awk '{sum+=$1}'` works). |
| 2 | `last_modified_iso` | ISO 8601 UTC second precision with `Z` suffix, produced via `datetime.strftime("%Y-%m-%dT%H:%M:%SZ")`. |
| 3 | `relative_path` | Same value as the short-format line. |

No header line is emitted; this matches `ls -l` / `find -printf` / `du` / `git ls-files`
conventions so the output is pipe-safe without `tail -n +2` gymnastics.

### Exit codes

| Situation | Exit code |
|-----------|-----------|
| All symbols succeeded (empty results still count as success) | `0` |
| Any symbol failed | `2` (per-symbol errors are logged to stderr as `ERROR: {symbol}: {message}`) |
| Argument validation error (mutually exclusive flags, missing interval, extra interval, no symbols) | `2` |
| `KeyboardInterrupt` | `130` |

### Composition with `list-symbols`

`list-files` is designed to be piped from `list-symbols`:

```
bhds archive list-symbols um --quote USDT --exclude-stables \
  | bhds archive list-files um --type klines --interval 1m
```

### Examples

```
# Single-symbol funding-rate listing
bhds archive list-files um --freq monthly --type fundingRate BTCUSDT

# Multi-symbol klines listing (long TSV, zip-only)
bhds archive list-files um --type klines --interval 1m -l --only-zip BTCUSDT ETHUSDT

# Pipe from list-symbols, verbose logging
bhds -v archive list-symbols um --quote USDT --exclude-stables \
  | bhds -v archive list-files um --type klines --interval 1m
```

---

See also: [Workflow layer](workflow.md) |
[Extending the Project — Adding a New CLI Command](../../extending.md#adding-a-new-cli-command)
