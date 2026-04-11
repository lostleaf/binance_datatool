# binance_datatool.common

Shared types and constants imported by all other packages. The `__init__.py` re-exports every
public symbol so callers can write `from binance_datatool.common import TradeType`.

## Root Package

| Symbol | Type | Description |
|--------|------|-------------|
| `__version__` | `str` | Semantic version string (currently `0.1.0`). |

## `common.constants`

| Constant | Type | Value | Description |
|----------|------|-------|-------------|
| `S3_LISTING_PREFIX` | `str` | `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision` | Base URL for the S3 listing endpoint. |
| `S3_HTTP_TIMEOUT_SECONDS` | `int` | `15` | Default timeout per HTTP request. |
| `QUOTE_ASSETS` | `tuple[str, ...]` | 48-item tuple | Quote suffixes used by spot and USD-M parsing. The tuple is ordered by descending length, then alphabetically within the same length. |
| `STABLECOINS` | `frozenset[str]` | 24-item set | Stablecoin and fiat-pegged assets used for `is_stable_pair` detection. |
| `LEVERAGE_SUFFIXES` | `tuple[str, ...]` | `("UP", "DOWN", "BULL", "BEAR")` | Leveraged-token suffixes for spot symbols. |
| `LEVERAGE_EXCLUDES` | `frozenset[str]` | `{"JUP", "SYRUP"}` | Base assets that would otherwise be false positives for leveraged-token detection. |
| `QUOTE_BASE_EXCLUDES` | `dict[str, tuple[str, frozenset[str]]]` | 4-entry mapping | Known cases where greedy long-quote matching must fall back to a shorter quote for specific base assets. |

### Quote Parsing Rules

`infer_spot_info()` and `infer_um_info()` parse symbols by walking `QUOTE_ASSETS` from longest suffix to
shortest suffix. This is necessary because Binance has overlapping quote tokens such as `RLUSD` and
`USD`; without greedy matching, `XRPRLUSD` would be misparsed as `XRPRL + USD` instead of `XRP + RLUSD`.

Greedy matching is necessary, but it is not sufficient. A smaller set of live Binance symbols also
creates the reverse ambiguity: the symbol ends with a longer quote token, but the correct parse uses a
shorter quote. `ADAEUR` is the canonical example. The string ends with `AEUR`, but Binance's
`exchangeInfo` defines the pair as `ADA + EUR`, not `AD + AEUR`.

`QUOTE_BASE_EXCLUDES` captures those known fallback rules explicitly. Each entry has this shape:

```python
LONG_QUOTE: (FALLBACK_QUOTE, {VALID_BASES_FOR_FALLBACK})
```

Interpretation:

- Try `LONG_QUOTE` first because it is longer.
- If the same symbol also ends with `FALLBACK_QUOTE` and the base asset derived from that fallback is
  listed in `VALID_BASES_FOR_FALLBACK`, reject the greedy `LONG_QUOTE` match.
- Continue parsing with the shorter quote.

Example:

```python
"AEUR": ("EUR", {"ADA", "ENA", "GALA", "LUNA", "THETA"})
```

This means symbols such as `ADAEUR` and `LUNAEUR` should not be accepted as `AD + AEUR` or
`LUN + AEUR`. They must fall back to `ADA + EUR` and `LUNA + EUR`.

The mapping is intentionally small and evidence-driven. It is not a general symbol registry. Add new
entries only when live Binance metadata proves that a greedy long-quote match is wrong.

## `common.enums`

**`TradeType(StrEnum)`** — Binance market segment.

| Member | Value | `s3_path` | Description |
|--------|-------|-----------|-------------|
| `spot` | `spot` | `spot` | Spot trading market. |
| `um` | `um` | `futures/um` | USD-M perpetual and delivery futures. |
| `cm` | `cm` | `futures/cm` | COIN-M perpetual and delivery futures. |

**`DataFrequency(StrEnum)`** — Archive partition frequency.

| Member | Value | Description |
|--------|-------|-------------|
| `daily` | `daily` | Files partitioned by day. |
| `monthly` | `monthly` | Files partitioned by month. |

**`DataType(StrEnum)`** — Dataset type on data.binance.vision.

| Member | Value | `has_interval_layer` |
|--------|-------|----------------------|
| `klines` | `klines` | `True` |
| `agg_trades` | `aggTrades` | `False` |
| `trades` | `trades` | `False` |
| `funding_rate` | `fundingRate` | `False` |
| `book_depth` | `bookDepth` | `False` |
| `book_ticker` | `bookTicker` | `False` |
| `index_price_klines` | `indexPriceKlines` | `True` |
| `mark_price_klines` | `markPriceKlines` | `True` |
| `premium_index_klines` | `premiumIndexKlines` | `True` |
| `metrics` | `metrics` | `False` |
| `liquidation_snapshot` | `liquidationSnapshot` | `False` |

> Member names use `snake_case`; values match the S3 path segment exactly.

The `has_interval_layer` property returns `True` for data types whose archive
path includes an interval directory segment (e.g. `1m`, `1h`) between the symbol
and the data files. It is the single source of truth for the
`interval`-vs-`data_type` consistency rule enforced by the archive client, the
`ArchiveListFilesWorkflow` constructor, and the `archive list-files` CLI command.

**`ContractType(StrEnum)`** — Futures contract settlement style.

| Member | Value | Description |
|--------|-------|-------------|
| `perpetual` | `perpetual` | Perpetual futures contract. |
| `delivery` | `delivery` | Dated delivery futures contract. |

## `common.types`

**`SymbolInfoBase`** — Shared fields for parsed symbol metadata.

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | `str` | Original input symbol, including any settled suffix. |
| `base_asset` | `str` | Parsed base asset. |
| `quote_asset` | `str` | Parsed quote asset. |

**`SpotSymbolInfo(SymbolInfoBase)`**

| Field | Type | Description |
|-------|------|-------------|
| `is_leverage` | `bool` | Whether the base asset is a leveraged token such as `BTCUP`. |
| `is_stable_pair` | `bool` | Whether both base and quote assets are in `STABLECOINS`. |

**`UmSymbolInfo(SymbolInfoBase)`**

| Field | Type | Description |
|-------|------|-------------|
| `contract_type` | `ContractType` | Whether the contract is perpetual or delivery. |
| `is_stable_pair` | `bool` | Whether both base and quote assets are in `STABLECOINS`. |

**`CmSymbolInfo(SymbolInfoBase)`**

| Field | Type | Description |
|-------|------|-------------|
| `contract_type` | `ContractType` | Whether the contract is perpetual or delivery. |

**`SymbolInfo`** — Union alias for `SpotSymbolInfo | UmSymbolInfo | CmSymbolInfo`.

## `common.logging`

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
sub-command runs; see [CLI — Root Callback and Verbosity](bhds/cli.md#root-callback-and-verbosity).
It is introduced as a shared helper so future CLI entry points (for example a
planned `bmds`) can reuse the same configuration without duplication.

## `common.symbols`

The symbol helpers accept raw Binance symbols and return typed metadata objects. They support the
settled suffix forms `_SETTLED`, `_SETTLED1`, and `SETTLED` across spot, USD-M, and COIN-M inputs.

| Function | Return Type | Description |
|----------|-------------|-------------|
| `infer_spot_info(symbol)` | `SpotSymbolInfo \| None` | Parse a spot symbol by matching `QUOTE_ASSETS` suffixes. |
| `infer_um_info(symbol)` | `UmSymbolInfo \| None` | Parse a USD-M futures symbol and infer perpetual vs delivery from the remaining underscore suffix. |
| `infer_cm_info(symbol)` | `CmSymbolInfo \| None` | Parse a COIN-M futures symbol in `BASEUSD_PERP` or `BASEUSD_YYMMDD` form. |

---

See also: [Architecture](../architecture.md) for how common types flow through the layered design.
