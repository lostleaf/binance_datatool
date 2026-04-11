# Architecture

This document describes the internal architecture of `binance-datatool`. It is intended for
contributors, maintainers, and AI agents working on the codebase.

## Package Tree

```
src/binance_datatool/
├── __init__.py              # Package root — exports __version__
├── py.typed                 # PEP 561 type stub marker
│
├── common/                  # Shared types and constants
│   ├── __init__.py          # Re-exports public symbols
│   ├── constants.py         # S3 settings, quote assets, stablecoins, leverage rules
│   ├── enums.py             # TradeType, DataFrequency, DataType, ContractType
│   ├── logging.py           # configure_cli_logging for CLI entry points
│   ├── types.py             # SymbolInfoBase, SpotSymbolInfo, UmSymbolInfo, CmSymbolInfo
│   └── symbols.py           # infer_spot_info, infer_um_info, infer_cm_info
│
└── bhds/                    # Binance Historical Data Service
    ├── __init__.py
    │
    ├── archive/             # S3 data access and typed filters
    │   ├── __init__.py      # Re-exports ArchiveClient, ArchiveFile, list_symbols, symbol filters
    │   ├── client.py        # HTTP client, XML parsing, ArchiveFile metadata
    │   └── filter.py        # Spot/Um/Cm symbol filters and build_symbol_filter()
    │
    ├── workflow/            # Business logic orchestration
    │   ├── __init__.py
    │   └── archive.py       # ArchiveListSymbolsWorkflow, ArchiveListFilesWorkflow, result dataclasses
    │
    └── cli/                 # Typer CLI layer
        ├── __init__.py      # Root callback with -v/-vv verbosity; sub-command registration
        └── archive.py       # list-symbols and list-files commands
```

## Layered Design

The package follows a strict three-layer architecture. Each layer depends only on the layers
below it — outer layers import inner layers, never the reverse.

```
CLI  (bhds/cli/)
 └─▶ Workflow  (bhds/workflow/)
       └─▶ Archive Client  (bhds/archive/)
             └─▶ Common  (common/)
```

| Layer | Package | Responsibility |
|-------|---------|----------------|
| **Common** | `binance_datatool.common` | Shared enums, constants, and types used across the project. |
| **Archive Client** | `binance_datatool.bhds.archive` | S3 HTTP communication with data.binance.vision. |
| **Workflow** | `binance_datatool.bhds.workflow` | Business logic orchestration; decouples CLI from the client. |
| **CLI** | `binance_datatool.bhds.cli` | Typer command definitions, argument parsing, output formatting. |

For detailed API docs see the [module reference](reference/).

### Why Three Layers?

- **Testability.** Workflows and clients can be tested independently of CLI parsing.
- **Composability.** Workflows can be reused from scripts or notebooks without importing Typer.
- **Extensibility.** Adding a new command means adding a thin CLI function that delegates to a
  workflow, without touching the archive client.

## CLI Entry Point and Logging

The root `bhds` Typer app exposes a `-v` / `-vv` verbosity flag on its callback. The
callback invokes `configure_cli_logging(verbosity)` from `common.logging` before any
sub-command runs, wiring `loguru` to `stderr` with the appropriate level and format:

| Verbosity | Level | Format |
|-----------|-------|--------|
| *(default)* | `WARNING` | `LEVEL: message` |
| `-v` | `INFO` | `LEVEL: message` |
| `-vv` | `DEBUG` | `HH:MM:SS.mmm \| LEVEL \| module:line - message` |

All CLI logging flows through `stderr`; stdout is reserved exclusively for command
results so that sub-commands remain safe to pipe into one another.

## Data Flow: `bhds archive list-symbols`

```
User runs:
  bhds archive list-symbols spot --quote USDT

  ┌──────────────────────────────────────────────────────┐
  │ CLI layer  (bhds/cli/archive.py)                     │
  │  Parses arguments, builds a typed symbol filter,     │
  │  runs the workflow, and prints matched symbols.      │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type,
                         │ symbol_filter
  ┌──────────────────────▼───────────────────────────────┐
  │ Workflow  (bhds/workflow/archive.py)                 │
  │  Fetches raw symbols, infers typed metadata per      │
  │  market, applies the filter, and returns a           │
  │  ListSymbolsResult.                                  │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type
  ┌──────────────────────▼───────────────────────────────┐
  │ Archive Client  (bhds/archive/client.py)             │
  │  Issues paginated S3 XML listings against            │
  │  data.binance.vision and returns sorted symbol       │
  │  names.                                              │
  └──────────────────────────────────────────────────────┘
```

## Data Flow: `bhds archive list-files`

`list-files` is the second archive listing command and is designed to compose with
`list-symbols` through a shell pipe. It reads one or more symbols, lists every file
under each symbol directory on `data.binance.vision`, and prints results in
caller-provided order.

```
User runs:
  bhds archive list-symbols um --quote USDT --exclude-stables \
    | bhds archive list-files um --type klines --interval 1m

  ┌──────────────────────────────────────────────────────┐
  │ CLI layer  (bhds/cli/archive.py)                     │
  │  Validates --only-zip / --only-checksum exclusivity  │
  │  and --interval vs data_type consistency; resolves   │
  │  symbols from positional args (winning) or piped     │
  │  stdin; uppercases each symbol; runs the workflow;   │
  │  prints short or TSV long output; logs per-symbol    │
  │  failures to stderr and exits 2 if any failed.       │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type,
                         │ symbols, interval
  ┌──────────────────────▼───────────────────────────────┐
  │ Workflow  (bhds/workflow/archive.py)                 │
  │  Re-validates interval consistency at construction;  │
  │  opens one shared aiohttp session; concurrently      │
  │  issues list_symbol_files per symbol via             │
  │  asyncio.gather(return_exceptions=True); wraps       │
  │  exceptions into SymbolListFilesResult.error and     │
  │  preserves caller input order.                       │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type,
                         │ symbol, interval, session
  ┌──────────────────────▼───────────────────────────────┐
  │ Archive Client  (bhds/archive/client.py)             │
  │  list_symbol_files builds the prefix and calls       │
  │  list_files_in_dir, which paginates S3 Contents      │
  │  entries into ArchiveFile dataclasses (key, size,    │
  │  last_modified tz-aware UTC).                        │
  └──────────────────────────────────────────────────────┘
```

The `interval` vs `data_type.has_interval_layer` consistency check is enforced at
**three layers** — CLI (`BadParameter`), Workflow (`__init__` `ValueError`), and
Client (`list_symbol_files` entry `ValueError`) — so each possible entry point
(shell, notebook importing the workflow, notebook importing the client directly)
fails loud on the same contract violation.

For S3 protocol details and proxy configuration, see the
[archive reference](reference/bhds/archive.md#s3-listing-protocol).
