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
│   ├── types.py             # SymbolInfoBase, SpotSymbolInfo, UmSymbolInfo, CmSymbolInfo
│   └── symbols.py           # infer_spot_info, infer_um_info, infer_cm_info
│
└── bhds/                    # Binance Historical Data Service
    ├── __init__.py
    │
    ├── archive/             # S3 data access
    │   ├── __init__.py      # Re-exports ArchiveClient, list_symbols
    │   └── client.py        # HTTP client and XML parsing
    │
    ├── workflow/            # Business logic orchestration
    │   ├── __init__.py
    │   └── archive.py       # ArchiveListSymbolsWorkflow
    │
    └── cli/                 # Typer CLI layer
        ├── __init__.py      # App definition and sub-command registration
        └── archive.py       # list-symbols command
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

## Data Flow: `bhds archive list-symbols`

The `list-symbols` command is the only implemented command. Below is its end-to-end data flow.

```
User runs:
  bhds archive list-symbols spot --freq daily --type klines

  ┌─────────────────────────────────────────────────────┐
  │ CLI layer  (bhds/cli/archive.py)                    │
  │  • Typer parses arguments into typed enum values.   │
  │  • Creates ArchiveListSymbolsWorkflow.              │
  │  • Calls asyncio.run(workflow.run()).                │
  │  • Prints each symbol to stdout, one per line.      │
  └──────────────────────┬──────────────────────────────┘
                         │
  ┌──────────────────────▼──────────────────────────────┐
  │ Workflow layer  (bhds/workflow/archive.py)           │
  │  • ArchiveListSymbolsWorkflow.run() delegates       │
  │    to ArchiveClient.list_symbols().                  │
  └──────────────────────┬──────────────────────────────┘
                         │
  ┌──────────────────────▼──────────────────────────────┐
  │ Archive Client  (bhds/archive/client.py)            │
  │  • Builds S3 prefix: data/spot/daily/klines/        │
  │  • Creates aiohttp session (trust_env=True).        │
  │  • Fetches S3 XML listing pages with pagination.    │
  │  • Parses XML via xmltodict; normalises edge cases. │
  │  • Extracts and sorts symbol names from prefixes.   │
  └─────────────────────────────────────────────────────┘
```

For S3 protocol details and proxy configuration, see the
[archive reference](reference/bhds/archive.md#s3-listing-protocol).

## Enum Design

Enums in `common/enums.py` use `StrEnum` so their values work directly as Typer CLI arguments and
as string components in S3 path construction.

- **`TradeType`** — Values are CLI-friendly short names (`spot`, `um`, `cm`). The `s3_path`
  property maps to the actual S3 directory component (e.g. `um` → `futures/um`).
- **`DataFrequency`** and **`DataType`** — Values match the S3 path segment exactly, so no
  additional mapping is needed.
