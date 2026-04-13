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
│   ├── path.py              # BHDS_HOME resolution and BhdsHomeNotConfiguredError
│   ├── types.py             # SymbolInfoBase, SpotSymbolInfo, UmSymbolInfo, CmSymbolInfo
│   └── symbols.py           # infer_spot_info, infer_um_info, infer_cm_info
│
└── bhds/                    # Binance Historical Data Service
    ├── __init__.py
    │
    ├── archive/             # S3 data access, typed filters, and download helpers
    │   ├── __init__.py      # Re-exports client, filter, and downloader symbols
    │   ├── client.py        # HTTP client, XML parsing, ArchiveFile metadata
    │   ├── downloader.py    # aria2c batch downloader with retry and proxy control
    │   └── filter.py        # Spot/Um/Cm symbol filters and build_symbol_filter()
    │
    ├── workflow/            # Business logic orchestration
    │   ├── __init__.py
    │   └── archive.py       # ArchiveListSymbolsWorkflow, ArchiveListFilesWorkflow, ArchiveDownloadWorkflow
    │
    └── cli/                 # Typer CLI layer
        ├── __init__.py      # Root callback with -v/-vv verbosity, --bhds-home; sub-command registration
        └── archive.py       # list-symbols, list-files, and download commands
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

## Data Flow

All CLI logging flows through `stderr`; stdout is reserved exclusively for command
results so that sub-commands remain safe to pipe. See the
[CLI overview](reference/bhds/cli/overview.md) for verbosity flag details.

Every CLI command follows the same three-layer call path: the CLI function parses
arguments, constructs a workflow, and presents the result. The workflow orchestrates
business logic and delegates S3 communication to the archive client. Per-command data
flow diagrams are documented alongside each command in the
[CLI reference](reference/bhds/cli/archive.md).
