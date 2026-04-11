# Documentation

Developer documentation for contributors, maintainers, and AI agents working on the `binance-datatool` codebase. 

For installation and usage instructions, see the repository root README.

## Architecture

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | Layered design, data flow, S3 protocol, and key decisions. |

## Module Reference

| Module | Description |
|--------|-------------|
| [common.constants](reference/common/constants.md) | S3 settings, quote assets, stablecoins, leverage rules. |
| [common.enums](reference/common/enums.md) | TradeType, DataFrequency, DataType, ContractType. |
| [common.types](reference/common/types.md) | SymbolInfoBase and per-market symbol info dataclasses. |
| [common.logging](reference/common/logging.md) | `configure_cli_logging` helper for CLI entry points. |
| [common.symbols](reference/common/symbols.md) | Symbol inference functions and quote parsing rules. |
| [bhds.archive](reference/bhds/archive.md) | S3 listing client for data.binance.vision. |
| [bhds.workflow](reference/bhds/workflow.md) | Business logic orchestration. |
| [bhds.cli](reference/bhds/cli/overview.md) | Typer CLI overview, verbosity, and sub-command index. |
| [bhds.cli.archive](reference/bhds/cli/archive.md) | Archive listing commands (list-symbols, list-files). |

## Guides

| Document | Description |
|----------|-------------|
| [Extending the Project](extending.md) | How to add commands, enums, workflows, and tests. |
