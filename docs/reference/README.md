# Module Reference

Reference documentation for the importable modules and CLI packages in
`binance_datatool`.

## Common

| Module | Description |
|--------|-------------|
| [common.constants](common/constants.md) | S3 settings, quote assets, stablecoins, leverage rules. |
| [common.enums](common/enums.md) | TradeType, DataFrequency, DataType, ContractType. |
| [common.types](common/types.md) | SymbolInfoBase and per-market symbol info dataclasses. |
| [common.logging](common/logging.md) | `configure_cli_logging` helper for CLI entry points. |
| [common.path](common/path.md) | BHDS home directory resolution. |
| [common.symbols](common/symbols.md) | Symbol inference functions and quote parsing rules. |

## BHDS

| Module | Description |
|--------|-------------|
| [bhds.archive](bhds/archive.md) | S3 listing client for data.binance.vision. |
| [bhds.archive (S3 protocol)](bhds/s3-protocol.md) | S3 XML listing protocol, pagination, retry, and proxy. |
| [bhds.workflow](bhds/workflow.md) | Business logic orchestration. |
| [bhds.cli](bhds/cli/overview.md) | Typer CLI overview, verbosity, and sub-command index. |
| [bhds.cli.archive](bhds/cli/archive.md) | Archive commands (`list-symbols`, `list-files`, `download`, `verify`). |

---

See also: [Architecture](../architecture.md) | [Extending the Project](../extending.md)
