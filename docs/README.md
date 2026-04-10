# Documentation

Developer documentation for `binance-datatool`.

## Architecture

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | Layered design, data flow, S3 protocol, and key decisions. |

## Module Reference

| Module | Description |
|--------|-------------|
| [common](reference/common.md) | Shared enums, constants, and types. |
| [bhds.archive](reference/bhds/archive.md) | S3 listing client for data.binance.vision. |
| [bhds.workflow](reference/bhds/workflow.md) | Business logic orchestration. |
| [bhds.cli](reference/bhds/cli.md) | Typer CLI commands. |

## Guides

| Document | Description |
|----------|-------------|
| [Extending the Project](extending.md) | How to add commands, enums, workflows, and tests. |
