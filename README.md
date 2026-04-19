# Binance DataTool - Built for Agents, Data Delivered.

[![PyPI](https://img.shields.io/pypi/v/binance-datatool?logo=pypi&logoColor=white)](https://pypi.org/project/binance-datatool/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-green)](LICENSE)
[![Ruff](https://img.shields.io/badge/code%20style-ruff-d4aa00?logo=ruff&logoColor=white)](https://docs.astral.sh/ruff/)
[![uv](https://img.shields.io/badge/package%20manager-uv-de5fe9?logo=uv&logoColor=white)](https://docs.astral.sh/uv/)

A non-official Python toolkit for managing historical market data from
[data.binance.vision](https://data.binance.vision/) — designed
from the ground up for AI agents and quantitative workflows.

**binance-datatool** is designed to make every command equally easy to call for AI agents and humans.:

- **Composable CLI** — stdin/stdout pipelines let agents chain
  commands without glue code
- **Atomic operations** — each command does one thing well; agents
  can inspect, decide, and act step by step
- **Deterministic output** — structured, parseable results that
  agents can reason about
- **Zero interaction** — no prompts, no confirmations; dry-run mode
  for safe previews

## Agent Skill

A packaged [Agent Skill](skills/binance-datatool/SKILL.md) is included in this repository. 

It teaches agents how to use `binance-datatool` to discover, download, and 
verify Binance historical market data — no manual command lookup required.

## Features

- **Multi-market coverage** — Spot, USD-M Futures, and COIN-M Futures
- **Smart symbol filtering** — filter by quote asset, exclude stablecoins
  and leveraged tokens, select contract types
- **Resumable batch downloads** — diff-based sync via
  [aria2](https://aria2.github.io/) that only fetches new or updated files
- **Data integrity verification** — SHA256 checksum validation with
  timestamped marker caching
- **Composable pipelines** — Unix-friendly design; commands read from
  stdin and write to stdout
- **Async I/O** — concurrent S3 listing and parallel checksum verification

## Quick Start

### Prerequisites

- [Python](https://www.python.org/) 3.11+
- [uv](https://docs.astral.sh/uv/) — fast Python package manager
- [aria2](https://aria2.github.io/) — required for the `download` command

### Install

```bash
# install the CLI
pipx install binance-datatool
# run `pipx ensurepath` if the command isn't found immediately

# system dependency for download command
# macOS
brew install aria2
# Ubuntu/Debian
sudo apt install aria2

export BINANCE_DATATOOL_ARCHIVE_HOME="$HOME/crypto_data/binance_archive"

binance-datatool --help
```

### Try it

```bash
# List USDT-quoted spot symbols, excluding stablecoins
binance-datatool list-symbols spot --quote USDT --exclude-stables

# Download daily 1m klines
binance-datatool list-symbols spot --quote USDT --exclude-stables \
  | binance-datatool download spot --type klines --interval 1m

# Verify integrity
binance-datatool verify spot --type klines --interval 1m BTCUSDT
```

## Supported Data Types

| Category | Types |
|----------|-------|
| Trade data | `klines`, `aggTrades`, `trades` |
| Derivatives | `fundingRate`, `liquidationSnapshot`, `metrics` |
| Order book | `bookDepth`, `bookTicker` |
| Index data | `indexPriceKlines`, `markPriceKlines`, `premiumIndexKlines` |

Each type is available in **daily** or **monthly** partitions. Kline types
support all standard intervals (`1m` `3m` `5m` `15m` `30m` `1h` `2h` `4h`
`6h` `8h` `12h` `1d` `3d` `1w` `1mo`).

## CLI Overview

The entry point is `binance-datatool`. All data commands are available
directly on the root app:

| Command | Description |
|---------|-------------|
| `list-symbols` | List available symbols on the remote archive |
| `list-files` | List archive files for given symbols |
| `download` | Download new or updated archive files |
| `verify` | Verify files against SHA256 checksums |

> [!TIP]
> Every command accepts symbols from **stdin** or as **positional args**,
> making it trivial to compose pipelines:
>
> ```bash
> binance-datatool list-symbols um --quote USDT \
>   | binance-datatool download um --type klines --interval 1m
> ```

Run `binance-datatool --help` for the full option reference.

## Architecture

```
CLI Layer        Thin Typer commands (argument parsing, output formatting)
    │
Workflow Layer   Business logic (diff computation, verification protocol)
    │
Archive Layer    S3 communication (HTTP listing, aria2 downloading, checksums)
    │
Common Layer     Shared types, enums, constants, and utilities
```

See [docs/architecture.md](docs/architecture.md) for a detailed design overview.

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Layered design, data flow, and key decisions |
| [Module Reference](docs/reference/README.md) | Per-subpackage API reference |
| [Extending](docs/extending.md) | How to add commands, enums, workflows, and tests |
| [Testing Guide](docs/reference/testing.md) | Test organization and conventions |
| [Agent Skill](skills/binance-datatool/SKILL.md) | Packaged skill for agents |
