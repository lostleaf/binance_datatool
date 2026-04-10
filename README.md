# binance-datatool

`binance-datatool` is a modern Python package for exploring and managing Binance historical
market data.

The package is being rewritten around a `src/` layout and a thin CLI. Phase A provides the first
end-to-end archive command:

```bash
uv run bhds archive list-symbols spot
uv run bhds archive list-symbols um --data-type fundingRate --data-freq monthly
```

## Documentation

Detailed developer documentation lives in [`docs/`](docs/README.md):

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Layered design, data flow, S3 protocol, and key decisions. |
| [Module Reference](docs/reference/) | Per-subpackage API reference (common, archive, workflow, CLI). |
| [Extending the Project](docs/extending.md) | How to add commands, enums, workflows, and tests. |

## Development

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run pytest
```
