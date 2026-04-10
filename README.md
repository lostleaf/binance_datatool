# binance-datatool

`binance-datatool` is a modern Python package for exploring and managing Binance historical
market data.

The package is being rewritten around a `src/` layout and a thin CLI. Phase A provides the first
end-to-end archive command:

```bash
uv run bhds archive list-symbols spot
uv run bhds archive list-symbols um --data-type fundingRate --data-freq monthly
```

## Development

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run pytest
```
