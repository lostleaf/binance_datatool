# binance_datatool — CLAUDE.md

## MUST
- Use English for code, comments, logs.
- Keep diffs small; don't touch `legacy/`.
- Run at least one test script before commit.

## Environment
- Python ≥ 3.12, [uv](https://docs.astral.sh/uv/), `aria2`.
- `CRYPTO_BASE_DIR` (default `~/crypto_data`); optional `HTTP_PROXY`.

## Commands
- setup: `uv sync && source .venv/bin/activate`
- format: `uv run black . && uv run isort .`
- cli example: `uv run bhds aws-download configs/download/spot_kline.yaml`
- library example: `uv run python examples/kline_download_task.py /path/to/data`
- test: `uv run tests/aws_client.py`

## Code Style
- Prefer `src/` over `legacy/`.
- Use `logger` from `bdt_common.log_kit`.
- Use Polars Lazy API; batch collect via `execute_polars_batch`.

## Directory Highlights
- `src/` – new modular CLI (`bhds/`), shared utils (`bdt_common/`).
- `configs/` – YAML task configs.
- `tests/` – executable scripts.

## Critical Docs
- `@docs/ARCHITECTURE.md` – project structure overview.
- `@configs/CLAUDE.md` – config fields & commands.
- `@tests/CLAUDE.md` – test catalog & usage.
- `@examples/CLAUDE.md` – library patterns.

## Don't
- Don't bypass checks (`--no-verify`) or commit large unrelated changes.
- Don't modify `legacy/` unless migrating.
