# examples/CLAUDE.md

Examples demonstrating the BHDS library, inheriting root @CLAUDE.md system rules.

## Example scripts
- `kline_download_task.py` – download spot 1m klines via BHDS library; requires network access.
- `cm_futures_holo.py` – merge and gap-check cm_futures holo 1m klines; uses `execute_polars_batch` and local files.

## Run commands
- `uv run python examples/kline_download_task.py <out_dir>`
- `uv run python examples/cm_futures_holo.py <data_dir>`

## Notes
- Update this file whenever examples change.

