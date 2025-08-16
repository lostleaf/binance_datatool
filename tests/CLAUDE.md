# tests/CLAUDE.md

This file is part of the parent CLAUDE.md system. It contains comprehensive testing documentation for the Binance Historical Data Service (BHDS).

## Running Tests

Each script under `tests/` is executable on its own.

```bash
# run a single file
uv run tests/aws_client.py
```

Environment variables:

- `CRYPTO_BASE_DIR`: data storage path (defaults to `~/crypto_data`).
- `HTTP_PROXY` (optional): set only when a network proxy is required.

## Test Files

### AWS Basics
- **`aws_client.py`**: Accesses Binance AWS directories and lists symbol files.
- **`aws_downloader.py`**: Downloads sample archives from AWS.
- **`local_aws_client.py`**: Manages local AWS data and verifies files.
- **`checksum.py`**: Validates MD5 checksums of downloaded archives.
- **`path_builder.py`**: Constructs canonical AWS paths.

### Data Processing
- **`parser.py`**: Parses unified CSV records.
- **`symbol_filter.py`**: Selects symbols based on filtering rules.
- **`csv_conv.py`**: Converts CSV data to Parquet.

### API Completion
- **`kline_comp.py`**: Runs kline completion via detector and executor.
- **`funding_comp.py`**: Completes missing funding rates.

### Holo kline
- **`holo_merger.py`**: Generates holographic 1-minute klines.
- **`gap_detector.py`**: Detects gaps in kline sequences.
- **`splitter.py`**: Splits klines around detected gaps.

### Utilities
- **`infer_exginfo.py`**: Infers exchange information from symbols.
- **`log_kit.py`**: Shows logging utilities.
- **`test_utils.py`**: Provides shared test helpers.

## Test Style
- Tests are isolated and avoid interdependencies.
- Temporary files are removed during teardown.
- Logging uses `logger` from `bdt_common.log_kit`; see [`tests/log_kit.py`](log_kit.py) for examples.

## Notes
- Tests are self-contained and require minimal setup.
- All tests use English for comments and logging.
- Test files follow the same coding standards as the main codebase.
- See individual test files for specific usage examples and edge cases.

