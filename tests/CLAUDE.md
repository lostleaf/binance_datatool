# tests/CLAUDE.md

This file is part of the parent CLAUDE.md system. It contains comprehensive testing documentation for the Binance Historical Data Service (BHDS).

## Testing Overview

Test files are located in the `tests/` directory and cover all major components of the system.

## Test Files

### Core AWS Components
- **`aws_client.py`**: AWS S3 client tests - Tests Binance AWS data directory access and symbol file listing
- **`aws_downloader.py`**: Downloader tests - Tests the AWS download functionality with actual data
- **`local_aws_client.py`**: Local file management tests - Tests offline file management and verification
- **`checksum.py`**: Checksum verification tests - Tests MD5 checksum validation for downloaded files
- **`path_builder.py`**: Path building tests - Tests AWS path construction utilities

### Data Processing
- **`parser.py`**: Unified CSV parser tests - Tests CSV parsing functionality
- **`symbol_filter.py`**: Symbol filtering tests - Tests symbol filtering and selection logic
- **`csv_conv.py`**: CSV conversion tests - Tests CSV to Parquet conversion utilities

### API Completion System
- **`kline_comp.py`**: Kline detector + DataExecutor integration tests - Tests kline data completion workflow
- **`funding_comp.py`**: Funding rate detector + DataExecutor integration tests - Tests funding rate completion workflow

### Holographic Kline System
- **`holo_merger.py`**: Holographic 1-minute kline synthesis tests - Tests Holo1mKlineMerger functionality
- **`gap_detector.py`**: Gap detection for holographic kline data - Tests gap detection in kline data

### Utilities
- **`infer_exginfo.py`**: Exchange info tests - Tests exchange information inference
- **`log_kit.py`**: Logging utilities tests - Tests logging system functionality
- **`test_utils.py`**: Shared test utilities - Common testing helpers and fixtures

## Running Tests

### Individual Test Execution Examples
```bash
# Run specific test files
uv run python tests/local_aws_client.py  # Test local file management
uv run python tests/holo_merger.py  # Test holographic kline synthesis
```

## Testing Patterns

Most tests work with sample data that gets automatically generated. For AWS-related tests:
- Use `CRYPTO_BASE_DIR` from environment variable, `~/crypto_data` as default value if not set
- Use `aws_data_dir = Path.home() / "crypto_data" / "binance_data" / "aws_data"` when needs to **read** data samples.
    Assume some csv zips are already downloaded to this directory.
- Use `http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")` when needs to **download** data samples.

### Test Structure
Each test file follows this pattern:
1. **Setup**: Create test data or use existing fixtures
2. **Execution**: Run the functionality being tested
3. **Verification**: Assert expected outcomes
4. **Cleanup**: Remove temporary files if needed

### Test Isolation
Each test is designed to be independent:
- No dependencies between test files
- Clean state for each test run
- Proper cleanup of temporary resources

### Debugging Tests
Tests include detailed logging and error reporting:
- Use `logger.debug()` for detailed test output
- Check test files for specific debug flags
- Most tests print sample data structures for verification

## Notes

- Tests are designed to be self-contained and require minimal setup
- All tests use English for comments and logging
- Test files follow the same coding standards as the main codebase
- See individual test files for specific usage examples and edge cases