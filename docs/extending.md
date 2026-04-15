# Extending the Project

This guide explains how to add new functionality to `binance-datatool` following the patterns
established in the current codebase.

## Adding a New Enum Member

To support a new dataset type (for example, `"premiumIndex"`):

1. Add the member to the appropriate enum in `common/enums.py`:

   ```python
   class DataType(StrEnum):
       ...
       premium_index = "premiumIndex"
   ```

2. Review whether the new member needs additional enum-property updates. The new member
   automatically becomes available in:
   - CLI arguments (Typer picks up `StrEnum` members).
   - S3 path construction (the value is used directly).

3. If the new data type uses an interval directory layer, update
   `DataType.has_interval_layer` and add coverage for the interval validation
   paths exercised by the CLI, workflow, and archive client.

If the new member requires a non-trivial path mapping (like `TradeType.s3_path`), add a property
method to the enum class.

## Adding a New CLI Command

Follow the three-layer pattern: **CLI → Workflow → Client**.

### Step 1: Add a Client Method

If the command needs new data access, add an `async` method to `ArchiveClient` in
`bhds/archive/client.py`. Keep the method focused on S3 communication and return plain data
structures.

```python
async def list_dates(self, trade_type: TradeType, ...) -> list[str]:
    """List available date directories for a symbol."""
    prefix = _build_prefix(trade_type, data_freq, data_type) + f"{symbol}/"
    ...
```

See `list_symbols` and `list_symbol_files` in `bhds/archive/client.py` for real examples.

### Step 2: Create a Workflow Class

Create a workflow in `bhds/workflow/archive.py` (or a new module if the scope warrants it).
Accept an optional `client` parameter for testability and return a typed result dataclass.

```python
class ArchiveListDatesWorkflow:
    def __init__(self, trade_type: TradeType, ..., client: ArchiveClient | None = None) -> None:
        ...

    async def run(self) -> list[str]:
        ...
```

See `ArchiveListSymbolsWorkflow`, `ArchiveListFilesWorkflow`, `ArchiveDownloadWorkflow`,
and `ArchiveVerifyWorkflow` in `bhds/workflow/archive.py` for real examples.

### Step 3: Add a CLI Command

Add a Typer command in `bhds/cli/archive.py`. The command parses arguments, constructs a
workflow, and prints the result.

```python
@archive_app.command("list-dates")
def list_dates_command(
    trade_type: Annotated[TradeType, typer.Argument(...)],
    ...
) -> None:
    """List available dates for a symbol."""
    workflow = ArchiveListDatesWorkflow(trade_type, ...)
    for date in asyncio.run(workflow.run()):
        typer.echo(date)
```

See `list_symbols_command`, `list_files_command`, `download_command`, and
`verify_command` in `bhds/cli/archive.py` for real examples.

### Step 4: Add Tests

Add tests at each layer. See [Test Organization](reference/testing.md) for directory layout,
conventions, and shared fixtures.

## Adding a New Sub-command Group

To add a command group alongside `archive` (for example, `bhds holo ...`):

1. Define a new Typer app in `bhds/cli/__init__.py`:

   ```python
   holo_app = typer.Typer(name="holo", help="Holographic kline generation.")
   app.add_typer(holo_app)
   ```

2. Create the command module `bhds/cli/holo.py` and register it with a side-effect import in
   `bhds/cli/__init__.py`:

   ```python
   # Register sub-command modules (side-effect import).
   from binance_datatool.bhds.cli import archive as _archive  # noqa: F401,E402
   from binance_datatool.bhds.cli import holo as _holo  # noqa: F401,E402
   ```

3. Follow the same CLI → Workflow → Client layering for the new commands.

## Test Organization

For the test directory layout, conventions, and shared fixtures, see
[Test Organization](reference/testing.md).
