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

2. No other changes are needed. The new member automatically becomes available in:
   - CLI arguments (Typer picks up `StrEnum` members).
   - S3 path construction (the value is used directly).

If the new member requires a non-trivial path mapping (like `TradeType.s3_path`), add a property
method to the enum class.

## Adding a New CLI Command

Follow the three-layer pattern: **CLI → Workflow → Client**.

### Step 1: Add a Client Method

If the command needs new data access, add a method to `ArchiveClient` in
`bhds/archive/client.py`. Keep the method focused on S3 communication and return plain data
structures.

```python
async def list_dates(
    self,
    trade_type: TradeType,
    data_freq: DataFrequency,
    data_type: DataType,
    symbol: str,
) -> list[str]:
    """List available date directories for a symbol.

    Args:
        trade_type: Market segment.
        data_freq: Partition frequency.
        data_type: Dataset type.
        symbol: Symbol name (e.g. ``"BTCUSDT"``).

    Returns:
        Sorted list of date directory names.
    """
    prefix = _build_prefix(trade_type, data_freq, data_type) + f"{symbol}/"
    async with self._create_session() as session:
        child_prefixes = await self.list_dir(session, prefix)
    return sorted(_extract_symbol(p) for p in child_prefixes)
```

### Step 2: Create a Workflow Class

Create a workflow in `bhds/workflow/archive.py` (or a new module if the scope warrants it).
The workflow accepts an optional `client` parameter for testability.

```python
class ArchiveListDatesWorkflow:
    """Workflow for listing available dates for a symbol."""

    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        symbol: str,
        client: ArchiveClient | None = None,
    ) -> None:
        """Initialise the workflow.

        Args:
            trade_type: Market segment to query.
            data_freq: Partition frequency.
            data_type: Dataset type.
            symbol: Symbol name.
            client: Optional pre-configured archive client.
        """
        ...

    async def run(self) -> list[str]:
        """Execute the workflow and return a sorted date list.

        Returns:
            Sorted list of date directory names.
        """
        return await self.client.list_dates(...)
```

### Step 3: Add a CLI Command

Add a Typer command in `bhds/cli/archive.py`:

```python
@archive_app.command("list-dates")
def list_dates_command(
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    symbol: Annotated[str, typer.Argument(help="Symbol name.")],
    data_freq: Annotated[
        DataFrequency,
        typer.Option("--freq", help="Partition frequency."),
    ] = DataFrequency.daily,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type."),
    ] = DataType.klines,
) -> None:
    """List available dates for a symbol under a Binance archive prefix."""
    workflow = ArchiveListDatesWorkflow(trade_type, data_freq, data_type, symbol)
    for date in asyncio.run(workflow.run()):
        typer.echo(date)
```

### Step 4: Add Tests

Add tests at each layer:

- **Unit test** — Monkeypatch `_fetch_xml` to supply mock XML responses and verify client logic
  (pagination, normalisation, sorting).
- **CLI test** — Use `typer.testing.CliRunner` with a monkeypatched workflow `run()` method.
- **Integration test** *(optional)* — Mark with `@pytest.mark.integration` for real network
  requests. These are skipped by default; run them with `pytest --run-integration`.

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

## Test Organisation

```
tests/
├── conftest.py                 # Shared fixtures and pytest configuration
├── test_archive_client.py      # Archive client unit and integration tests
└── test_cli.py                 # CLI smoke tests
```

**Conventions:**

- **Unit tests** use `monkeypatch` to replace HTTP methods with fake responses.
- **Integration tests** are marked with `@pytest.mark.integration` and skipped by default.
  Run them explicitly with `pytest --run-integration`.
- **CLI tests** use `typer.testing.CliRunner` and monkeypatch the workflow's `run()` method so
  they run without network access.
