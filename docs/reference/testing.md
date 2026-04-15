# Test Organization

```
tests/
├── conftest.py                 # Shared test support (FakeArchiveClient, sample_archive_files)
├── test_archive_client.py      # Archive client unit and integration tests
├── test_archive_workflow.py    # Workflow unit and integration tests (list + download + verify)
├── test_checksum.py            # checksum module unit tests (calc, read, verify_single_file)
├── test_cli.py                 # CLI smoke tests (list-symbols, list-files, download, verify)
├── test_downloader.py          # aria2 downloader unit tests (proxy, batching, retry)
├── test_enums.py               # common.enums property tests
├── test_filter.py              # Symbol filter unit tests
├── test_logging.py             # configure_cli_logging verbosity and format tests
├── test_path.py                # BHDS home resolution (override, env var, missing)
└── test_symbols.py             # Symbol inference unit and integration tests
```

## Conventions

- **Unit tests** use `monkeypatch` to replace HTTP methods with fake responses.
- **Integration tests** are marked with `@pytest.mark.integration` and skipped by default.
  Run them explicitly with `pytest --run-integration`.
- **CLI tests** use `typer.testing.CliRunner` and monkeypatch the workflow's `run()` method so
  they run without network access.

## Shared Test Support in `conftest.py`

- **`FakeArchiveClient`** — a programmable stub configured entirely via constructor
  kwargs (`symbols=`, `files_by_symbol=`, `errors_by_symbol=`). It implements both
  `list_symbols` and `list_symbol_files`. The `session=` kwarg is accepted for API
  parity with `ArchiveClient` but ignored, so stubs stay network-free regardless of
  what the workflow does with its shared session. Prefer extending this stub over
  re-implementing per-test fakes when you add new workflows.
- **`sample_archive_files`** — a representative pair of `ArchiveFile` entries
  (`.zip` + `.zip.CHECKSUM`) for list-files-style workflow and CLI tests.
- **`--run-integration`** — a custom pytest option that unlocks the `integration`
  marker. The default run skips every integration test.

---

See also: [Extending the Project](../extending.md) | [Architecture](../architecture.md)
