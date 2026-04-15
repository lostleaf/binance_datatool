# bhds archive

Archive commands for querying and downloading data from data.binance.vision.

## `list-symbols`

```
bhds archive list-symbols <TRADE_TYPE>
    [--freq FREQ] [--type TYPE]
    [--quote QUOTE ...] [--exclude-leverage] [--exclude-stables]
    [--contract-type CONTRACT_TYPE]
```

| Argument / Option | Type | Default | Description |
|-------------------|------|---------|-------------|
| `TRADE_TYPE` | `TradeType` | *(required)* | Market segment (positional argument). |
| `--freq` | `DataFrequency` | `daily` | Partition frequency. |
| `--type` | `DataType` | `klines` | Dataset type. |
| `--quote` | `list[str]` | `None` | Filter by quote asset. Repeat to allow multiple values. Normalized to uppercase and deduplicated. |
| `--exclude-leverage` | `bool` | `False` | Exclude leveraged spot tokens. Ignored for USD-M and COIN-M. |
| `--exclude-stables` | `bool` | `False` | Exclude stablecoin pairs. Ignored for COIN-M. |
| `--contract-type` | `ContractType` | `None` | Restrict futures symbols to `perpetual` or `delivery`. Ignored for spot. |

Flags that do not apply to the chosen market are silently ignored. When every filter flag is
at its default, no filter is constructed and the workflow runs in unfiltered mode.

**Output:** one matched symbol per line to stdout. The CLI only prints
`ListSymbolsResult.matched`; raw symbols that failed inference and symbols that were
filtered out are not printed, regardless of whether any filter flag was passed.

**Examples:**

```
bhds archive list-symbols spot --quote USDT --exclude-leverage --exclude-stables
bhds archive list-symbols um --quote USDT --quote USDC --contract-type perpetual
bhds archive list-symbols cm --contract-type delivery
```

### Data flow

```
User runs:
  bhds archive list-symbols spot --quote USDT

  ┌──────────────────────────────────────────────────────┐
  │ CLI layer  (bhds/cli/archive.py)                     │
  │  Parses arguments, builds a typed symbol filter,     │
  │  runs the workflow, and prints matched symbols.      │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type,
                         │ symbol_filter
  ┌──────────────────────▼───────────────────────────────┐
  │ Workflow  (bhds/workflow/archive.py)                 │
  │  Fetches raw symbols, infers typed metadata per      │
  │  market, applies the filter, and returns a           │
  │  ListSymbolsResult.                                  │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type
  ┌──────────────────────▼───────────────────────────────┐
  │ Archive Client  (bhds/archive/client.py)             │
  │  Issues paginated S3 XML listings against            │
  │  data.binance.vision and returns sorted symbol       │
  │  names.                                              │
  └──────────────────────────────────────────────────────┘
```

## `list-files`

```
bhds archive list-files <TRADE_TYPE> [SYMBOLS...]
    [--freq FREQ] [--type TYPE] [--interval INTERVAL]
    [-l | --long]
    [--only-zip | --only-checksum]
```

Lists every file under one or more symbol directories on `data.binance.vision`,
preserving the caller-provided symbol order.

| Argument / Option | Type | Default | Description |
|-------------------|------|---------|-------------|
| `TRADE_TYPE` | `TradeType` | *(required)* | Market segment (positional argument). |
| `SYMBOLS` | `list[str]` | *(see below)* | Symbols to list, as variadic positional arguments. Accepts lowercase input; every symbol is normalized to uppercase before being passed to the workflow. |
| `--freq` | `DataFrequency` | `daily` | Partition frequency. |
| `--type` | `DataType` | `klines` | Dataset type. |
| `--interval` | `str \| None` | `None` | Kline interval directory. **Required** when `data_type.has_interval_layer` is `True` (the four kline-class data types); **rejected** otherwise. |
| `-l` / `--long` | `bool` | `False` | Switch from short relative-path output to a three-column TSV: `size_bytes<TAB>last_modified_iso<TAB>relative_path`. |
| `--only-zip` | `bool` | `False` | Print only `.zip` files. Mutually exclusive with `--only-checksum`. |
| `--only-checksum` | `bool` | `False` | Print only `.zip.CHECKSUM` files. Mutually exclusive with `--only-zip`. |

### Symbol input resolution

| State | Behavior |
|-------|----------|
| Positional args present | Use positional args, ignoring stdin (same convention as `grep` / `cat`). |
| No positional args and stdin is piped (`isatty()` is `False`) | Read `sys.stdin`, split on newlines, drop empty lines. |
| No positional args and stdin is a tty | Raise `typer.BadParameter` → exit 2. |

Every resolved symbol is stripped and uppercased before the workflow sees it, so
`btcusdt` and `BTCUSDT` behave identically.

### Output formats

**Short (default):** one relative path per line, `{symbol}[/{interval}]/{filename}`:

```
BTCUSDT/BTCUSDT-fundingRate-2026-03.zip
BTCUSDT/BTCUSDT-fundingRate-2026-03.zip.CHECKSUM
```

For kline-class data types the interval directory appears between symbol and
filename:

```
BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip
```

**Long (`-l` / `--long`):** three-column TSV, **no header row**, suitable for
piping into `awk` / `cut` / Polars:

```
1048	2026-04-01T08:06:34Z	BTCUSDT/BTCUSDT-fundingRate-2026-03.zip
105	2026-04-01T08:06:34Z	BTCUSDT/BTCUSDT-fundingRate-2026-03.zip.CHECKSUM
```

Columns are:

| # | Field | Format |
|---|-------|--------|
| 1 | `size_bytes` | Raw integer byte count. No human-readable suffix (so `awk '{sum+=$1}'` works). |
| 2 | `last_modified_iso` | ISO 8601 UTC second precision with `Z` suffix, produced via `datetime.strftime("%Y-%m-%dT%H:%M:%SZ")`. |
| 3 | `relative_path` | Same value as the short-format line. |

No header line is emitted; this matches `ls -l` / `find -printf` / `du` / `git ls-files`
conventions so the output is pipe-safe without `tail -n +2` gymnastics.

If every requested symbol is listed successfully but no files are found, the
command still exits `0` and prints a warning to stderr. Treat that warning as a
hint to verify the upstream directory layout on `data.binance.vision` for the
selected market, frequency, and data type rather than as a local runtime error.

### Exit codes

| Situation | Exit code |
|-----------|-----------|
| All symbols succeeded (empty results still count as success) | `0` |
| Any symbol failed | `2` (per-symbol errors are logged to stderr as `ERROR: {symbol}: {message}`) |
| Argument validation error (mutually exclusive flags, missing interval, extra interval, no symbols) | `2` |
| `KeyboardInterrupt` | `130` |

### Composition with `list-symbols`

`list-files` is designed to be piped from `list-symbols`:

```
bhds archive list-symbols um --quote USDT --exclude-stables \
  | bhds archive list-files um --type klines --interval 1m
```

### Examples

```
# Single-symbol funding-rate listing
bhds archive list-files um --freq monthly --type fundingRate BTCUSDT

# Multi-symbol klines listing (long TSV, zip-only)
bhds archive list-files um --type klines --interval 1m -l --only-zip BTCUSDT ETHUSDT

# Pipe from list-symbols, verbose logging
bhds -v archive list-symbols um --quote USDT --exclude-stables \
  | bhds -v archive list-files um --type klines --interval 1m
```

### Data flow

```
User runs:
  bhds archive list-symbols um --quote USDT --exclude-stables \
    | bhds archive list-files um --type klines --interval 1m

  ┌──────────────────────────────────────────────────────┐
  │ CLI layer  (bhds/cli/archive.py)                     │
  │  Validates --only-zip / --only-checksum exclusivity  │
  │  and --interval vs data_type consistency; resolves   │
  │  symbols from positional args (winning) or piped     │
  │  stdin; uppercases each symbol; runs the workflow;   │
  │  prints short or TSV long output; logs per-symbol    │
  │  failures to stderr and exits 2 if any failed.       │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type,
                         │ symbols, interval
  ┌──────────────────────▼───────────────────────────────┐
  │ Workflow  (bhds/workflow/archive.py)                 │
  │  Re-validates interval consistency at construction;  │
  │  opens one shared aiohttp session; concurrently      │
  │  issues list_symbol_files per symbol via             │
  │  asyncio.gather(return_exceptions=True); wraps       │
  │  exceptions into SymbolListFilesResult.error and     │
  │  preserves caller input order.                       │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type,
                         │ symbol, interval, session
  ┌──────────────────────▼───────────────────────────────┐
  │ Archive Client  (bhds/archive/client.py)             │
  │  list_symbol_files builds the prefix and calls       │
  │  list_files_in_dir, which paginates S3 Contents      │
  │  entries into ArchiveFile dataclasses (key, size,    │
  │  last_modified tz-aware UTC).                        │
  └──────────────────────────────────────────────────────┘
```

The `interval` vs `data_type.has_interval_layer` consistency check is enforced at
all three layers (CLI, Workflow, and Client) so every entry point fails loud on the
same contract violation. See the [archive client reference](../archive/client.md)
and [S3 protocol reference](../s3-protocol.md) for archive listing details.

## `download`

```
bhds [--bhds-home PATH] archive download <TRADE_TYPE> [SYMBOLS...]
    [--freq FREQ] [--type TYPE] [--interval INTERVAL]
    [-n | --dry-run]
    [--aria2-proxy]
```

Downloads new or updated archive files into the local BHDS data directory.
Requires `aria2c` to be available in `PATH`.

| Argument / Option | Type | Default | Description |
|-------------------|------|---------|-------------|
| `TRADE_TYPE` | `TradeType` | *(required)* | Market segment (positional argument). |
| `SYMBOLS` | `list[str]` | *(see below)* | Symbols to download, as variadic positional arguments. Same resolution rules as `list-files`. |
| `--freq` | `DataFrequency` | `daily` | Partition frequency. |
| `--type` | `DataType` | `klines` | Dataset type. |
| `--interval` | `str \| None` | `None` | Kline interval directory. Same validation as `list-files`. |
| `-n` / `--dry-run` | `bool` | `False` | Show what would be downloaded without writing files. Outputs TSV to stdout. |
| `--aria2-proxy` | `bool` | `False` | Allow aria2c to inherit system proxy environment variables. By default, proxy env vars (`HTTP_PROXY`, `HTTPS_PROXY`, etc.) are stripped from the aria2c subprocess. |

### BHDS home resolution

The download command requires a local data directory. Resolution priority:

| Priority | Source |
|----------|--------|
| 1 | `--bhds-home` (root-level option) |
| 2 | `$BHDS_HOME` environment variable |
| 3 | Exit 2 with a descriptive error |

See [`common.path`](../../common/path.md) for implementation details.

### Diff semantics

For each remote file, the workflow checks the local path at
`bhds_home/aws_data/{archive_key}`:

| Local state | Action | Reason |
|-------------|--------|--------|
| File does not exist | Download | `new` |
| File exists, `local_mtime < remote_last_modified` | Download | `updated` |
| File exists, `local_mtime >= remote_last_modified` | Skip | up to date |

When a file is classified as `updated`, any stale `.verified` markers for the
corresponding zip file are deleted before downloading. Both legacy markers
(`file.zip.verified`) and timestamped markers (`file.zip.<ts>.verified`) are
cleaned.

### Dry-run output

In dry-run mode (`-n`), the command prints a three-column TSV to stdout:

```
new	1048	BTCUSDT/BTCUSDT-fundingRate-2026-03.zip
updated	105	BTCUSDT/BTCUSDT-fundingRate-2026-03.zip.CHECKSUM
```

Columns: `reason<TAB>size_bytes<TAB>relative_path`. A summary line is printed
to stderr: `N files to download, M up to date`.

### Normal output

In download mode, progress is shown on stderr with:
- Scan summary: symbol count and file counts
- Per-batch status messages such as `Downloading batch ...` and `Retrying batch ...`
- A tqdm progress bar when stderr is a TTY
- Final summary: `Done: N downloaded, M failed, K skipped`

### Exit codes

| Situation | Exit code |
|-----------|-----------|
| All files downloaded successfully | `0` |
| Any symbol listing failed | `2` |
| Any download failed | `2` |
| BHDS_HOME not configured | `2` |
| Argument validation error | `2` |

### Composition with other commands

```
# Dry-run: preview what would be downloaded
bhds --bhds-home ~/data archive download um --freq monthly --type fundingRate -n BTCUSDT

# Download with proxy passthrough
bhds archive download um --type klines --interval 1m --aria2-proxy BTCUSDT ETHUSDT

# Pipe from list-symbols
bhds archive list-symbols um --quote USDT --exclude-stables \
  | bhds archive download um --type klines --interval 1m
```

### Data flow

```
User runs:
  bhds --bhds-home ~/data archive download um --type fundingRate BTCUSDT

  ┌──────────────────────────────────────────────────────┐
  │ CLI layer  (bhds/cli/archive.py)                     │
  │  Validates interval vs data_type; resolves symbols   │
  │  from args or stdin; resolves BHDS home from         │
  │  --bhds-home or $BHDS_HOME; constructs the download  │
  │  workflow; prints dry-run TSV or download summary;   │
  │  logs listing errors to stderr and exits 2 on fail.  │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type,
                         │ symbols, bhds_home, interval,
                         │ dry_run, inherit_aria2_proxy
  ┌──────────────────────▼───────────────────────────────┐
  │ Workflow  (bhds/workflow/archive.py)                 │
  │  Delegates to ArchiveListFilesWorkflow for remote    │
  │  listing; computes diff (new/updated/skipped) by     │
  │  comparing local timestamps; invalidates stale       │
  │  .verified markers; invokes aria2 downloader with    │
  │  batch retry.                                        │
  └──────────────────────┬───────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
  ┌───────▼────────────────┐   ┌────────▼──────────────────┐
  │ Archive Client         │   │ Downloader                │
  │  (bhds/archive/        │   │  (bhds/archive/           │
  │   client.py)           │   │   downloader.py)          │
  │  Concurrent S3 file    │   │  aria2c batch download    │
  │  listings              │   │  with retry and proxy     │
  │                        │   │  control                  │
  └────────────────────────┘   └───────────────────────────┘
```

## `verify`

```
bhds [--bhds-home PATH] archive verify <TRADE_TYPE> [SYMBOLS...]
    [--freq FREQ] [--type TYPE] [--interval INTERVAL]
    [--keep-failed]
    [-n | --dry-run]
```

Verifies local archive zip files against their sibling `.CHECKSUM` files using
SHA256. Requires `BHDS_HOME` to be configured (same resolution as `download`).

| Argument / Option | Type | Default | Description |
|-------------------|------|---------|-------------|
| `TRADE_TYPE` | `TradeType` | *(required)* | Market segment (positional argument). |
| `SYMBOLS` | `list[str]` | *(see below)* | Symbols to verify, as variadic positional arguments. Same resolution rules as `list-files` and `download`. |
| `--freq` | `DataFrequency` | `daily` | Partition frequency. |
| `--type` | `DataType` | `klines` | Dataset type. |
| `--interval` | `str \| None` | `None` | Kline interval directory. Same validation as `list-files`. |
| `--keep-failed` | `bool` | `False` | Keep failed zip and checksum files instead of deleting them. |
| `-n` / `--dry-run` | `bool` | `False` | Show what would be verified without computing checksums or modifying files. |

### Symbol input resolution

Same convention as `list-files` and `download`: positional args win over piped
stdin, every symbol is stripped and uppercased.

### Dry-run output

In dry-run mode (`-n`), the command prints one relative path per line to stdout
for each zip file that would be verified:

```
BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip
BTCUSDT/1m/BTCUSDT-1m-2024-01-02.zip
```

A summary line is printed to stderr:

```
12 to verify, 5 up to date, 0 orphan zip, 0 orphan checksum
```

If no local zip files match the requested selection, the command still exits `0`
and prints a warning to stderr asking you to re-check `--bhds-home`, the path
selection flags, and the symbol list.

### Normal output

In verify mode, results are printed to stderr:

```
Done: 12 verified, 0 failed, 5 skipped
```

If orphans were found, an additional line describes the cleanup:

```
Cleaned 1 orphan zip markers, deleted 2 orphan checksums
```

If `--keep-failed` is enabled and there are failures:

```
Failed files were kept because --keep-failed is enabled.
```

Per-file failures are logged to stderr as `ERROR: {filename}: {detail}`.

Like dry-run mode, a successful but empty local scan still emits a warning to
stderr instead of silently pretending everything is current.

### Orphan handling

| Orphan type | Action |
|-------------|--------|
| Zip without sibling `.CHECKSUM` | Clear all markers for the zip; **keep** the zip file. |
| `.CHECKSUM` without sibling `.zip` | Delete the checksum file and clear its markers. |

### Verified marker protocol

After a successful verification, the workflow writes a timestamped marker file
(`{zip_name}.{ts}.verified`) next to the zip. Subsequent runs skip the zip if the
marker timestamp is still fresh relative to the zip and checksum file mtimes.
Legacy markers without a timestamp are treated as invalid.

See the [workflow reference](../workflow.md#timestamped-marker-protocol) for
protocol details.

### Exit codes

| Situation | Exit code |
|-----------|-----------|
| All files verified successfully (including mismatches and orphan cleanup) | `0` |
| Runtime error (e.g. process pool failure) | `2` |
| BHDS_HOME not configured | `2` |
| Argument validation error | `2` |

### Composition with other commands

```
# Dry-run: preview what would be verified
bhds --bhds-home ~/data archive verify um --type klines --interval 1m -n BTCUSDT

# Verify and delete failed files (default)
bhds archive verify um --type klines --interval 1m BTCUSDT ETHUSDT

# Keep failed files for inspection
bhds archive verify um --freq monthly --type fundingRate --keep-failed BTCUSDT

# Pipe from list-symbols
bhds archive list-symbols um --quote USDT --exclude-stables \
  | bhds archive verify um --type klines --interval 1m
```

### Data flow

```
User runs:
  bhds --bhds-home ~/data archive verify um --type klines --interval 1m BTCUSDT

  ┌──────────────────────────────────────────────────────┐
  │ CLI layer  (bhds/cli/archive.py)                     │
  │  Validates interval vs data_type; resolves symbols   │
  │  from args or stdin; resolves BHDS home; constructs  │
  │  the verify workflow; prints dry-run paths or verify │
  │  summary; logs per-file failures to stderr.          │
  └──────────────────────┬───────────────────────────────┘
                         │ trade_type, data_freq, data_type,
                         │ symbols, bhds_home, interval,
                         │ keep_failed, dry_run
  ┌──────────────────────▼───────────────────────────────┐
  │ Workflow  (bhds/workflow/archive.py)                 │
  │  Scans local symbol directories; classifies zips     │
  │  into verify/skip/orphan buckets; cleans orphans;    │
  │  verifies SHA256 checksums in parallel via           │
  │  ProcessPoolExecutor (spawn); writes/clears markers; │
  │  optionally deletes failed files.                    │
  └──────────────────────┬───────────────────────────────┘
                         │ zip_path
  ┌──────────────────────▼───────────────────────────────┐
  │ Checksum  (bhds/archive/checksum.py)                 │
  │  verify_single_file reads the .CHECKSUM sibling,     │
  │  computes SHA256 via hashlib.file_digest, and        │
  │  returns VerifyFileResult (passed/failed + detail).  │
  └──────────────────────────────────────────────────────┘
```

---

See also: [CLI overview](README.md) | [Workflow layer](../workflow.md) |
[Archive package](../archive/) | [Archive client](../archive/client.md)
