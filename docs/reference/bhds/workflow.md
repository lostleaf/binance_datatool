# binance_datatool.bhds.workflow

Business logic orchestration layer between the CLI and the archive client.

## `ArchiveListSymbolsWorkflow`

Fetches raw symbols via `ArchiveClient`, infers typed metadata per market segment, and
optionally applies a typed symbol filter.

```python
from binance_datatool.bhds.archive import SpotSymbolFilter
from binance_datatool.bhds.workflow.archive import ArchiveListSymbolsWorkflow, ListSymbolsResult
from binance_datatool.common import DataFrequency, DataType, TradeType

workflow = ArchiveListSymbolsWorkflow(
    trade_type=TradeType.spot,
    data_freq=DataFrequency.daily,
    data_type=DataType.klines,
    symbol_filter=SpotSymbolFilter(
        quote_assets=frozenset({"USDT"}),
        exclude_leverage=True,
    ),
)
result: ListSymbolsResult = await workflow.run()
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_type` | *(required)* | Market segment to query. |
| `data_freq` | *(required)* | Partition frequency. |
| `data_type` | *(required)* | Dataset type. |
| `symbol_filter` | `None` | Optional typed filter applied to inferred metadata. `None` disables filtering. |
| `client` | `None` | Optional pre-configured `ArchiveClient`. A default client is created when omitted. |

### `run()`

```python
async def run(self) -> ListSymbolsResult
```

1. Calls `client.list_symbols()` for the configured market.
2. Dispatches each raw symbol through `infer_spot_info` / `infer_um_info` / `infer_cm_info`
   according to `trade_type`.
3. Splits the results into `inferred` and `unmatched` (raw strings that failed inference).
4. If `symbol_filter` is set, further splits `inferred` into `matched` and `filtered_out`
   via `symbol_filter.matches()`. Otherwise `matched == inferred` and `filtered_out` is empty.

## `ListSymbolsResult`

Structured return type for `ArchiveListSymbolsWorkflow.run()`. Declared as a `slots=True`
dataclass in the workflow module because it is the workflow's result shape, not a general
shared type.

| Field | Type | Description |
|-------|------|-------------|
| `matched` | `list[SymbolInfo]` | Inferred symbols that passed the filter (or all inferred symbols when no filter is set). |
| `unmatched` | `list[str]` | Raw symbols that could not be parsed by the per-market inference function. |
| `filtered_out` | `list[SymbolInfo]` | Inferred symbols rejected by the filter. Always empty when no filter is set. |

Input order is preserved across all three buckets.

---

See also: [Archive client](archive.md) | [CLI commands](cli.md)
