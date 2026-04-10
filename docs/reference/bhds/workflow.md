# binance_datatool.bhds.workflow

Business logic orchestration layer between the CLI and the archive client.

## `ArchiveListSymbolsWorkflow`

```python
from binance_datatool.bhds.workflow.archive import ArchiveListSymbolsWorkflow

workflow = ArchiveListSymbolsWorkflow(
    trade_type=TradeType.spot,
    data_freq=DataFrequency.daily,
    data_type=DataType.klines,
    client=None,  # uses default ArchiveClient
)
symbols: list[str] = await workflow.run()
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_type` | *(required)* | Market segment to query. |
| `data_freq` | *(required)* | Partition frequency. |
| `data_type` | *(required)* | Dataset type. |
| `client` | `None` | Optional pre-configured `ArchiveClient`. |

---

See also: [Archive client](archive.md) | [CLI commands](cli.md)
