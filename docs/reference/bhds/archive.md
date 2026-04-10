# binance_datatool.bhds.archive

S3 listing client for data.binance.vision.

## `ArchiveClient`

The main class for communicating with the Binance public data archive.

```python
from binance_datatool.bhds.archive import ArchiveClient

client = ArchiveClient(timeout_seconds=15, trust_env=True)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `timeout_seconds` | `15` | Total timeout in seconds per HTTP request. |
| `trust_env` | `True` | Read proxy settings from environment variables (`http_proxy`, etc.). |

**Public methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `list_dir` | `async (session, prefix) -> list[str]` | List child prefixes under an S3 prefix, handling pagination automatically. |
| `list_symbols` | `async (trade_type, data_freq, data_type) -> list[str]` | List sorted symbol names for a given archive path. Creates its own session internally. |

## `list_symbols` (module-level convenience function)

```python
from binance_datatool.bhds.archive import list_symbols

symbols = await list_symbols(TradeType.spot, DataFrequency.daily, DataType.klines)
```

Creates a temporary `ArchiveClient` and delegates to `ArchiveClient.list_symbols`.

## S3 Listing Protocol

The archive client communicates with data.binance.vision's S3-compatible XML listing API.

**Request format:**

```
GET https://s3-ap-northeast-1.amazonaws.com/data.binance.vision
    ?delimiter=/&prefix=data/spot/daily/klines/
```

**Response structure (simplified):**

```xml
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <NextMarker>...</NextMarker>
  <CommonPrefixes>
    <Prefix>data/spot/daily/klines/BTCUSDT/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>data/spot/daily/klines/ETHUSDT/</Prefix>
  </CommonPrefixes>
</ListBucketResult>
```

**Key behaviours:**

| Behaviour | Detail |
|-----------|--------|
| **Pagination** | S3 returns at most 1000 entries per page. When `IsTruncated` is `"true"`, the client uses `NextMarker` (or falls back to the last prefix) to fetch the next page. |
| **xmltodict normalisation** | When only one `CommonPrefixes` element exists, `xmltodict` returns a `dict` instead of a `list`. The `_normalize_entries()` helper handles this. |
| **Retry with backoff** | HTTP requests use `tenacity` with exponential backoff (up to 5 attempts), retrying only on `aiohttp.ClientError` and `asyncio.TimeoutError`. |

## Proxy Support

`ArchiveClient` creates `aiohttp.ClientSession` with `trust_env=True` by default. This means
the session reads proxy configuration from standard environment variables:

- `http_proxy` / `HTTP_PROXY`
- `https_proxy` / `HTTPS_PROXY`
- `no_proxy` / `NO_PROXY`

No additional proxy configuration is needed at the application level.

---

See also: [Architecture](../../architecture.md) | [Workflow](workflow.md)
