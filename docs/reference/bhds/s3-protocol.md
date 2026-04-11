# S3 Listing Protocol

The archive client communicates with data.binance.vision's S3-compatible XML listing API.

## Request Format

```
GET https://s3-ap-northeast-1.amazonaws.com/data.binance.vision
    ?delimiter=/&prefix=data/spot/daily/klines/
```

## Response Structure

**Directory listing (simplified):**

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

**File listing (simplified):**

```xml
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-03.zip</Key>
    <LastModified>2026-04-01T08:06:34.000Z</LastModified>
    <Size>1048</Size>
  </Contents>
  <Contents>
    <Key>data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-03.zip.CHECKSUM</Key>
    <LastModified>2026-04-01T08:06:34.000Z</LastModified>
    <Size>105</Size>
  </Contents>
</ListBucketResult>
```

## Key Behaviors

| Behavior | Detail |
|-----------|--------|
| **Pagination** | S3 returns at most 1000 entries per page. When `IsTruncated` is `"true"`, the client uses `NextMarker`, or falls back to the last emitted prefix (directory listing) or the last emitted key (file listing) to fetch the next page. |
| **xmltodict normalization** | When only one `CommonPrefixes` or `Contents` element exists, `xmltodict` returns a `dict` instead of a `list`. The `_normalize_entries()` helper handles both shapes. |
| **Retry with backoff** | HTTP requests use `tenacity` with exponential backoff (up to 5 attempts), retrying only on `aiohttp.ClientError` and `asyncio.TimeoutError`. |
| **Timestamp parsing** | `Contents.LastModified` strings are parsed via `datetime.fromisoformat` (with a `Z` → `+00:00` substitution) so every `ArchiveFile.last_modified` is tz-aware in UTC. |

## Proxy Support

`ArchiveClient` creates `aiohttp.ClientSession` with `trust_env=True` by default. This means
the session reads proxy configuration from standard environment variables:

- `http_proxy` / `HTTP_PROXY`
- `https_proxy` / `HTTPS_PROXY`
- `no_proxy` / `NO_PROXY`

No additional proxy configuration is needed at the application level.

---

See also: [Archive client](archive.md) | [Architecture](../../architecture.md)
