# binance_datatool.common

Shared types and constants imported by all other packages. The `__init__.py` re-exports every
public symbol so callers can write `from binance_datatool.common import TradeType`.

## Root Package

| Symbol | Type | Description |
|--------|------|-------------|
| `__version__` | `str` | Semantic version string (currently `0.1.0`). |

## `common.constants`

| Constant | Type | Value | Description |
|----------|------|-------|-------------|
| `S3_LISTING_PREFIX` | `str` | `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision` | Base URL for the S3 listing endpoint. |
| `S3_HTTP_TIMEOUT_SECONDS` | `int` | `15` | Default timeout per HTTP request. |

## `common.enums`

**`TradeType(StrEnum)`** — Binance market segment.

| Member | Value | `s3_path` | Description |
|--------|-------|-----------|-------------|
| `spot` | `spot` | `spot` | Spot trading market. |
| `um` | `um` | `futures/um` | USD-M perpetual and delivery futures. |
| `cm` | `cm` | `futures/cm` | COIN-M perpetual and delivery futures. |

**`DataFrequency(StrEnum)`** — Archive partition frequency.

| Member | Value | Description |
|--------|-------|-------------|
| `daily` | `daily` | Files partitioned by day. |
| `monthly` | `monthly` | Files partitioned by month. |

**`DataType(StrEnum)`** — Dataset type on data.binance.vision.

| Member | Value |
|--------|-------|
| `klines` | `klines` |
| `agg_trades` | `aggTrades` |
| `trades` | `trades` |
| `funding_rate` | `fundingRate` |
| `book_depth` | `bookDepth` |
| `book_ticker` | `bookTicker` |
| `index_price_klines` | `indexPriceKlines` |
| `mark_price_klines` | `markPriceKlines` |
| `premium_index_klines` | `premiumIndexKlines` |
| `metrics` | `metrics` |
| `liquidation_snapshot` | `liquidationSnapshot` |

> Member names use `snake_case`; values match the S3 path segment exactly.

---

See also: [Architecture](../architecture.md) for how common types flow through the layered design.
