# Archive Path Structure

S3 directory hierarchy used by data.binance.vision. For the XML listing protocol
that queries these paths, see [S3 protocol](s3-protocol.md).

## Top-level Tree

```
data/
├── spot/                       # Spot market
│   ├── daily/
│   └── monthly/
├── futures/
│   ├── um/                     # USD-M futures
│   │   ├── daily/
│   │   └── monthly/
│   └── cm/                     # COIN-M futures
│       ├── daily/
│       └── monthly/
└── option/                     # Options (BVOLIndex, EOHSummary — not covered by this tool)
    └── daily/
```

The first two path segments map directly to `TradeType.s3_path` and `DataFrequency`:

| `TradeType` | S3 path segment |
|-------------|-----------------|
| `spot` | `data/spot/` |
| `um` | `data/futures/um/` |
| `cm` | `data/futures/cm/` |

## Data Type Availability

The next path segment is the `DataType` value. Available data types vary by
market and frequency.

### spot

| `DataType` | daily | monthly |
|------------|:-----:|:-------:|
| `klines` | Y | Y |
| `aggTrades` | Y | Y |
| `trades` | Y | Y |

### futures/um

| `DataType` | daily | monthly |
|------------|:-----:|:-------:|
| `klines` | Y | Y |
| `aggTrades` | Y | Y |
| `trades` | Y | Y |
| `fundingRate` | — | Y |
| `bookDepth` | Y | — |
| `bookTicker` | Y | Y |
| `indexPriceKlines` | Y | Y |
| `markPriceKlines` | Y | Y |
| `premiumIndexKlines` | Y | Y |
| `metrics` | Y | — |

### futures/cm

| `DataType` | daily | monthly |
|------------|:-----:|:-------:|
| `klines` | Y | Y |
| `aggTrades` | Y | Y |
| `trades` | Y | Y |
| `fundingRate` | — | Y |
| `bookDepth` | Y | — |
| `bookTicker` | Y | Y |
| `indexPriceKlines` | Y | Y |
| `markPriceKlines` | Y | Y |
| `premiumIndexKlines` | Y | Y |
| `metrics` | Y | — |
| `liquidationSnapshot` | Y | — |

**Key rules:**

- `fundingRate` exists only under `monthly`. The `daily` prefix returns an empty listing.
- `bookDepth` and `metrics` exist only under `daily`.
- `liquidationSnapshot` exists only under `cm/daily`.

## Symbol Layer

Path pattern: `data/{trade_type}/{data_freq}/{data_type}/{symbol}/`

### Naming conventions

| Market | Contract style | Format | Examples |
|--------|----------------|--------|----------|
| spot | — | `{BASE}{QUOTE}` | `BTCUSDT`, `ETHUSDT`, `1000SHIBUSDT` |
| um (perpetual) | PERPETUAL | `{BASE}{QUOTE}` | `BTCUSDT`, `0GUSDT`, `1000BONKUSDT` |
| um (delivery) | DELIVERY | `{BASE}{QUOTE}_{YYMMDD}` | `BTCUSDT_260925`, `ETHUSDT_210326` |
| cm (perpetual) | PERPETUAL | `{BASE}USD_PERP` | `BTCUSD_PERP`, `ETHUSD_PERP` |
| cm (delivery) | DELIVERY | `{BASE}USD_{YYMMDD}` | `ADAUSD_200925`, `BTCUSD_250627` |

**Notes:**

- um perpetual symbols do **not** carry a `_PERP` suffix. A um symbol is a delivery
  contract only when its name ends with `_{YYMMDD}` (six digits). Most um symbols
  are perpetual.
- cm symbols always carry either `_PERP` or `_{YYMMDD}`.
- Some symbols have numeric prefixes: `1000BONKUSDT`, `1000000MOGUSDT`, `0GUSDT`.
- A `SETTLED` suffix marks delisted contracts: `CVCUSDTSETTLED`, `ICPUSDTSETTLED`.
- **cm kline-class data types use different symbol formats:**
  - `indexPriceKlines` — bare base pair `{BASE}USD` (e.g. `AAVEUSD`), no contract suffix.
  - `premiumIndexKlines` — perpetual contracts only (`{BASE}USD_PERP`).
  - `markPriceKlines` — both `_PERP` and `_{YYMMDD}` contracts.

## Interval Layer

Data types where `DataType.has_interval_layer` is `True` have an additional interval
directory between the symbol and the data files:

| `DataType` | Interval layer |
|------------|:--------------:|
| `klines` | Yes |
| `indexPriceKlines` | Yes |
| `markPriceKlines` | Yes |
| `premiumIndexKlines` | Yes |
| All other types | No |

### Available intervals

| Interval | spot `klines` | um/cm `klines` | um/cm `indexPriceKlines` / `markPriceKlines` / `premiumIndexKlines` |
|----------|:-------------:|:--------------:|:------------------------------------------------------------------:|
| `1s` | Y | — | — |
| `1m` | Y | Y | Y |
| `3m` | Y | Y | Y |
| `5m` | Y | Y | Y |
| `15m` | Y | Y | Y |
| `30m` | Y | Y | Y |
| `1h` | Y | Y | Y |
| `2h` | Y | Y | Y |
| `4h` | Y | Y | Y |
| `6h` | Y | Y | Y |
| `8h` | Y | Y | Y |
| `12h` | Y | Y | Y |
| `1d` | Y | Y | Y |
| `3d` | — | Y | Y |
| `1w` | — | Y | Y |
| `1mo` | — | Y | Y |

`1s` (second-level klines) is spot-only. `3d`, `1w`, and `1mo` are futures-only.

## File Naming

Each file covers a single time partition and sits directly under the symbol (or interval)
directory.

**daily files** (date format `YYYY-MM-DD`):

| Has interval layer | Pattern | Example |
|:------------------:|---------|---------|
| Yes | `{SYMBOL}-{interval}-{YYYY-MM-DD}.zip` | `BTCUSDT-1m-2024-01-01.zip` |
| No | `{SYMBOL}-{data_type}-{YYYY-MM-DD}.zip` | `BTCUSDT-aggTrades-2024-01-01.zip` |

**monthly files** (date format `YYYY-MM`):

| Has interval layer | Pattern | Example |
|:------------------:|---------|---------|
| Yes | `{SYMBOL}-{interval}-{YYYY-MM}.zip` | `BTCUSDT-1m-2024-01.zip` |
| No | `{SYMBOL}-{data_type}-{YYYY-MM}.zip` | `BTCUSDT-fundingRate-2024-01.zip` |

### Sidecar files

Every `.zip` file may have a companion checksum file:

| Suffix | Description |
|--------|-------------|
| `.zip.CHECKSUM` | SHA256 hash in the format `{hash}  {filename}`. |

## Path Hierarchy Summary

Kline-class data types (`has_interval_layer=True`):

```
data / {trade_type} / {data_freq} / {data_type} / {symbol} / {interval} / {filename}.zip
```

Non-kline data types:

```
data / {trade_type} / {data_freq} / {data_type} / {symbol} / {filename}.zip
```

### Full path examples

```
data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip
data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-01.zip
data/futures/cm/daily/klines/BTCUSD_PERP/1m/BTCUSD_PERP-1m-2024-01-01.zip
data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip

data/futures/um/daily/indexPriceKlines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip
data/futures/um/daily/markPriceKlines/BTCUSDT/5m/BTCUSDT-5m-2024-01-01.zip

data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip
data/futures/cm/monthly/fundingRate/BTCUSD_PERP/BTCUSD_PERP-fundingRate-2024-01.zip

data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-01.zip
data/futures/um/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-01.zip
data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2024-01-01.zip
data/futures/um/daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2024-01-01.zip
data/futures/um/daily/bookTicker/BTCUSDT/BTCUSDT-bookTicker-2024-01-01.zip
data/futures/cm/daily/liquidationSnapshot/BTCUSD_PERP/BTCUSD_PERP-liquidationSnapshot-2024-01-01.zip
```

---

See also: [Enums](../common/enums.md) | [S3 protocol](s3-protocol.md) | [Symbol directory](symbol_dir.md) | [Architecture](../../architecture.md)
