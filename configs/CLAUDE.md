# configs/CLAUDE.md

BHDS YAML configuration index, inheriting root @CLAUDE.md system rules.

## Directory Tree

```
configs/
├── download/   Download task configs
├── parsing/    Parsing task configs
└── holo_1m/    Holo 1-minute synthesis configs
```

## Entry Points

- **download**: [download/spot_kline.yaml](download/spot_kline.yaml), [download/um_kline.yaml](download/um_kline.yaml), etc.
- **parsing**: [parsing/spot_kline.yaml](parsing/spot_kline.yaml), [parsing/um_kline.yaml](parsing/um_kline.yaml), etc.
- **holo_1m**: [holo_1m/spot.yaml](holo_1m/spot.yaml), [holo_1m/um.yaml](holo_1m/um.yaml), [holo_1m/cm.yaml](holo_1m/cm.yaml)

## Core Fields

| Field | Allowed values |
| --- | --- |
| `data_type` | `klines`, `fundingRate`, `aggTrades`, `liquidationSnapshot`, `metrics` |
| `trade_type` | `spot`, `futures/um`, `futures/cm` |
| `data_freq` | `daily`, `monthly` |
| `time_interval` | `1m`, `5m`, `1h`, `1d` |

### Symbol Filter Options

| Key | Allowed values | Applies to |
| --- | --- | --- |
| `quote` | `USDT`, `USDC`, `BTC`, `ETH` | Spot, UM futures |
| `stable_pairs` | `true`, `false` | Spot, UM futures |
| `leverage_tokens` | `true`, `false` | Spot only |
| `contract_type` | `PERPETUAL`, `DELIVERY` | UM & CM futures |

### Environment Variables

- `CRYPTO_BASE_DIR`: Data root directory (default `~/crypto_data`)
- `HTTP_PROXY`: Proxy for downloads (optional)

## Common Commands

```bash
uv run bhds aws-download configs/download/spot_kline.yaml
uv run bhds parse-aws-data configs/parsing/um_kline.yaml
uv run bhds holo-1m-kline configs/holo_1m/cm.yaml
```

Update this index when configuration files change.


