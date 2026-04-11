# binance_datatool.common.types

Typed metadata records for parsed symbol information.

**`SymbolInfoBase`** — Shared fields for parsed symbol metadata.

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | `str` | Original input symbol, including any settled suffix. |
| `base_asset` | `str` | Parsed base asset. |
| `quote_asset` | `str` | Parsed quote asset. |

**`SpotSymbolInfo(SymbolInfoBase)`**

| Field | Type | Description |
|-------|------|-------------|
| `is_leverage` | `bool` | Whether the base asset is a leveraged token such as `BTCUP`. |
| `is_stable_pair` | `bool` | Whether both base and quote assets are in `STABLECOINS`. |

**`UmSymbolInfo(SymbolInfoBase)`**

| Field | Type | Description |
|-------|------|-------------|
| `contract_type` | `ContractType` | Whether the contract is perpetual or delivery. |
| `is_stable_pair` | `bool` | Whether both base and quote assets are in `STABLECOINS`. |

**`CmSymbolInfo(SymbolInfoBase)`**

| Field | Type | Description |
|-------|------|-------------|
| `contract_type` | `ContractType` | Whether the contract is perpetual or delivery. |

**`SymbolInfo`** — Union alias for `SpotSymbolInfo | UmSymbolInfo | CmSymbolInfo`.

---

See also: [enums](enums.md) | [symbols](symbols.md) | [Architecture](../../architecture.md)
