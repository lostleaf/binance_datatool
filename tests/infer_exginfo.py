import json

from bdt_common.infer_exginfo import infer_cm_futures_info, infer_spot_info, infer_um_futures_info


def pretty(obj) -> str:
    return json.dumps(obj, indent=2, sort_keys=True, default=str)


ess_spot = [
    # Basic spot pairs
    "BTCUSDT",
    "ETHBTC",
    # Leveraged token example (if supported by constants)
    "BNBUPUSDT",
    "BNBBULLUSDT",
    # Could be misinterpreted as leverage token
    "JUPUSDT",
    # Stablecoin vs stablecoin
    "USDCUSDT",
    # With SP_ prefix (delisted/relisted pattern)
    "SP0_LUNAUSDT",
]


ess_um = [
    # Perpetual (no suffix)
    "BTCUSDT",
    # Delivery (date suffix)
    "ETHUSDT_240927",
    # With SP_ prefix (should be treated as perpetual after stripping)
    "SP0_BNXUSDT",
    # Another perpetual with different quote
    "USDCUSDT",
]


ess_cm = [
    # Perpetual
    "BTCUSD_PERP",
    # Delivery (date suffix)
    "ETHUSD_240927",
]


def run_spot_tests():
    print("=== Spot ===")
    for s in ess_spot:
        res = infer_spot_info(s)
        print(f"Symbol: {s}")
        print(pretty(res))
        print("-")


def run_um_tests():
    print("=== USDⓈ-M Futures (UM) ===")
    for s in ess_um:
        res = infer_um_futures_info(s)
        print(f"Symbol: {s}")
        print(pretty(res))
        print("-")


def run_cm_tests():
    print("=== Coin-Margined Futures (CM) ===")
    for s in ess_cm:
        res = infer_cm_futures_info(s)
        print(f"Symbol: {s}")
        print(pretty(res))
        print("-")


if __name__ == "__main__":
    run_spot_tests()
    run_um_tests()
    run_cm_tests()
