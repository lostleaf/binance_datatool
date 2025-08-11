import re

from bdt_common.constants import LEVERAGE_EXCLUDES, LEVERAGE_SUFFIXES, QUOTES, STABLECOINS
from bdt_common.enums import ContractType

def infer_spot_info(symbol: str) -> dict | None:
    """
    Infer spot trading pair information from a given symbol.

    Args:
        symbol (str): The trading pair symbol (e.g., "BTCUSDT", "ETHUSDT", "ADAUPUSDT")

    Returns:
        dict or None: A dictionary containing the parsed information if successful,
                     None if no valid quote asset is found in the symbol.

        Dictionary structure:
        - symbol (str): Original symbol string
        - quote_asset (str): The quote currency (e.g., "USDT", "BTC", "ETH")
        - base_asset (str): The base currency (e.g., "BTC", "ETH", "ADAUP")
        - is_leverage (bool): True if the base asset is a leveraged token
        - is_stable_pair (bool): True if both base and quote are stablecoins

    Examples:
        >>> infer_spot_info("BTCUSDT")
        {'symbol': 'BTCUSDT', 'quote_asset': 'USDT', 'base_asset': 'BTC', 'is_leverage': False, 'is_stable_pair': False}
    """
    symbol_original = symbol

    # Remove SP_{number} prefix that appears when symbols are delisted/relisted in BHDS
    symbol = re.sub(r"^SP\d+_", "", symbol)
    for quote in QUOTES:
        if symbol.endswith(quote):
            base = symbol[: -len(quote)]
            is_leverage = base.endswith(LEVERAGE_SUFFIXES) and base not in LEVERAGE_EXCLUDES
            is_stable_pair = (base in STABLECOINS) and (quote in STABLECOINS)
            return {
                "symbol": symbol_original,
                "quote_asset": quote,
                "base_asset": base,
                "is_leverage": is_leverage,
                "is_stable_pair": is_stable_pair,
            }
    return None


def infer_um_futures_info(symbol: str):
    """
    Infer USDⓈ-M futures (UM) contract information from a given symbol.

    Args:
        symbol (str): The futures contract symbol (e.g., "BTCUSDT", "ETHUSDT", "BTCUSDT_240927")

    Returns:
        dict or None: A dictionary containing the parsed information if successful,
                     None if no valid quote asset is found in the symbol.

        Dictionary structure:
        - symbol (str): Original symbol string
        - quote_asset (str): The quote currency (e.g., "USDT", "BUSD")
        - base_asset (str): The base currency (e.g., "BTC", "ETH")
        - contract_type (ContractType): The type of futures contract (perpetual or delivery)
        - is_stable_pair (bool): True if both base and quote are stablecoins

    Examples:
        >>> infer_um_futures_info("BTCUSDT")
        {'symbol': 'BTCUSDT', 'quote_asset': 'USDT', 'base_asset': 'BTC', 'contract_type': ContractType.perpetual,
        'is_stable_pair': False}
    """
    symbol_original = symbol

    # Remove SP_{number} prefix that appears when symbols are delisted/relisted in BHDS
    symbol = re.sub(r"^SP\d+_", "", symbol)

    # Determine contract type based on underscore suffix
    # Delivery contracts have expiration date suffix (e.g., "_240927" for September 27, 2024)
    if "_" in symbol:
        contract_type = ContractType.delivery
        symbol = symbol.split("_")[0]
    else:
        contract_type = ContractType.perpetual

    # Extract base and quote assets by checking against known quote currencies
    for quote in QUOTES:
        if symbol.endswith(quote):
            base = symbol[: -len(quote)]
            is_stable_pair = base in STABLECOINS and quote in STABLECOINS
            return {
                "symbol": symbol_original,
                "quote_asset": quote,
                "base_asset": base,
                "contract_type": contract_type,
                "is_stable_pair": is_stable_pair,
            }
    return None


def infer_cm_futures_info(symbol: str):
    """
    Infer coin-margined futures (CM) contract information from a given symbol.

    Args:
        symbol (str): The futures contract symbol (e.g., "BTCUSD_PERP", "ETHUSD_240927")

    Returns:
        dict or None: A dictionary containing the parsed information if successful,
                     None if the symbol format is invalid or suffix is not recognized.

        Dictionary structure:
        - symbol (str): Original symbol string
        - quote_asset (str): Always "USD" for coin-margined futures
        - base_asset (str): The base currency (e.g., "BTC", "ETH")
        - contract_type (ContractType): The type of futures contract (perpetual or delivery)

    Examples:
        >>> infer_cm_futures_info("BTCUSD_PERP")
        {'symbol': 'BTCUSD_PERP', 'quote_asset': 'USD', 'base_asset': 'BTC', 'contract_type': ContractType.perpetual}

        >>> infer_cm_futures_info("ETHUSD_240927")
        {'symbol': 'ETHUSD_240927', 'quote_asset': 'USD', 'base_asset': 'ETH', 'contract_type': ContractType.delivery}
    """
    # Coin-margined futures symbols must contain underscore separator
    if "_" not in symbol:
        return None

    # Split symbol into underlying asset and contract suffix
    underlying, suffix = symbol.split("_")

    # Determine contract type based on suffix
    # "PERP" indicates perpetual contracts, numeric suffix indicates delivery contracts
    if suffix == "PERP":
        contract_type = ContractType.perpetual
    elif suffix.isdigit():
        contract_type = ContractType.delivery
    else:
        # Invalid suffix format
        return None

    # Coin-margined futures always have "USD" as part of underlying symbol
    base_asset = underlying[:-3]

    return {
        "symbol": symbol,
        "quote_asset": "USD",
        "base_asset": base_asset,
        "contract_type": contract_type,
    }
