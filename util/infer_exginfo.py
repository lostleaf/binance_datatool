from config.config import ContractType


def infer_spot_info(symbol: str):
    quotes = ['USDT', 'USDC', 'BTC', 'ETH']
    leverage_suffixes = ('UP', 'DOWN', 'BULL', 'BEAR')
    for quote in quotes:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            is_leverage = base.endswith(leverage_suffixes) and base != 'JUP'
            return {
                'symbol': symbol,
                'quote_asset': quote,
                'base_asset': base,
                'is_leverage': is_leverage,
            }
    return None


def infer_um_futures_info(symbol: str):
    symbol_original = symbol
    quotes = ['USDT', 'USDC', 'BTC']

    if '_' in symbol:
        contract_type = ContractType.delivery
        symbol = symbol.split('_')[0]
    else:
        contract_type = ContractType.perpetual

    for quote in quotes:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return {
                'symbol': symbol_original,
                'quote_asset': quote,
                'base_asset': base,
                'contract_type': contract_type,
            }
    return None


def infer_cm_futures_info(symbol: str):
    if '_' not in symbol:
        return None

    underlying, suffix = symbol.split('_')

    if suffix == 'PERP':
        contract_type = ContractType.perpetual
    elif suffix.isdigit():
        contract_type = ContractType.delivery
    else:
        return None

    return {
        'symbol': symbol,
        'quote_asset': 'USD',
        'base_asset': underlying[:-3],
        'contract_type': contract_type,
    }
