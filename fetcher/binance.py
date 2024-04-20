from util import async_retry_getter
from decimal import Decimal

from api.binance import BinanceMarketCMDapi, BinanceMarketSpotApi, BinanceMarketUMFapi, BinanceBaseMarketApi


def _get_from_filters(filters, filter_type, field_name):
    for f in filters:
        if f['filterType'] == filter_type:
            return f[field_name]


def _parse_usdt_futures_syminfo(info):
    filters = info['filters']
    return {
        'symbol': info['symbol'],
        'contract_type': info['contractType'],
        'status': info['status'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'margin_asset': info['marginAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(_get_from_filters(filters, 'LOT_SIZE', 'stepSize')),
        'min_notional_value': Decimal(_get_from_filters(filters, 'MIN_NOTIONAL', 'notional'))
    }


def _parse_coin_futures_syminfo(info):
    filters = info['filters']
    return {
        'symbol': info['symbol'],
        'contract_type': info['contractType'],
        'status': info['contractStatus'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'margin_asset': info['marginAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(info['contractSize'])
    }


def _parse_spot_syminfo(info):
    filters = info['filters']
    return {
        'symbol': info['symbol'],
        'status': info['status'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(_get_from_filters(filters, 'LOT_SIZE', 'stepSize')),
        'min_notional_value': Decimal(_get_from_filters(filters, 'NOTIONAL', 'minNotional'))
    }


class BinanceFetcher:

    TYPE_LIST = [
        (BinanceMarketUMFapi, 'usdt_futures', _parse_usdt_futures_syminfo),
        (BinanceMarketCMDapi, 'coin_futures', _parse_coin_futures_syminfo),
        (BinanceMarketSpotApi, 'spot', _parse_spot_syminfo),
    ]

    def __init__(self, market_api: BinanceBaseMarketApi):
        self.market_api = market_api
        self.trade_type = None

        for api_cls, trade_type, parse_func in self.TYPE_LIST:
            if isinstance(self.market_api, api_cls):
                self.trade_type = trade_type
                self.syminfo_parse_func = parse_func

        if self.trade_type is None:
            raise ValueError(f'Market Api {str(type(market_api))} not supported')

    async def get_exchange_info(self):
        """
        Parse trading rules from return values of /exchangeinfo API
        """
        exg_info = await async_retry_getter(self.market_api.aioreq_exchange_info)
        results = dict()
        for info in exg_info['symbols']:
            results[info['symbol']] = self.syminfo_parse_func(info)
        return results
