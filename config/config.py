import os
import json

_DEFAULT_BASE_DIR = os.path.join(os.path.expanduser('~'), 'crypto_data')
_BASE_DIR = os.getenv('CRYPTO_BASE_DIR', _DEFAULT_BASE_DIR)
_CUR_DIR = os.path.dirname(os.path.realpath(__file__))


class CFG:
    BINANCE_DATA_DIR = os.path.join(_BASE_DIR, 'binance_data')
    BINANCE_QUANTCLASS_DIR = os.path.join(_BASE_DIR, 'binance_quantclass')
    BINANCE_EXGINFO_PATH = {
        'spot': os.path.join(_CUR_DIR, 'binance_exginfo_spot.json'),
        'coin_futures': os.path.join(_CUR_DIR, 'binance_exginfo_coin_futures.json'),
        'usdt_futures': os.path.join(_CUR_DIR, 'binance_exginfo_usdt_futures.json')
    }

    @property
    def BINANCE_CANDLE_SPLITS(self):

        def _load():
            return json.load(open(os.path.join(_CUR_DIR, 'binance_candle_split.json')))

        return self._get_with_cache('BINANCE_CANDLE_SPLITS', _load)

    @property
    def BINANCE_EXGINFO(self):

        def _load():
            return {k: json.load(open(p)) for k, p in self.BINANCE_EXGINFO_PATH.items()}

        return self._get_with_cache('BINANCE_EXGINFO', _load)

    N_JOBS = int(os.getenv('CRYPTO_NJOBS', os.cpu_count() - 1))

    _CACHE = dict()

    def _get_with_cache(self, key, _load):
        if key not in self._CACHE:
            self._CACHE[key] = _load()
        return self._CACHE[key]


Config = CFG()
