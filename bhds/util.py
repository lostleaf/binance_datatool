import os

STABLECOINS = {
    'BKRWUSDT', 'USDCUSDT', 'USDPUSDT', 'TUSDUSDT', 'BUSDUSDT', 'FDUSDUSDT', 'DAIUSDT', 'EURUSDT', 'GBPUSDT',
    'USBPUSDT', 'SUSDUSDT', 'PAXGUSDT', 'AEURUSDT'
}

BLACKLIST = {'NBTUSDT'}


def filter_symbols(symbols):
    lev_symbols = {x for x in symbols if x.endswith(('UPUSDT', 'DOWNUSDT', 'BEARUSDT', 'BULLUSDT')) and x != 'JUPUSDT'}
    not_usdt_symbols = {x for x in symbols if not x.endswith('USDT')}

    excludes = set.union(not_usdt_symbols, lev_symbols, STABLECOINS, BLACKLIST).intersection(symbols)

    symbols_filtered = sorted(set(symbols) - excludes)
    return symbols_filtered


def get_filtered_symbols(input_dir):
    symbols = sorted(os.path.splitext(x)[0] for x in os.listdir(input_dir))
    symbols = filter_symbols(symbols)
    return symbols
