import asyncio
import os
import shutil
from collections import defaultdict
from glob import glob
from pathlib import Path

import pandas as pd
from joblib import Parallel, delayed

from config import Config
from fetcher.binance import BinanceFetcher
from util import (DEFAULT_TZ, batched, convert_interval_to_timedelta, create_aiohttp_session)
from util.log_kit import get_logger, divider

from .aws_util import (aws_batch_list_dir, aws_download_symbol_files, aws_get_candle_dir, aws_list_dir)
from .bhds_util import read_candle_splits
from .checksum import verify_checksum

logger = get_logger()


async def get_aws_candle(type_, time_interval, symbols):
    symbol_to_dpath = {sym: aws_get_candle_dir(type_, sym, time_interval) for sym in symbols}
    prefix_dir = os.path.join(Config.BINANCE_DATA_DIR, 'aws_data')
    symbol_to_lddir = {
        sym: os.path.join(prefix_dir, aws_get_candle_dir(type_, sym, time_interval, local=True)) for sym in symbols
    }
    dpath_to_aws_paths = await aws_batch_list_dir(symbol_to_dpath.values())
    aws_download_symbol_files(symbol_to_dpath, symbol_to_lddir, dpath_to_aws_paths)


async def get_aws_all_coin_perpetual(time_interval):
    logger = get_logger()

    d = aws_get_candle_dir('coin_futures', '', '')[:-2]
    divider(f'Download spot from {d}', logger_=logger)

    paths = await aws_list_dir(d)
    symbols = [Path(os.path.normpath(p)).parts[-1] for p in paths]
    symbols_perp = [s for s in symbols if s.endswith('_PERP')]

    logger.info(f'Download={len(symbols_perp)}')

    await get_aws_candle('coin_futures', time_interval, symbols_perp)


async def get_aws_all_usdt_perpetual(time_interval):
    logger = get_logger()

    d = aws_get_candle_dir('usdt_futures', '', '')[:-2]
    divider(f'Download spot from {d}', logger_=logger)

    paths = await aws_list_dir(d)
    symbols = [Path(os.path.normpath(p)).parts[-1] for p in paths]
    symbols_perp = [s for s in symbols if s.endswith('USDT')]

    logger.info(f'Download={len(symbols_perp)}')

    await get_aws_candle('usdt_futures', time_interval, symbols_perp)


async def get_aws_all_usdt_spot(time_interval):
    logger = get_logger()

    d = aws_get_candle_dir('spot', '', '')[:-2]

    divider(f'Download spot from {d}', logger_=logger)
    paths = await aws_list_dir(d)

    symbols = [Path(os.path.normpath(p)).parts[-1] for p in paths]
    symbols = [s for s in symbols if s.endswith('USDT')]
    n_total = len(symbols)

    lev_symbols = [x for x in symbols if x.endswith(('UPUSDT', 'DOWNUSDT', 'BEARUSDT', 'BULLUSDT')) and x != 'JUPUSDT']

    stables = ('BKRWUSDT', 'USDCUSDT', 'USDPUSDT', 'TUSDUSDT', 'BUSDUSDT', 'FDUSDUSDT', 'DAIUSDT', 'EURUSDT', 'GBPUSDT',
               'USBPUSDT', 'SUSDUSDT', 'PAXGUSDT', 'AEURUSDT')

    symbols = sorted(set(symbols) - set(lev_symbols) - set(stables))
    n_download = len(symbols)
    n_skip = n_total - n_download

    logger.debug('Skip leverage tokens, skip stablecoins')
    logger.info(f'Total={n_total}, Downlaod={n_download}, Skip={n_skip}')
    await get_aws_candle('spot', time_interval, symbols)


def _read_aws_futures_candle_csv(p):
    columns = [
        'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ]
    df = pd.read_csv(p, names=columns)
    if df['candle_begin_time'].at[0] == 'open_time':
        df.drop(index=0, inplace=True)
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'].astype(int), unit='ms', utc=True)
    df['close_time'] = pd.to_datetime(df['close_time'].astype(int), unit='ms', utc=True)
    df.drop(columns='ignore', inplace=True)
    columns = [
        'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume'
    ]
    for c in columns:
        df[c] = df[c].astype(float)
    return df


def _verify(data_path):
    if not verify_checksum(data_path):
        return False

    try:
        _read_aws_futures_candle_csv(data_path)
    except:
        logger.error('Cannot read csv %s', data_path)
        return False

    return True


def verify_aws_candle(type_, time_interval):
    local_dirs = glob(
        os.path.join(
            Config.BINANCE_DATA_DIR,
            'aws_data',
            aws_get_candle_dir(type_, '*', time_interval, local=True),
        ))
    symbols = [Path(os.path.normpath(d)).parts[-2] for d in local_dirs]
    for symbol in symbols:
        verify_candle(type_, symbol, time_interval)


def verify_candle(type_, symbol, time_interval):
    logger = get_logger()
    local_dir = os.path.join(Config.BINANCE_DATA_DIR, 'aws_data',
                             aws_get_candle_dir(type_, symbol, time_interval, local=True))
    divider(f'Verify {type_} {symbol} {time_interval}', logger_=logger)
    logger.info(f'Local directory {local_dir}')

    paths = sorted(glob(os.path.join(local_dir, '*.zip')))
    unverified_paths = []

    for p in paths:
        verify_file = p + '.verified'
        if not os.path.exists(verify_file):
            unverified_paths.append(p)

    logger.debug('Will not verify number of candles')

    n_total,n_success = len(unverified_paths), 0
    if not unverified_paths:
        logger.ok('All files verified')
        return

    tasks = [delayed(_verify)(p) for p in unverified_paths]

    results = Parallel(n_jobs=Config.N_JOBS)(tasks)

    for unverified_path, verify_success in zip(unverified_paths, results):
        if verify_success:
            n_success += 1
            with open(unverified_path + '.verified', 'w') as fout:
                fout.write('')
        else:
            logger.warning('%s failed to verify, deleting', unverified_path)
            if os.path.exists(unverified_path):
                os.remove(unverified_path)
            checksum_path = unverified_path + '.CHECKSUM'
            if os.path.exists(checksum_path):
                os.remove(checksum_path)

    logger.ok(f'{n_success}/{n_total} verified')

def convert_aws_candle_csv(type_, time_interval):
    paths = glob(
        os.path.join(
            Config.BINANCE_DATA_DIR,
            'aws_data',
            aws_get_candle_dir(type_, '*', time_interval, local=True),
            '*.zip',
        ))
    paths = sorted(paths)

    sym_paths = defaultdict(list)
    for p in paths:
        sym = p.split(os.sep)[-3]
        sym_paths[sym].append(p)

    logger.info('Symbols %s', list(sym_paths.keys()))

    odir = os.path.join(Config.BINANCE_DATA_DIR, 'candle_parquet', type_, time_interval)
    if os.path.exists(odir):
        logger.warning('%s exists, deleting', odir)
        shutil.rmtree(odir)
    os.makedirs(odir)

    delta = convert_interval_to_timedelta(time_interval)

    def convert_symbol(symbol, paths):
        dfs = [_read_aws_futures_candle_csv(p) for p in paths]
        df = pd.concat(dfs)
        symbol_api_dir = os.path.join(Config.BINANCE_DATA_DIR, 'api_data', type_, time_interval, symbol)
        if os.path.exists(symbol_api_dir):
            symbol_api_paths = glob(os.path.join(symbol_api_dir, '*.pqt'))
            dfs = [pd.read_parquet(p) for p in symbol_api_paths]
            dfs.append(df)
            df = pd.concat(dfs)
        df.sort_values('candle_begin_time', inplace=True, ignore_index=True)
        df.drop_duplicates('candle_begin_time', keep='last', inplace=True, ignore_index=True)
        df['candle_end_time'] = df['candle_begin_time'] + delta
        df.set_index('candle_end_time', inplace=True)
        output_path = os.path.join(odir, f'{symbol}.pqt')
        df.to_parquet(output_path, compression='zstd')

    Parallel(n_jobs=Config.N_JOBS, verbose=1)(delayed(convert_symbol)(s, ps) for s, ps in sym_paths.items())


def _get_aws_candle_missing_dts(dir_path, splits, symbol_api_dir):
    paths = sorted(glob(os.path.join(dir_path, '*.zip')))
    dts = [os.path.splitext(os.path.basename(p))[0] for p in paths]
    dts = {dt.split('-', 2)[-1].replace('-', '') for dt in dts}
    dt_start, dt_end = min(dts), max(dts)

    segs = []
    if splits is not None:
        for st, en, _ in splits:
            if st is None:
                st = dt_start
            if en is None:
                en = dt_end
            st = pd.to_datetime(st).strftime('%Y%m%d')
            en = pd.to_datetime(en).strftime('%Y%m%d')
            segs.append((st, en))
    else:
        segs = [(dt_start, dt_end)]

    missings = set()
    for dt_start, dt_end in segs:
        dt_range = {x.strftime('%Y%m%d') for x in pd.date_range(dt_start, dt_end)}
        missings = missings.union(dt_range - dts)

    if os.path.exists(symbol_api_dir):
        downloaded_dts = {p.replace('.pqt', '') for p in os.listdir(symbol_api_dir)}
        missings = missings - downloaded_dts

    return sorted(missings)


async def download_aws_missing_from_api(type_, time_interval):
    _aws_dir = os.path.join(
        Config.BINANCE_DATA_DIR,
        'aws_data',
        aws_get_candle_dir(type_, '*', time_interval, local=True),
    )
    symbol_aws_dirs = glob(_aws_dir)
    api_dir = os.path.join(Config.BINANCE_DATA_DIR, 'api_data', type_, time_interval)
    if not os.path.exists(api_dir):
        logger.warning('%s not exists, creating', api_dir)
        os.makedirs(api_dir)

    tasks = []
    for symbol_aws_dir in symbol_aws_dirs:
        symbol = Path(symbol_aws_dir).parts[-2]
        splits = None
        binance_candle_splits = read_candle_splits()
        if type_ in binance_candle_splits:
            splits = binance_candle_splits[type_].get(symbol, None)
        symbol_api_dir = os.path.join(api_dir, symbol)
        missings = _get_aws_candle_missing_dts(symbol_aws_dir, splits, symbol_api_dir)
        if missings:
            logger.info('%s missing dts %s', symbol, missings)
        for dt in missings:
            tasks.append((symbol, dt))

    async with create_aiohttp_session(30) as session:
        fetcher = BinanceFetcher(type_, session)
        for task_batch in batched(tasks, 10):
            timestamp, weight = await fetcher.market_api.aioreq_time_and_weight()
            server_ts = pd.to_datetime(timestamp, unit='ms', utc=True).tz_convert(DEFAULT_TZ)
            logger.info('Server time %s, weight used %d, from %s to %s', server_ts, weight, task_batch[0],
                        task_batch[-1])
            max_minute_weight, _ = fetcher.get_api_limits()
            if weight > max_minute_weight * 0.9:
                await asyncio.sleep(60)
            download_tasks = []
            for symbol, dt in task_batch:
                start_ts = pd.to_datetime(dt)
                end_ts = start_ts + pd.Timedelta(hours=23, minutes=59, seconds=59)

                download_tasks.append(
                    fetcher.get_candle(symbol,
                                       time_interval,
                                       startTime=start_ts.value // 1000000,
                                       endTime=end_ts.value // 1000000))
            results = await asyncio.gather(*download_tasks)
            for (symbol, dt), df_market in zip(task_batch, results):
                symbol_dir = os.path.join(api_dir, symbol)
                if not os.path.exists(symbol_dir):
                    os.makedirs(symbol_dir)
                output_dir = os.path.join(symbol_dir, f'{dt}.pqt')
                df_market.to_parquet(output_dir)
