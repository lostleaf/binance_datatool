import os
from glob import glob

from config import Config

from .aws_util import (aws_batch_list_dir, aws_download_symbol_files, aws_filter_recent_dates, aws_get_aggtrades_dir)
from .checksum import verify_checksum
from joblib import Parallel, delayed
from util import get_logger

logger = get_logger()

async def get_aws_aggtrades(type_, recent, symbols):
    logger.info('Get AWS aggtrades for %d symbols, %d recent days', len(symbols), recent)
    symbol_to_dpath = {sym: aws_get_aggtrades_dir(type_, sym) for sym in symbols}
    dpath_to_aws_paths = await aws_batch_list_dir(symbol_to_dpath.values())
    dpath_to_aws_paths = {dp: aws_filter_recent_dates(ps, recent) for dp, ps in dpath_to_aws_paths.items()}

    prefix_dir = os.path.join(Config.BINANCE_DATA_DIR, 'aws_data')
    symbol_to_lddir = {sym: os.path.join(prefix_dir, aws_get_aggtrades_dir(type_, sym, local=True)) for sym in symbols}
    aws_download_symbol_files(symbol_to_dpath, symbol_to_lddir, dpath_to_aws_paths)


def verify_aws_aggtrades(type_):
    prefix_dir = os.path.join(Config.BINANCE_DATA_DIR, 'aws_data')
    local_dir = os.path.join(prefix_dir, aws_get_aggtrades_dir(type_, '*', local=True))
    logger.info('Local directory %s', local_dir)

    paths = sorted(glob(os.path.join(local_dir, '*.zip')))
    unverified_paths = []

    for p in paths:
        verify_file = p + '.verified'
        if not os.path.exists(verify_file):
            unverified_paths.append(p)

    logger.info('%d files to be verified', len(unverified_paths))

    results = Parallel(Config.N_JOBS)(delayed(verify_checksum)(p) for p in paths)

    for unverified_path, verify_success in zip(unverified_paths, results):
        if verify_success:
            with open(unverified_path + '.verified', 'w') as fout:
                fout.write('')
        else:
            logger.warning('%s failed to verify, deleting', unverified_path)
            if os.path.exists(unverified_path):
                os.remove(unverified_path)
            checksum_path = unverified_path + '.CHECKSUM'
            if os.path.exists(checksum_path):
                os.remove(checksum_path)
