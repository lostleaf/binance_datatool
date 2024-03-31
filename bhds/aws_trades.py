import logging
import os

from config import Config

from .aws_util import aws_get_aggtrades_dir, aws_batch_list_dir, aws_filter_recent_dates, aws_download_symbol_files


async def get_aws_aggtrades(type_, recent, symbols):
    logging.info('Get AWS aggtrades for %d symbols, %d recent days', len(symbols), recent)
    symbol_to_dpath = {sym: aws_get_aggtrades_dir(type_, sym) for sym in symbols}
    dpath_to_aws_paths = await aws_batch_list_dir(symbol_to_dpath.values())
    dpath_to_aws_paths = {dp: aws_filter_recent_dates(ps, recent) for dp, ps in dpath_to_aws_paths.items()}

    prefix_dir = os.path.join(Config.BINANCE_DATA_DIR, 'aws_data')
    symbol_to_lddir = {
        sym: os.path.join(prefix_dir, aws_get_aggtrades_dir(type_, sym, local=True))
        for sym in symbols
    }
    aws_download_symbol_files(symbol_to_dpath, symbol_to_lddir, dpath_to_aws_paths)