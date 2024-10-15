import os
from glob import glob

from bhds.checksum import run_verify_checksum
from config import Config

from .aws_util import (aws_batch_list_dir, aws_download_symbol_files, aws_get_funding_dir, aws_download)
from util.log_kit import get_logger, divider


async def get_aws_funding(type_, symbols):
    logger = get_logger()
    logger.info('Get AWS funding rates for %d symbols', len(symbols))

    symbol_to_dpath = {sym: aws_get_funding_dir(type_, sym) for sym in symbols}
    dpath_to_aws_fpaths = await aws_batch_list_dir(symbol_to_dpath.values())

    prefix_dir = os.path.join(Config.BINANCE_DATA_DIR, 'aws_data')
    for symbol, dpath in symbol_to_dpath.items():
        divider(f'Funding rate {type_} {symbol}', logger_=logger)
        logger.info(f'DPath={dpath}')
        aws_fpaths = dpath_to_aws_fpaths[dpath]
        local_dpath = os.path.join(prefix_dir, aws_get_funding_dir(type_, symbol, True))
        logger.info(f'Local={local_dpath}')
        aws_download(local_dpath, aws_fpaths)


def verify_aws_funding(type_):
    logger = get_logger()

    prefix_dir = os.path.join(Config.BINANCE_DATA_DIR, 'aws_data')
    local_dir = os.path.join(prefix_dir, aws_get_funding_dir(type_, '*', local=True))
    logger.info('Local directory %s', local_dir)

    paths = sorted(glob(os.path.join(local_dir, '*.zip')))
    run_verify_checksum(paths)
