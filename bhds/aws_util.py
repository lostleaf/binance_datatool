import asyncio
import os
import subprocess

import aiohttp
import xmltodict

from util import async_retry_getter, create_aiohttp_session
from util.log_kit import get_logger, divider

AWS_TYPE_MAP = {
    'spot': ['data', 'spot'],
    'usdt_futures': ['data', 'futures', 'um'],
    'coin_futures': ['data', 'futures', 'cm'],
}

AWS_TIMEOUT_SEC = 30

PREFIX = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
PATH_API_URL = f'{PREFIX}?delimiter=/&prefix='
DOWNLOAD_URL = f'{PREFIX}/'


def parse_aws_dt_from_filepath(p):
    filename = os.path.basename(p)
    dt = filename.split('.')[0].split('-', 2)[-1].replace('-', '')
    return dt


def aws_filter_recent_dates(paths, recent):
    zip_paths = [p for p in paths if p.endswith('.zip')]
    zip_paths = zip_paths[-recent:]
    checksum_paths = [p for p in paths if p.endswith('.CHECKSUM') and p.replace('.CHECKSUM', '') in zip_paths]
    return sorted(zip_paths + checksum_paths)


def _get_dir(path_tokens, local):
    if local:
        return os.path.join(*path_tokens) + os.sep

    # As AWS web path
    return '/'.join(path_tokens) + '/'


def aws_get_candle_dir(type_, symbol, time_interval, local=False):
    return _get_dir(AWS_TYPE_MAP[type_] + ['daily', 'klines', symbol, time_interval], local)


def aws_get_aggtrades_dir(type_, symbol, local=False):
    return _get_dir(AWS_TYPE_MAP[type_] + ['daily', 'aggTrades', symbol], local)


async def _aio_get(session: aiohttp.ClientSession, url):
    async with session.get(url) as resp:
        data = await resp.text()
    return xmltodict.parse(data)


async def _list_dir(session, path):
    url = PATH_API_URL + path
    base_url = url
    results = []
    while True:
        data = await async_retry_getter(_aio_get, session=session, url=url)
        xml_data = data['ListBucketResult']
        if 'CommonPrefixes' in xml_data:
            results.extend([x['Prefix'] for x in xml_data['CommonPrefixes']])
        elif 'Contents' in xml_data:
            results.extend([x['Key'] for x in xml_data['Contents']])
        if xml_data['IsTruncated'] == 'false':
            break
        url = base_url + '&marker=' + xml_data['NextMarker']
    return results


async def aws_list_dir(path):
    async with create_aiohttp_session(AWS_TIMEOUT_SEC) as session:
        return await _list_dir(session, path)


async def aws_batch_list_dir(paths):
    async with create_aiohttp_session(AWS_TIMEOUT_SEC) as session:
        tasks = [_list_dir(session, p) for p in paths]
        results = await asyncio.gather(*tasks)
    return {p: r for p, r in zip(paths, results)}


def aws_download_into_folder(paths, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    paths = [DOWNLOAD_URL + p for p in paths]
    cmd = ['aria2c', '-c', '-d', output_dir, '-Z'] + paths

    subprocess.run(cmd)


def aws_download_symbol_files(symbol_to_dpath, symbol_to_lddir, dpath_to_aws_paths):
    logger = get_logger()
    for symbol, dir_path in symbol_to_dpath.items():
        
        local_dir = symbol_to_lddir[symbol]

        if not os.path.exists(local_dir):
            logger.warning('Local directory not exists, creating')
            os.makedirs(local_dir)

        aws_paths = dpath_to_aws_paths[dir_path]
        local_filenames = set(os.listdir(local_dir))
        missing_file_paths = []

        for aws_path in aws_paths:
            filename = os.path.basename(aws_path)
            if filename not in local_filenames:
                missing_file_paths.append(aws_path)

        divider(f'Download {symbol} files from {dir_path}', sep='-')
        logger.info(f'Local directory {local_dir}')

        if missing_file_paths:
            aws_download_into_folder(missing_file_paths, local_dir)

        logger.ok(f'{len(missing_file_paths)} files downloaded')
