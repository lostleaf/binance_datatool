import asyncio
import subprocess
import tempfile
from pathlib import Path

import xmltodict
from aiohttp import ClientSession

from config import Config
from util.log_kit import logger
from util.network import async_retry_getter
from constant import TradeType
AWS_TIMEOUT_SEC = 15

PREFIX = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
DOWNLOAD_URL = f'{PREFIX}/'

TYPE_BASE_DIR = {
    TradeType.spot: ['data', 'spot'],
    TradeType.um_futures: ['data', 'futures', 'um'],
    TradeType.cm_futures: ['data', 'futures', 'cm'],
}


def get_aws_dir(path_tokens):
    # As AWS web path
    return '/'.join(path_tokens) + '/'


class AwsClient:

    def __init__(self, session: ClientSession, http_proxy=None):
        self.session = session
        self.http_proxy = http_proxy

    async def _aio_get_xml(self, url):
        async with self.session.get(url, proxy=self.http_proxy) as resp:
            data = await resp.text()

        return xmltodict.parse(data)

    async def list_dir(self, dir_path):
        url = f'{PREFIX}?delimiter=/&prefix={dir_path}'
        base_url = url
        results = []
        while True:
            data = await async_retry_getter(self._aio_get_xml, url=url)
            xml_data = data['ListBucketResult']
            if 'CommonPrefixes' in xml_data:
                results.extend([x['Prefix'] for x in xml_data['CommonPrefixes']])
            elif 'Contents' in xml_data:
                results.extend([x['Key'] for x in xml_data['Contents']])
            if xml_data['IsTruncated'] == 'false':
                break
            url = base_url + '&marker=' + xml_data['NextMarker']
        return results

    async def batch_list_dir(self, dir_paths):
        tasks = [self.list_dir(p) for p in dir_paths]
        results = await asyncio.gather(*tasks)
        return {p: r for p, r in zip(dir_paths, results)}


def aws_download(aws_files, http_proxy):
    local_aws_dir = Path(Config.BINANCE_DATA_DIR) / 'aws_data'
    missing_file_paths = []

    for aws_file_path in aws_files:
        local_path = local_aws_dir / Path(aws_file_path)
        if not local_path.exists():
            missing_file_paths.append(aws_file_path)

    if not missing_file_paths:
        logger.ok('All files downloaded, nothing missing')
        return

    logger.debug(f'{len(missing_file_paths)} files to be downloaded')

    with tempfile.NamedTemporaryFile(mode='w', delete_on_close=False, prefix='bhds_') as aria_file:
        for aws_file_path in missing_file_paths:
            local_path = local_aws_dir / Path(aws_file_path)
            url = DOWNLOAD_URL + aws_file_path
            aria_file.write(f'{url}\n  dir={local_path.parent}\n')

        aria_file.close()

        cmd = ['aria2c', '-i', aria_file.name, '-q']
        if http_proxy is not None:
            cmd.append(f'--all-proxy={http_proxy}')

        subprocess.run(cmd, check=True)

    logger.ok('Download finished')
