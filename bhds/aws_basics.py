import asyncio
import os
import subprocess
import tempfile
from pathlib import Path

import xmltodict
from aiohttp import ClientSession

from config import Config
from constant import TradeType
from util.log_kit import logger
from util.network import async_retry_getter

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


def aws_download(aws_files, http_proxy, max_tries=3):
    for idx in range(1, max_tries + 1):
        returncode = run_aws_download(aws_files, http_proxy)

        if returncode == 0:
            logger.ok('Aria2 download successfully')
            return

        if max_tries == idx:
            logger.error(f'Aria2 exited with code {returncode}. Please manually run download again')
        else:
            logger.warning(f'Aria2 exited with code {returncode}, left_tries={max_tries - idx - 1}. Download again...')


def run_aws_download(aws_files, http_proxy):
    local_aws_dir = Path(Config.BINANCE_DATA_DIR) / 'aws_data'
    missing_file_paths = []

    for aws_file_path in aws_files:
        local_path = local_aws_dir / Path(aws_file_path)
        if not local_path.exists():
            missing_file_paths.append(aws_file_path)

    if not missing_file_paths:
        logger.ok('All files downloaded, nothing missing')
        return 0

    logger.debug(f'{len(missing_file_paths)} files to be downloaded')

    with tempfile.NamedTemporaryFile(mode='w', delete_on_close=False, prefix='bhds_') as aria_file:
        for aws_file_path in missing_file_paths:
            local_path = local_aws_dir / Path(aws_file_path)
            url = DOWNLOAD_URL + aws_file_path
            aria_file.write(f'{url}\n  dir={local_path.parent}\n')

        aria_file.close()

        cmd = ['aria2c', '-i', aria_file.name, '-j32', '-x4', '-q']
        if http_proxy is not None:
            cmd.append(f'--all-proxy={http_proxy}')

        run_result = subprocess.run(cmd)
        returncode = run_result.returncode
    return returncode


def get_funding_rate_path_tokens(trade_type: TradeType):
    return [*TYPE_BASE_DIR[trade_type], 'monthly', 'fundingRate']


def get_kline_path_tokens(trade_type: TradeType):
    return [*TYPE_BASE_DIR[trade_type], 'daily', 'klines']


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


class AwsMonthlyFundingRateClient(AwsClient):

    def __init__(self, session, http_proxy, trade_type):
        super().__init__(session, http_proxy)
        self.trade_type = trade_type

    @property
    def base_dir_tokens(self):
        return get_funding_rate_path_tokens(self.trade_type)

    async def list_symbols(self):
        aws_dir = get_aws_dir(self.base_dir_tokens)
        paths = await self.list_dir(aws_dir)
        symbols = [Path(os.path.normpath(p)).parts[-1] for p in paths]
        return sorted(symbols)

    async def list_funding_rate_files(self, symbol):
        aws_dir = get_aws_dir(self.base_dir_tokens + [symbol])
        return sorted(await self.list_dir(aws_dir))

    async def batch_list_funding_rate_files(self, symbols):
        results = await asyncio.gather(*[self.list_funding_rate_files(symbol) for symbol in symbols])
        return {symbol: list_result for symbol, list_result in zip(symbols, results)}


class AwsDailyKlineClient(AwsClient):

    def __init__(self, session, http_proxy, trade_type):
        super().__init__(session, http_proxy)
        self.trade_type = trade_type

    @property
    def base_dir_tokens(self):
        return get_kline_path_tokens(self.trade_type)

    async def list_symbols(self):
        aws_dir = get_aws_dir(self.base_dir_tokens)
        paths = await self.list_dir(aws_dir)
        symbols = [Path(os.path.normpath(p)).parts[-1] for p in paths]
        return sorted(symbols)

    async def list_kline_files(self, time_interval, symbol):
        aws_dir = get_aws_dir(self.base_dir_tokens + [symbol, time_interval])
        return await sorted(self.list_dir(aws_dir))

    async def batch_list_kline_files(self, time_interval, symbols):
        tasks = []
        for symbol in symbols:
            aws_dir = get_aws_dir(self.base_dir_tokens + [symbol, time_interval])
            tasks.append(self.list_dir(aws_dir))
        results = await asyncio.gather(*tasks)
        return {symbol: list_result for symbol, list_result in zip(symbols, results)}
