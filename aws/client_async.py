import asyncio
import subprocess
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path, PurePosixPath

import xmltodict
from aiohttp import ClientSession

import config
from config import DataFreq, TradeType
from util.log_kit import logger
from util.network import async_retry_getter


def run_aws_download(download_infos: list[tuple[str, Path]], http_proxy):
    download_infos_missing: list[tuple[str, Path]] = []
    for aws_url, local_file in download_infos:
        if not local_file.exists():
            download_infos_missing.append((aws_url, local_file))

    if not download_infos_missing:
        logger.ok('All files downloaded, nothing missing')
        return 0

    logger.debug(f'{len(download_infos_missing)} files to be downloaded')

    with tempfile.NamedTemporaryFile(mode='w', delete_on_close=False, prefix='bhds_') as aria_file:
        for aws_url, local_file in download_infos_missing:
            aria_file.write(f'{aws_url}\n  dir={local_file.parent}\n')
        aria_file.close()

        cmd = ['aria2c', '-i', aria_file.name, '-j32', '-x4', '-q']
        if http_proxy is not None:
            cmd.append(f'--all-proxy={http_proxy}')

        run_result = subprocess.run(cmd)
        returncode = run_result.returncode
    return returncode


class AwsClient(ABC):
    PREFIX = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
    LOCAL_DIR = config.BINANCE_DATA_DIR / 'aws_data'
    TYPE_BASE_DIR = {
        TradeType.spot: PurePosixPath('data') / 'spot',
        TradeType.um_futures: PurePosixPath('data') / 'futures' / 'um',
        TradeType.cm_futures: PurePosixPath('data') / 'futures' / 'cm',
    }

    def __init__(self, session: ClientSession, base_dir: PurePosixPath, http_proxy):
        self.session = session
        self.http_proxy = http_proxy
        self.base_dir = base_dir

    async def _aio_get_xml(self, url):
        async with self.session.get(url, proxy=self.http_proxy) as resp:
            data = await resp.text()

        return xmltodict.parse(data)

    @classmethod
    @abstractmethod
    def get_base_dir(self, trade_type: str, data_freq: DataFreq) -> PurePosixPath:
        pass

    @abstractmethod
    def get_symbol_dir(self, symbol) -> PurePosixPath:
        pass

    @classmethod
    def _get_aws_dir(cls, dir_path: PurePosixPath) -> str:
        # As AWS web path
        return str(dir_path) + '/'

    async def list_dir(self, dir_path: PurePosixPath) -> list[PurePosixPath]:
        aws_dir_str = self._get_aws_dir(dir_path)
        base_url = url = f'{self.PREFIX}?delimiter=/&prefix={aws_dir_str}'
        results = []
        while True:
            data = await async_retry_getter(self._aio_get_xml, url=url)
            xml_data = data['ListBucketResult']
            if 'CommonPrefixes' in xml_data:
                results.extend([PurePosixPath(x['Prefix']) for x in xml_data['CommonPrefixes']])
            elif 'Contents' in xml_data:
                results.extend([PurePosixPath(x['Key']) for x in xml_data['Contents']])
            if xml_data['IsTruncated'] == 'false':
                break
            url = base_url + '&marker=' + xml_data['NextMarker']
        return sorted(results)

    async def list_symbols(self) -> list[str]:
        paths = await self.list_dir(self.base_dir)
        symbols = sorted(p.name for p in paths)
        return symbols

    async def list_data_files(self, symbol: str) -> list[PurePosixPath]:
        symbol_dir = self.get_symbol_dir(symbol)
        return await self.list_dir(symbol_dir)

    async def batch_list_data_files(self, symbols: list[str]) -> dict[str, list[PurePosixPath]]:
        tasks = [self.list_data_files(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return {symbol: list_result for symbol, list_result in zip(symbols, results)}

    def aws_download(self, aws_files: list[PurePosixPath], max_tries=3):
        download_infos = []
        for aws_file in aws_files:
            local_file = self.LOCAL_DIR / aws_file
            aws_url = f'{self.PREFIX}/{str(aws_file)}'
            download_infos.append([aws_url, local_file])

        for idx in range(max_tries):
            returncode = run_aws_download(download_infos, self.http_proxy)

            if returncode == 0:
                logger.ok('Aria2 download successfully')
                return

            left_tries = max_tries - idx - 1
            if left_tries == 0:
                logger.error(f'Aria2 exited with code {returncode}. Please manually run download again')
            else:
                logger.warning(f'Aria2 exited with code {returncode}, left_tries={left_tries}. Download again...')


class AwsFundingRateClient(AwsClient):

    def __init__(self, session, trade_type: TradeType, data_freq: DataFreq = DataFreq.monthly, http_proxy: str = None):
        self.trade_type = trade_type

        base_dir = self.get_base_dir(trade_type, data_freq)
        super().__init__(session, base_dir, http_proxy)

    @classmethod
    def get_base_dir(cls, trade_type: TradeType, data_freq: DataFreq):
        return cls.TYPE_BASE_DIR[trade_type] / data_freq.value / 'fundingRate'

    def get_symbol_dir(self, symbol) -> PurePosixPath:
        return self.base_dir / symbol


class AwsKlineClient(AwsClient):

    def __init__(self,
                 session,
                 trade_type: TradeType,
                 time_interval: str,
                 data_freq: DataFreq = DataFreq.daily,
                 http_proxy: str = None):
        self.trade_type = trade_type
        self.time_interval = time_interval

        base_dir = self.get_base_dir(trade_type, data_freq)
        super().__init__(session, base_dir, http_proxy)

    @classmethod
    def get_base_dir(cls, trade_type, data_freq: DataFreq) -> PurePosixPath:
        return cls.TYPE_BASE_DIR[trade_type] / data_freq.value / 'klines'

    def get_symbol_dir(self, symbol) -> PurePosixPath:
        return self.base_dir / symbol / self.time_interval
