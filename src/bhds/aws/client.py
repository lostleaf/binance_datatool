import asyncio
from pathlib import PurePosixPath
from typing import Optional

import xmltodict
from aiohttp import ClientSession

from bdt_common.constants import BINANCE_AWS_PREFIX
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.network import async_retry_getter


class AwsClient:
    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        session: ClientSession,
        http_proxy: Optional[str] = None,
    ):
        self.session = session
        self.http_proxy = http_proxy
        self.base_dir = PurePosixPath("data") / trade_type / data_freq / data_type

    async def _aio_get_xml(self, url):
        async with self.session.get(url, proxy=self.http_proxy) as resp:
            data = await resp.text()

        return xmltodict.parse(data)

    def get_symbol_dir(self, symbol) -> PurePosixPath:
        return self.base_dir / symbol

    async def list_dir(self, dir_path: PurePosixPath) -> list[PurePosixPath]:
        aws_dir_str = str(dir_path) + "/"
        base_url = url = f"{BINANCE_AWS_PREFIX}?delimiter=/&prefix={aws_dir_str}"
        results = []
        while True:
            data = await async_retry_getter(self._aio_get_xml, url=url)
            xml_data = data["ListBucketResult"]
            if "CommonPrefixes" in xml_data:
                results.extend([PurePosixPath(x["Prefix"]) for x in xml_data["CommonPrefixes"]])
            elif "Contents" in xml_data:
                results.extend([PurePosixPath(x["Key"]) for x in xml_data["Contents"]])
            if xml_data["IsTruncated"] == "false":
                break
            url = base_url + "&marker=" + xml_data["NextMarker"]
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


class AwsKlineClient(AwsClient):
    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        time_interval: str,
        session: ClientSession,
        http_proxy: Optional[str] = None,
    ):
        super().__init__(trade_type, data_freq, DataType.kline, session, http_proxy)
        self.time_interval = time_interval

    def get_symbol_dir(self, symbol) -> PurePosixPath:
        return self.base_dir / symbol / self.time_interval
