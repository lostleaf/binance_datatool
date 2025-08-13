import asyncio
from pathlib import PurePosixPath
from typing import Optional

import xmltodict
from aiohttp import ClientSession

from bdt_common.constants import BINANCE_AWS_PREFIX
from bdt_common.network import async_retry_getter
from bhds.aws.path_builder import AwsPathBuilder


class AwsClient:
    """
    AWS client for accessing Binance historical data from AWS S3 buckets.
    
    This client provides methods to list and navigate the directory structure
    """
    
    def __init__(
        self,
        path_builder: AwsPathBuilder,
        session: ClientSession,
        http_proxy: Optional[str] = None,
    ):
        """
        Initialize AWS client for Binance data access.
        
        Args:
            path_builder: AWS path builder for constructing directory paths
            session: aiohttp ClientSession for HTTP requests
            http_proxy: Optional HTTP proxy URL for requests
        """
        self.session = session
        self.http_proxy = http_proxy
        self.path_builder = path_builder

    async def _aio_get_xml(self, url):
        """
        Fetch XML data from AWS S3 URL and parse it into a Python dictionary.
        
        Args:
            url: The AWS S3 URL to fetch XML data from
            
        Returns:
            dict: Parsed XML data as a nested Python dictionary
        """
        # Perform async HTTP GET request with optional proxy support
        async with self.session.get(url, proxy=self.http_proxy) as resp:
            # Read response text content (XML format)
            data = await resp.text()

        # Parse XML string into Python dictionary structure
        return xmltodict.parse(data)

    def get_symbol_dir(self, symbol) -> PurePosixPath:
        """
        Get the directory path for a specific symbol.
        
        Args:
            symbol: Trading symbol (e.g., BTCUSDT)
            
        Returns:
            PurePosixPath object representing the symbol directory path
        """
        return self.path_builder.get_symbol_dir(symbol)

    async def list_dir(self, dir_path: PurePosixPath) -> list[PurePosixPath]:
        """
        List contents of a directory in AWS S3.
        
        Handles pagination for large directories by making multiple requests until all items are retrieved.

        Args:
            dir_path: Directory path to list contents from
            
        Returns:
            List of PurePosixPath objects representing files and subdirectories
        """
        # Convert directory path to AWS S3 prefix format (ends with '/')
        aws_dir_str = str(dir_path) + "/"
        base_url = url = f"{BINANCE_AWS_PREFIX}?delimiter=/&prefix={aws_dir_str}"
        results = []
        
        # Handle pagination - AWS S3 returns max 1000 items per request
        while True:
            # Fetch XML response with retry logic
            data = await async_retry_getter(self._aio_get_xml, url=url)
            xml_data = data["ListBucketResult"]
            
            # Process subdirectories (CommonPrefixes) and files (Contents)
            if "CommonPrefixes" in xml_data:
                # CommonPrefixes contains directory-like prefixes
                results.extend([PurePosixPath(x["Prefix"]) for x in xml_data["CommonPrefixes"]])
            elif "Contents" in xml_data:
                # Contents contains actual file objects
                results.extend([PurePosixPath(x["Key"]) for x in xml_data["Contents"]])
            
            # Check if more results are available
            if xml_data["IsTruncated"] == "false":
                break
                
            # Continue pagination with NextMarker
            url = base_url + "&marker=" + xml_data["NextMarker"]
            
        return sorted(results)

    async def list_symbols(self) -> list[str]:
        """
        List all available symbols for the configured data type.
        
        Returns:
            Sorted list of symbol names as strings
        """
        paths = await self.list_dir(self.path_builder.base_dir)
        symbols = sorted(p.name for p in paths)
        return symbols

    async def list_data_files(self, symbol: str) -> list[PurePosixPath]:
        """
        List all data files for a specific symbol.
        
        Args:
            symbol: Trading symbol to list files for
            
        Returns:
            List of PurePosixPath objects representing data file paths
        """
        symbol_dir = self.get_symbol_dir(symbol)
        return await self.list_dir(symbol_dir)

    async def batch_list_data_files(self, symbols: list[str]) -> dict[str, list[PurePosixPath]]:
        """
        List data files for multiple symbols concurrently.
        
        Uses asyncio.gather to fetch file lists for all symbols in parallel,
        improving performance for batch operations.
        
        Args:
            symbols: List of trading symbols to list files for
            
        Returns:
            Dictionary mapping symbol names to their respective file lists
        """
        tasks = [self.list_data_files(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return {symbol: list_result for symbol, list_result in zip(symbols, results)}
