"""
Unified CSV parser for Binance historical data.

This module provides a unified interface for parsing different types of CSV data from Binance's AWS data center.
It uses an abstract base class design with concrete implementations for klines and funding rate data.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List
from zipfile import ZipFile

import polars as pl

from bdt_common.enums import DataType


class BaseCsvParser(ABC):
    """Abstract base class for CSV file parsers.

    This class defines the common interface for all CSV parsers used in BHDS.
    Subclasses must implement the required properties and methods to handle specific types.
    """

    @property
    @abstractmethod
    def column_definitions(self) -> Dict[str, pl.DataType]:
        """Column name to data type mapping.

        Returns:
            Dict[str, pl.DataType]: Mapping of column names to their Polars data types.
        """
        pass

    @property
    @abstractmethod
    def all_columns(self) -> List[str]:
        """List of all column names in the raw CSV.

        Returns:
            List[str]: Complete list of column names as they appear in the CSV.
        """
        pass

    @property
    @abstractmethod
    def header_check_prefix(self) -> str:
        """Prefix to check for CSV header presence.

        Returns:
            str: String prefix to identify if the first row is a header.
        """
        pass

    @abstractmethod
    def post_process(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Post-process the lazy dataframe.

        Args:
            df: Lazy dataframe to be processed.

        Returns:
            pl.LazyFrame: Processed lazy dataframe.
        """
        pass

    def read_csv_from_zip(self, zip_file: Path) -> pl.DataFrame:
        """Read and parse CSV data from a zip file.

        Extracts CSV content from a zip archive, handles headers, applies column
        definitions, and performs post-processing.

        Args:
            zip_file: Path to the zip file containing the CSV data.

        Returns:
            pl.DataFrame: Processed dataframe with proper schema and timezone.

        Raises:
            FileNotFoundError: If the zip file does not exist.
            ValueError: If the zip file is corrupted or contains no CSV data.
        """
        if not zip_file.exists():
            raise FileNotFoundError(f"Zip file not found: {zip_file}")

        with ZipFile(zip_file) as f:
            # Get the first file in the zip archive
            csv_filename = f.namelist()[0]
            lines = f.open(csv_filename).readlines()

            # Skip header row if present
            if lines and lines[0].decode().startswith(self.header_check_prefix):
                lines = lines[1:]

        # Create lazy dataframe with proper schema
        ldf = pl.scan_csv(
            lines, has_header=False, new_columns=self.all_columns, schema_overrides=self.column_definitions
        )

        # Apply post-processing and collect
        return self.post_process(ldf).collect()


class KlineParser(BaseCsvParser):
    """Parser for kline (OHLCV) data from Binance.

    Handles CSV files containing candlestick data with open, high, low, close,
    volume, and additional trading metrics.
    """

    @property
    def column_definitions(self) -> Dict[str, pl.DataType]:
        """Column definitions for kline data."""
        return {
            "candle_begin_time": pl.Int64,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "quote_volume": pl.Float64,
            "trade_num": pl.Int64,
            "taker_buy_base_asset_volume": pl.Float64,
            "taker_buy_quote_asset_volume": pl.Float64,
        }

    @property
    def all_columns(self) -> List[str]:
        """All columns in kline CSV files."""
        return [
            "candle_begin_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trade_num",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ]

    @property
    def header_check_prefix(self) -> str:
        """Header prefix for kline CSV files."""
        return "open_time"

    def post_process(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Post-process kline data with timezone conversion."""
        return df.select(self.column_definitions.keys()).with_columns(
            pl.when(pl.col("candle_begin_time").max() >= 10**15)
            .then(pl.col("candle_begin_time").cast(pl.Datetime("us")))
            .otherwise(pl.col("candle_begin_time").cast(pl.Datetime("ms")))
            .dt.replace_time_zone("UTC")
            .dt.cast_time_unit("ms")
            .alias("candle_begin_time")
        )


class FundingParser(BaseCsvParser):
    """Parser for funding rate data from Binance.

    Handles CSV files containing funding rate information for perpetual futures.
    """

    @property
    def column_definitions(self) -> Dict[str, pl.DataType]:
        """Column definitions for funding rate data."""
        return {
            "funding_time": pl.Int64,
            "funding_interval_hours": pl.Int64,
            "funding_rate": pl.Float64,
        }

    @property
    def all_columns(self) -> List[str]:
        """All columns in funding rate CSV files."""
        return ["funding_time", "funding_interval_hours", "funding_rate"]

    @property
    def header_check_prefix(self) -> str:
        """Header prefix for funding rate CSV files."""
        return "calc_time"

    def post_process(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Post-process funding rate data with candle alignment."""
        return df.with_columns(
            (pl.col("funding_time") - pl.col("funding_time") % (60 * 60 * 1000)).alias("candle_begin_time")
        ).with_columns(
            pl.col("candle_begin_time").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC"),
            pl.col("funding_time").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC"),
        )


def create_aws_parser(data_type: DataType) -> BaseCsvParser:
    """Create a parser instance for the specified AWS data type.

    Simple factory function to instantiate parsers for different Binance AWS data types.

    Args:
        data_type: Type of data to parse (e.g., DataType.kline, DataType.funding_rate).

    Returns:
        BaseCsvParser: Configured parser instance.

    Raises:
        ValueError: If the data type is not supported.

    Examples:
        >>> parser = create_aws_parser(DataType.kline)
        >>> df = parser.read_csv_from_zip("BTCUSDT-1m-2023-01-01.zip")

        >>> parser = create_aws_parser(DataType.funding_rate)
        >>> df = parser.read_csv_from_zip("BTCUSDT-funding-2023-01.zip")
    """
    parsers = {DataType.kline: KlineParser, DataType.funding_rate: FundingParser}

    if data_type not in parsers:
        available = list(parsers.keys())
        raise ValueError(f"Unsupported data type: {data_type}. Available types: {available}")
    return parsers[data_type]()
