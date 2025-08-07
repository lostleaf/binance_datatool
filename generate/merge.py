from typing import Optional
from config import BINANCE_DATA_DIR, TradeType


import polars as pl


def merge_klines(trade_type: TradeType, symbol: str, time_interval: str, exclude_empty: bool) -> Optional[pl.DataFrame]:
    """
    Merge K-line data from AWS parsed data and API downloaded data

    Args:
        trade_type: Trading type (e.g. SPOT, UM, CM)
        symbol: Trading pair symbol (e.g. BTCUSDT)
        time_interval: K-line interval (e.g. 1m, 1h)
        exclude_empty: Whether to exclude K-lines with zero volume

    Returns:
        Merged DataFrame containing data from both sources, or None if no AWS data found
    """
    # Get AWS parsed data directory
    parsed_symbol_kline_dir = BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "klines" / symbol / time_interval
    
    # Get AWS parsed data files
    aws_files = list(parsed_symbol_kline_dir.glob("*.parquet"))
    if not aws_files:
        return None

    # Scan AWS data using LazyFrame
    aws_lf = pl.scan_parquet(aws_files)
    
    # Apply exclude_empty filter if needed
    if exclude_empty:
        aws_lf = aws_lf.filter(pl.col("volume") > 0)

    # Get API data directory
    api_kline_dir = BINANCE_DATA_DIR / "api_data" / trade_type.value / "klines" / symbol / time_interval
    # Get all API data files
    api_files = list(api_kline_dir.glob("*.parquet"))

    if not api_files:
        return aws_lf.collect()

    # Scan API data using LazyFrame
    api_lf = pl.scan_parquet(api_files)

    # Apply exclude_empty filter if needed
    if exclude_empty:
        api_lf = api_lf.filter(pl.col("volume") > 0)

    # Merge the LazyFrames, keeping all rows from both sources
    merged_lf = pl.concat([aws_lf, api_lf])

    # Remove duplicates and sort by timestamp, then collect
    merged_df = merged_lf.unique(subset=["candle_begin_time"], keep="last").sort("candle_begin_time").collect()

    return merged_df


def merge_funding_rates(trade_type: TradeType, symbol: str) -> Optional[pl.DataFrame]:
    """
    Merge funding rates from AWS parsed data and API downloaded data

    Args:
        trade_type: Trading type (e.g. UM, CM)
        symbol: Trading pair symbol (e.g. BTCUSDT)

    Returns:
        Merged DataFrame containing data from both sources, or None if no AWS data found
    """
    # Get AWS parsed data file
    parsed_funding_file = BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "funding" / f"{symbol}.parquet"

    # Check if AWS parsed data exists
    if not parsed_funding_file.exists():
        return None

    # Scan AWS data using LazyFrame
    aws_lf = pl.scan_parquet(parsed_funding_file).select(["candle_begin_time", "funding_rate"])

    # Get API data file
    api_funding_file = BINANCE_DATA_DIR / "api_data" / trade_type.value / "funding_rate" / f"{symbol}.parquet"

    # If API data doesn't exist, collect AWS data and return
    if not api_funding_file.exists():
        aws_df = aws_lf.collect()
        if aws_df.is_empty():
            return None
        return aws_df

    # Scan API data using LazyFrame
    api_lf = pl.scan_parquet(api_funding_file).select(["candle_begin_time", "funding_rate"])

    # Merge the LazyFrames, keeping all rows from both sources
    merged_lf = pl.concat([aws_lf, api_lf])

    # Remove duplicates and sort by timestamp, then collect
    merged_df = merged_lf.unique(subset=["candle_begin_time"], keep="last").sort("candle_begin_time").collect()

    # Check if result is empty
    if merged_df.is_empty():
        return None

    return merged_df
