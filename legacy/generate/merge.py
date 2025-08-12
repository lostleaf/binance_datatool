from typing import Optional
from config import BINANCE_DATA_DIR, TradeType


import polars as pl


def merge_klines(trade_type: TradeType, symbol: str, time_interval: str, exclude_empty: bool) -> Optional[pl.LazyFrame]:
    """
    Merge K-line data from AWS parsed data and API downloaded data

    Args:
        trade_type: Trading type (e.g. SPOT, UM, CM)
        symbol: Trading pair symbol (e.g. BTCUSDT)
        time_interval: K-line interval (e.g. 1m, 1h)
        exclude_empty: Whether to exclude K-lines with zero volume

    Returns:
        Merged LazyFrame containing data from both sources, or None if no AWS data found
    """
    # Define parquet data directories
    parsed_kline_dir = BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "klines" / symbol / time_interval
    api_kline_dir = BINANCE_DATA_DIR / "api_data" / trade_type.value / "klines" / symbol / time_interval

    # Glob parquet files
    api_files = list(api_kline_dir.glob("*.parquet"))
    aws_files = list(parsed_kline_dir.glob("*.parquet"))
   
    if not aws_files:
        return None

    # Scan AWS data using LazyFrame
    aws_lf = pl.scan_parquet(aws_files)

    if  api_files:
        # Scan API data using LazyFrame
        api_lf = pl.scan_parquet(api_files)
        # Merge the LazyFrames, remove duplicates and sort by timestamp, then collect
        merged_lf = pl.concat([aws_lf, api_lf]).unique(subset=["candle_begin_time"], keep="last")
    else:
        merged_lf = aws_lf

    # Apply exclude_empty filter if needed
    if exclude_empty:
        merged_lf = merged_lf.filter(pl.col("volume") > 0)

    return merged_lf.sort("candle_begin_time")


def merge_funding_rates(trade_type: TradeType, symbol: str) -> Optional[pl.LazyFrame]:
    """
    Merge funding rates from AWS-parsed data and API-downloaded data with a fully LazyFrame-based pipeline.

    Args:
        trade_type: Trading type (e.g. UM, CM)
        symbol: Trading pair symbol (e.g. BTCUSDT)

    Returns:
        A LazyFrame with merged funding rates from available sources, or None if no data exists.
    """
    aws = BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "funding" / f"{symbol}.parquet"
    api = BINANCE_DATA_DIR / "api_data" / trade_type.value / "funding_rate" / f"{symbol}.parquet"

    # Keep only existing files
    paths = [p for p in (aws, api) if p.exists()]
    if not paths:
        return None

    # Build LazyFrame: select columns early (projection pushdown), deduplicate, sort
    lf = (
        pl.scan_parquet(paths)
        .select(["candle_begin_time", "funding_rate"])
        .unique(subset=["candle_begin_time"], keep="last")
        .sort("candle_begin_time")
    )
    return lf
