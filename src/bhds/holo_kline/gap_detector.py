import polars as pl
from pathlib import Path
from datetime import timedelta


class HoloKlineGapDetector:
    """Gap detector for holographic kline data"""
    
    def __init__(self, min_days: int, min_price_chg: float):
        self.min_days = min_days
        self.min_price_chg = min_price_chg
    
    def detect(self, kline_file: Path) -> pl.LazyFrame:
        """
        Detect gaps in holographic kline data using a two-tier detection strategy.
        
        It uses a dual-detection approach to capture different types of gaps:
        
        1. Primary Detection: Identifies gaps that meet both minimum time duration 
           (min_days) and minimum price change (min_price_chg) thresholds.
           
        2. Secondary Detection: Identifies gaps with extended time duration (2x min_days)
           but no price change requirement.
        
        Args:
            kline_file (Path): Path to the parquet file containing kline data.
                              Expected columns: candle_begin_time, open, close, volume
                              
        Returns:
            pl.LazyFrame: A lazy frame containing detected gaps with columns:
                         - prev_begin_time: Timestamp of the candle before the gap
                         - candle_begin_time: Timestamp of the candle after the gap
                         - prev_close: Close price before the gap
                         - open: Open price after the gap
                         - time_diff: Duration of the gap
                         - price_change: Relative price change across the gap
        """
        ldf_kline = pl.scan_parquet(kline_file).filter(pl.col("volume") > 0)
        
        # Primary gap detection: time + price change criteria
        primary_gaps = self._scan_gaps(ldf_kline, self.min_days, self.min_price_chg)
        
        # Secondary gap detection: extended time criteria only
        secondary_gaps = self._scan_gaps(ldf_kline, self.min_days * 2, 0.0)
        
        # Combine and deduplicate
        return pl.concat([primary_gaps, secondary_gaps]).unique(
            "candle_begin_time", 
            keep="last"
        )
    
    def _scan_gaps(self, ldf: pl.LazyFrame, min_days: int, min_price_chg: float) -> pl.LazyFrame:
        """
        Scan for gaps in kline data based on time and price change criteria.
        
        This method:
        1. Calculates the time difference between consecutive candles
        2. Computes the price change ratio from previous close to current open
        3. Identifies gaps meeting both time duration and price change thresholds
        4. Returns a LazyFrame with gap details including timestamps and price info
        
        Args:
            ldf: Input LazyFrame containing kline data with volume > 0
            min_days: Minimum gap duration in days
            min_price_chg: Minimum absolute price change ratio threshold
            
        Returns:
            LazyFrame with columns: prev_begin_time, candle_begin_time, prev_close, open, time_diff, price_change
        """
        ldf = ldf.with_columns(
            pl.col("candle_begin_time").diff().alias("time_diff"),
            (pl.col("open") / pl.col("close").shift() - 1).alias("price_change"),
            pl.col("candle_begin_time").shift().alias("prev_begin_time"),
            pl.col("close").shift().alias("prev_close"),
        )

        min_delta = timedelta(days=min_days)
        ldf_gap = ldf.filter(
            (pl.col("time_diff") > min_delta) & 
            (pl.col("price_change").abs() > min_price_chg)
        )
        
        return ldf_gap.select(
            "prev_begin_time", 
            "candle_begin_time", 
            "prev_close", 
            "open", 
            "time_diff", 
            "price_change"
        )