from typing import Optional, Dict, List
from pathlib import Path
import polars as pl


class HoloKlineSplitter:
    """
    Split kline data based on detected gaps.
    
    This class provides functionality to split kline DataFrames into segments based on detected gaps
    """
    
    def __init__(self, prefix: str = "SP"):
        """
        Initialize the splitter.
        
        Args:
            prefix: Prefix for split segment names (default: "SP")
        """
        self.prefix = prefix
    
    def split(
        self, 
        df: pl.DataFrame, 
        df_gap: pl.DataFrame, 
        symbol: str
    ) -> Optional[Dict[str, pl.DataFrame]]:
        """
        Split DataFrame into segments based on gaps.
        
        Args:
            df: Original kline DataFrame to split
            df_gap: DataFrame containing gap information with columns:
                   - prev_begin_time: Start time of gap
                   - candle_begin_time: End time of gap
            symbol: Trading pair symbol (e.g., "BTCUSDT")
        
        Returns:
            Dictionary mapping split symbol names to DataFrames, or None if no valid splits
            Naming pattern:
            - First n-1 segments: "{prefix}{i}_{symbol}" (e.g., "SP0_BTCUSDT")
            - Final segment: original symbol name (e.g., "BTCUSDT")
        """
        # Handle case with no gaps
        if df_gap.is_empty():
            return {symbol: df}
        
        # Collect segments
        segments = []
        prev_gap_end = None
        
        # Process each gap to create segments
        for gap in df_gap.iter_rows(named=True):
            # Build filter condition for this segment
            condition = pl.col("candle_begin_time") <= gap['prev_begin_time']
            if prev_gap_end is not None:
                condition = condition & (pl.col("candle_begin_time") >= prev_gap_end)
            
            segment_df = df.filter(condition)
            
            # Skip empty segments
            if not segment_df.is_empty():
                segments.append(segment_df)
                prev_gap_end = gap['candle_begin_time']
        
        # Add final segment after last gap
        if prev_gap_end is not None:
            final_segment = df.filter(pl.col("candle_begin_time") >= prev_gap_end)
            if not final_segment.is_empty():
                segments.append(final_segment)
        
        # Handle case with no valid segments
        if not segments:
            return None
        
        # Generate result dictionary with proper naming
        result = {}
        for i, segment_df in enumerate(segments):
            if i < len(segments) - 1:
                # Prefix naming for non-final segments
                segment_symbol = f"{self.prefix}{i}_{symbol}"
            else:
                # Keep original symbol for final segment
                segment_symbol = symbol
            result[segment_symbol] = segment_df
        
        return result
    
    def split_file(
        self, 
        kline_file: Path, 
        df_gap: pl.DataFrame
    ) -> List[Path]:
        """
        Split kline file and write segments to same directory.
        
        Args:
            kline_file: Path to kline parquet file
            df_gap: DataFrame containing gap information
        
        Returns:
            List of paths to generated segment files
        """
        # Read kline data
        df = pl.read_parquet(kline_file)
        
        # Extract symbol from filename
        symbol = kline_file.stem
        
        # Split data
        segments = self.split(df, df_gap, symbol)
        
        if segments is None:
            return []
        
        # Write segments to files
        output_files = []
        for segment_symbol, segment_df in segments.items():
            output_file = kline_file.parent / f"{segment_symbol}.parquet"
            segment_df.write_parquet(output_file)
            output_files.append(output_file)
        
        return output_files