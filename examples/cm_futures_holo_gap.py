#!/usr/bin/env python3
"""
Batch processing example for cm_futures 1m holo klines with gap detection

This script demonstrates:
1. Using Holo1mKlineMerger.generate_all() for cm_futures
2. Batch processing with execute_polars_batch
3. Gap detection for all symbols
4. Only showing symbols with actual gaps
5. Summary statistics
"""

import tempfile
from pathlib import Path
import polars as pl

from bdt_common.enums import TradeType
from bdt_common.polars_utils import execute_polars_batch
from bhds.holo_kline.merger import Holo1mKlineMerger
from bhds.holo_kline.gap_detector import HoloKlineGapDetector


def main():
    """Main execution function"""
    
    # Configuration
    parsed_data_dir = Path.home() / "crypto_data" / "binance_data" / "parsed_data"
    trade_type = TradeType.cm_futures
    
    print("=" * 80)
    print("CM Futures 1m Holo Kline Batch Processing with Gap Detection")
    print("=" * 80)
    print(f"📊 Input directory: {parsed_data_dir}")
    print(f"📝 Trade type: {trade_type}")
    
    # Gap detection parameters
    min_days = 1
    min_price_chg = 0.1
    
    print(f"\n📊 Gap Detection Configuration:")
    print(f"     Min days: {min_days}")
    print(f"     Min price change: {min_price_chg * 100}%")
    
    with tempfile.TemporaryDirectory(prefix="cm_futures_holo_gap_") as temp_dir:
        temp_path = Path(temp_dir)
        print(f"\n📁 Temp directory: {temp_path}")
        
        try:
            # Step 1: Generate all cm_futures 1m holo klines
            print(f"\n🔄 Step 1: Generating cm_futures 1m holo klines...")
            merger = Holo1mKlineMerger(
                trade_type=trade_type,
                base_dir=parsed_data_dir,
                include_vwap=True,
                include_funding=True,
            )
            
            # Generate all symbols - this creates the parquet files
            lazy_frames = merger.generate_all(temp_path)
            if not lazy_frames:
                print("❌ No symbols found to process")
                return
            
            # The LazyFrames in results will create files when collected
            # We need to collect them to trigger file creation
            execute_polars_batch(lazy_frames, "Collecting kline data")
            
            # Step 2: Detect gaps in all generated files
            print(f"\n🔄 Step 2: Detecting gaps in all symbols...")
            
            detector = HoloKlineGapDetector(min_days, min_price_chg)
            
            # Get actual generated files
            generated_files = list(temp_path.glob("*.parquet"))
            
            # Generate gap detection tasks
            gap_tasks = [detector.detect(file_path) for file_path in generated_files]
            gap_results = execute_polars_batch(gap_tasks, "Detecting gaps", return_results=True)

            if not gap_results:
                print("❌No gap results returned")
                return
            
            # Step 3: Analyze and display results
            symbols_with_gaps = 0
            total_symbols = len(generated_files)
            
            # Process gap results with symbols
            for file_path, gaps_df in zip(generated_files, gap_results):
                if len(gaps_df) > 0:
                    symbol = file_path.stem
                    symbols_with_gaps += 1
                    
                    print(f"\n🔍 {symbol} - {len(gaps_df)} gap(s)")
                    print("-" * 40)
                    
                    for gap in gaps_df.sort("time_diff", descending=True).iter_rows(named=True):
                        print(f"  {gap['prev_begin_time']} → {gap['candle_begin_time']}")
                        print(f"  Duration: {gap['time_diff']}, Change: {gap['price_change']:.2%}")
            
            # Summary
            print(f"\n📈 Summary: {symbols_with_gaps}/{total_symbols} symbols have gaps")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()