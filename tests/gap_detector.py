#!/usr/bin/env python3
"""
Test script for HoloKlineGapDetector

This script tests gap detection in holographic 1-minute kline data for:
- Spot 1m LUNAUSDT klines with gap analysis
"""

import tempfile
from pathlib import Path
import polars as pl

from bdt_common.enums import TradeType
from bhds.holo_kline.merger import Holo1mKlineMerger
from bhds.holo_kline.gap_detector import HoloKlineGapDetector


def test_gap_detection():
    """Test gap detection for spot 1m LUNAUSDT klines"""
    # Test configuration
    parsed_data_dir = Path.home() / "crypto_data" / "binance_data" / "parsed_data"
    symbol = "LUNAUSDT"
    trade_type = TradeType.spot
    
    print("=" * 60)
    print("Testing HoloKlineGapDetector")
    print("=" * 60)
    print(f"📊 Input directory: {parsed_data_dir}")
    print(f"📝 Test symbol: {symbol}")
    print(f"📝 Trade type: {trade_type}")
    
    include_vwap = True
    include_funding = False  # Spot markets do not have funding rates
    
    print(f"\n📊 Configuration: VWAP={include_vwap}, Funding={include_funding}")
    print("-" * 40)
    
    # Create temporary output directory
    with tempfile.TemporaryDirectory(prefix="gap_detector_test_") as temp_dir:
        temp_path = Path(temp_dir)
        print(f"📁 Temp directory: {temp_path}")
        print(f"📝 Note: Temp directory will be automatically cleaned up after test")
        
        try:
            # Step 1: Generate holographic kline data
            print(f"\n🔄 Step 1: Generating holographic kline data for {symbol}")
            merger = Holo1mKlineMerger(
                trade_type=trade_type,
                base_dir=parsed_data_dir,
                include_vwap=include_vwap,
                include_funding=include_funding,
            )
            
            output_file = temp_path / f"{symbol}.parquet"
            ldf = merger.generate(symbol, output_file)
            
            # Collect the LazyFrame to trigger file writing
            ldf.collect()
            
            # Read and analyze the generated kline data
            df = pl.read_parquet(output_file)
            print(f"\n📈 Generated kline data:")
            print(f"     Shape: {df.shape}")
            print(f"     Time range: {df['candle_begin_time'].min()} to {df['candle_begin_time'].max()}")
            print(f"     Total records: {len(df)}")
            
            # Step 2: Detect gaps in the generated data
            print(f"\n🔄 Step 2: Detecting gaps in {symbol} kline data")
            
            # Configure gap detector parameters
            min_days = 1
            min_price_chg = 0.1  # 10% price change threshold
            
            detector = HoloKlineGapDetector(
                min_days=min_days,
                min_price_chg=min_price_chg
            )
            
            # Detect gaps
            gaps_ldf = detector.detect(output_file)
            gaps_df = gaps_ldf.collect()
            
            print(f"\n📊 Gap Detection Results:")
            print(f"     Min days: {min_days}")
            print(f"     Min price change: {min_price_chg * 100}%")
            print(f"     Total gaps detected: {len(gaps_df)}")
            
            if len(gaps_df) > 0:
                print(f"\n🔍 Gap Details:")
                print("-" * 60)
                
                # Sort gaps by duration (longest first)
                gaps_df = gaps_df.sort("time_diff", descending=True)
                
                for idx, gap in enumerate(gaps_df.iter_rows(named=True), 1):
                    print(f"\nGap #{idx}:")
                    print(f"     Start time: {gap['prev_begin_time']}")
                    print(f"     End time: {gap['candle_begin_time']}")
                    print(f"     Duration: {gap['time_diff']}")
                    print(f"     Price change: {gap['price_change']:.2%}")
                    print(f"     Previous close: {gap['prev_close']}")
                    print(f"     Next open: {gap['open']}")
                
                # Summary statistics
                print(f"\n📈 Gap Statistics:")
                print(f"     Average gap duration: {gaps_df['time_diff'].mean()}")
                print(f"     Longest gap: {gaps_df['time_diff'].max()}")
                print(f"     Largest price change: {gaps_df['price_change'].abs().max():.2%}")
                print(f"     Price change: {gaps_df['price_change'].min():.2%} to {gaps_df['price_change'].max():.2%}")
            else:
                print(f"     ✅ No gaps detected with current thresholds")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n🧹 Temp directory will be cleaned up automatically")


if __name__ == "__main__":
    test_gap_detection()