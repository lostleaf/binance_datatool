#!/bin/sh

export PY=~/anaconda3/envs/crypto/bin/python

trade_types=( "spot" "usdt_futures" "coin_futures" )

for i in 1 2
do
    # Download 1H candlestick data for all trade types
    $PY cli.py bhds get_aws_all 1h

    for trade_type in "${trade_types[@]}"
    do 
        # Verify 1H usdt perpetual candlestick data and delete corrupted
        $PY cli.py bhds verify_aws_candle $trade_type 1h
    done
done

for trade_type in "${trade_types[@]}"
do 
    # Update latest exchange info
    $PY cli.py bhds update_exchange_info $trade_type
    # Download missing 1H candlestick data from market data api 
    $PY cli.py bhds download_aws_missing_candle $trade_type 1h
    # Convert 1H candlestick data to Pandas Parquet
    $PY cli.py bhds convert_aws_candle_csv $trade_type 1h
    # Split 1H candlestick and fill gaps
    $PY cli.py bhds fix_candle aws $trade_type 1h
done

# Download recent 30 days aggtrades data for given symbol
$PY cli.py bhds get_aws_aggtrades usdt_futures --recent=30 BTCUSDT ETHUSDT
# Verify aggtrades data and delete corrupted
$PY cli.py bhds verify_aws_aggtrades usdt_futures
