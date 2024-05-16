#!/bin/sh

for i in 1 2
do
    # Download 1H candlestick data for all usdt perpetual symbols
    python cli.py bhds get_aws_all_usdt_perpetual 1h
    # Verify 1H usdt perpetual candlestick data and delete corrupted
    python cli.py bhds verify_aws_candle usdt_futures 1h

    # Download 1H candlestick data for all coin perpetual symbols
    python cli.py bhds get_aws_all_coin_perpetual 1h
    # Verify 1H coin perpetual candlestick data and delete corrupted
    python cli.py bhds verify_aws_candle coin_futures 1h

    # Download 1H candlestick data for all usdt spot symbols
    python cli.py bhds get_aws_all_usdt_spot 1h
    # Verify 1H spot candle data and delete corrupted
    python cli.py bhds verify_aws_candle spot 1h
done

# Download usdt perpetual missing 1H candlestick data from market data api 
python cli.py bhds download_aws_missing_candle usdt_futures 1h

# Download spot missing 1H candlestick data from market data api
python cli.py bhds download_aws_missing_candle spot 1h

# Download coin perpetual missing 1H candlestick data from market data api
python cli.py bhds download_aws_missing_candle coin_futures 1h

# Convert 1H usdt perpetual candlestick data to Pandas Parquet
python cli.py bhds convert_aws_candle_csv usdt_futures 1h

# Convert 1H coin perpetual candlestick data to Pandas Parquet
python cli.py bhds convert_aws_candle_csv coin_futures 1h

# Convert 1H spot candlestick data to Pandas Parquet
python cli.py bhds convert_aws_candle_csv spot 1h

# Split 1H usdt perpetual candlestick and fill gaps
python cli.py bhds fix_candle aws usdt_futures 1h

# Split 1H spot candlestick and fill gaps
python cli.py bhds fix_candle aws spot 1h

# Split 1H coin perpetual candlestick and fill gaps
python cli.py bhds fix_candle aws coin_futures 1h

# Download recent 30 days aggtrades data for given symbol
python cli.py bhds get_aws_aggtrades usdt_futures --recent=30 BTCUSDT ETHUSDT
# Verify aggtrades data and delete corrupted
python cli.py bhds verify_aws_aggtrades usdt_futures
