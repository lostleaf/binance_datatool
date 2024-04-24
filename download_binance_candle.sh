#!/bin/sh

export PY=python

for i in 1 2
do
    # Download all usdt perptual candle data and verify
    $PY cli.py bhds get_aws_all_usdt_perpetual 1h 1m
    $PY cli.py bhds verify_aws_candle usdt_futures 1h 1m

    # Download all coin perptual candle data and verify
    $PY cli.py bhds get_aws_all_coin_perpetual 1h 1m
    $PY cli.py bhds verify_aws_candle coin_futures 1h 1m

    # Download all usdt spot candle data and verify
    $PY cli.py bhds get_aws_all_usdt_spot 1h 1m
    $PY cli.py bhds verify_aws_candle spot 1h 1m
done

# Convert usdt perptual
$PY cli.py bhds convert_aws_candle_csv usdt_futures 1h 1m

# Convert coin perptual
$PY cli.py bhds convert_aws_candle_csv coin_futures 1h 1m

# Convert usdt spot
$PY cli.py bhds convert_aws_candle_csv spot 1h 1m
