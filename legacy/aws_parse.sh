#!/usr/bin/env bash

# Default interval
interval=${1:-1m}

# Parse AWS funding rate data for USDⓈ-M Futures
python bhds.py aws_funding parse-type-all um_futures
# Parse AWS funding rate data for COIN-M Futures
python bhds.py aws_funding parse-type-all cm_futures

# Parse AWS kline data for spot trading
python bhds.py aws_kline parse-type-all spot $interval
# Parse AWS kline data for USDⓈ-M Futures
python bhds.py aws_kline parse-type-all um_futures $interval
# Parse AWS kline data for COIN-M Futures
python bhds.py aws_kline parse-type-all cm_futures $interval
