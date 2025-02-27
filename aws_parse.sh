#!/usr/bin/env bash

# Parse AWS funding rate data for USDⓈ-M Futures
python bhds.py aws_funding parse-type-all um_futures
# Parse AWS funding rate data for COIN-M Futures
python bhds.py aws_funding parse-type-all cm_futures

# Parse AWS 1-minute kline data for spot trading
python bhds.py aws_kline parse-type-all spot 1m
# Parse AWS 1-minute kline data for USDⓈ-M Futures
python bhds.py aws_kline parse-type-all um_futures 1m
# Parse AWS 1-minute kline data for COIN-M Futures
python bhds.py aws_kline parse-type-all cm_futures 1m
