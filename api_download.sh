#!/usr/bin/env bash

# Download missing 1-minute kline data for spot trading
python bhds.py api_data download-aws-missing-kline-type spot 1m
# Download missing 1-minute kline data for USDⓈ-M Futures
python bhds.py api_data download-aws-missing-kline-type um_futures 1m
# Download missing 1-minute kline data for COIN-M Futures
python bhds.py api_data download-aws-missing-kline-type cm_futures 1m

# Download recent funding rate data for USDⓈ-M Futures
python bhds.py api_data download-recent-funding-type um_futures
# Download recent funding rate data for COIN-M Futures
python bhds.py api_data download-recent-funding-type cm_futures
