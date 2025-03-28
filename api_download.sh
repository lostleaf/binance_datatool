#!/usr/bin/env bash

interval=${1:-1m}

# Download missing kline data for spot trading
python bhds.py api_data download-aws-missing-kline-type spot $interval
# Download missing kline data for USDⓈ-M Futures
python bhds.py api_data download-aws-missing-kline-type um_futures $interval
# Download missing kline data for COIN-M Futures
python bhds.py api_data download-aws-missing-kline-type cm_futures $interval

# Download recent funding rate data for USDⓈ-M Futures
python bhds.py api_data download-recent-funding-type um_futures
# Download recent funding rate data for COIN-M Futures
python bhds.py api_data download-recent-funding-type cm_futures
