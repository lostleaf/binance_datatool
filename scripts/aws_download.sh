#!/usr/bin/env bash

interval=${1:-1m}

# Download funding rate data for USDⓈ-M Futures
python bhds.py aws_funding download-um-futures
# Verify funding rate data for USDⓈ-M Futures
python bhds.py aws_funding verify-type-all um_futures

# Download funding rate data for COIN-M Futures
python bhds.py aws_funding download-cm-futures
# Verify funding rate data for COIN-M Futures
python bhds.py aws_funding verify-type-all cm_futures

# Download kline data for spot trading
python bhds.py aws_kline download-spot "$interval"
# Verify kline data for spot trading
python bhds.py aws_kline verify-type-all spot "$interval"

# Download kline data for USDⓈ-M Futures
python bhds.py aws_kline download-um-futures "$interval"
# Verify kline data for USDⓈ-M Futures
python bhds.py aws_kline verify-type-all um_futures "$interval"

# Download kline data for COIN-M Futures
python bhds.py aws_kline download-cm-futures "$interval"
# Verify kline data for COIN-M Futures
python bhds.py aws_kline verify-type-all cm_futures "$interval"
