#!/usr/bin/env bash

# Download funding rate data for USDⓈ-M Futures
python bhds.py aws_funding download-um-futures
# Verify funding rate data for USDⓈ-M Futures
python bhds.py aws_funding verify-type-all um_futures

# Download funding rate data for COIN-M Futures
python bhds.py aws_funding download-cm-futures
# Verify funding rate data for COIN-M Futures
python bhds.py aws_funding verify-type-all cm_futures

# Download 1-minute kline data for spot trading
python bhds.py aws_kline download-spot 1m
# Verify 1-minute kline data for spot trading
python bhds.py aws_kline verify-type-all spot 1m

# Download 1-minute kline data for USDⓈ-M Futures
python bhds.py aws_kline download-um-futures 1m
# Verify 1-minute kline data for USDⓈ-M Futures
python bhds.py aws_kline verify-type-all um_futures 1m

# Download 1-minute kline data for COIN-M Futures
python bhds.py aws_kline download-cm-futures 1m
# Verify 1-minute kline data for COIN-M Futures
python bhds.py aws_kline verify-type-all cm_futures 1m
