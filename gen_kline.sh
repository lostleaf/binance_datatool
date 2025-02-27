#!/usr/bin/env bash

# Generate merged and gaps split kline data for spot trading with VWAP
python bhds.py generate kline-type spot 1m --split-gaps --with-vwap --no-with-funding-rates
# Generate merged and gaps split kline data for USDⓈ-M Futures with VWAP and funding rates
python bhds.py generate kline-type um_futures 1m --split-gaps --with-vwap --with-funding-rates
# Generate merged and gaps split kline data for COIN-M Futures with VWAP and funding rates
python bhds.py generate kline-type cm_futures 1m --split-gaps --with-vwap --with-funding-rates
