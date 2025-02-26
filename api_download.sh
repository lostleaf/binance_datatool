#!/usr/bin/env bash

python bhds.py api_data download-aws-missing-kline-type spot 1m
python bhds.py api_data download-aws-missing-kline-type um_futures 1m
python bhds.py api_data download-aws-missing-kline-type cm_futures 1m

python bhds.py api_data download-recent-funding-type um_futures
python bhds.py api_data download-recent-funding-type cm_futures
