#!/usr/bin/env bash

python bhds.py aws_kline parse-type-all spot 1m
python bhds.py api_data download-aws-missing-kline-type spot 1m
python bhds.py generate merged-split-kline-type-all spot 1m --split-gaps --with-vwap

python bhds.py aws_kline parse-type-all um_futures 1m
python bhds.py api_data download-aws-missing-kline-type um_futures 1m

python bhds.py aws_kline parse-type-all cm_futures 1m
python bhds.py api_data download-aws-missing-kline-type cm_futures 1m
