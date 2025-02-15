#!/usr/bin/env bash

python bhds.py aws_kline parse-type-all spot 1m
python bhds.py api_data download-aws-missing-kline spot 1m

python bhds.py aws_kline parse-type-all um_futures 1m
python bhds.py api_data download-aws-missing-kline um_futures 1m

python bhds.py aws_kline parse-type-all cm_futures 1m
python bhds.py api_data download-aws-missing-kline cm_futures 1m
