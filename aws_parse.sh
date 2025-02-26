#!/usr/bin/env bash

python bhds.py aws_funding parse-type-all um_futures
python bhds.py aws_funding parse-type-all cm_futures

python bhds.py aws_kline parse-type-all spot 1m
python bhds.py aws_kline parse-type-all um_futures 1m
python bhds.py aws_kline parse-type-all cm_futures 1m
