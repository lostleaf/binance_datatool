#!/usr/bin/env bash

python bhds.py aws_kline download-spot 1m
python bhds.py aws_kline download-spot 1m
python bhds.py aws_kline verify-type-all spot 1m

python bhds.py aws_kline download-um-futures 1m
python bhds.py aws_kline download-um-futures 1m
python bhds.py aws_kline verify-type-all um_futures 1m

python bhds.py aws_kline download-cm-futures 1m
python bhds.py aws_kline download-cm-futures 1m
python bhds.py aws_kline verify-type-all cm_futures 1m

