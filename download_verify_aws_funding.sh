#!/usr/bin/env bash

python bhds.py aws_funding download-um-futures
python bhds.py aws_funding verify-type-all um_futures

python bhds.py aws_funding download-cm-futures
python bhds.py aws_funding verify-type-all cm_futures
