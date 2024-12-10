#!/usr/bin/env bash

python bhds.py download-um-futures-klines 1m 5m 1h
python bhds.py verify-klines-all-symbols um_futures 1m 5m 1h

python bhds.py download-cm-futures-klines 1m 5m 1h
python bhds.py verify-klines-all-symbols cm_futures 1m 5m 1h

python bhds.py download-spot-klines 1m 5m 1h
python bhds.py verify-klines-all-symbols spot 1m 5m 1h
