# resample 1h spot klines with multiple of 5m offset
python bhds.py generate resample-type spot 1h 5m

# resample 1h um_futures klines with multiple of 5m offset
python bhds.py generate resample-type um_futures 1h 5m

# resample 1h cm_futures klines with multiple of 5m offset
python bhds.py generate resample-type cm_futures 1h 5m

# resample 5m spot klines with 0 offset
python bhds.py generate resample-type spot 5m 0m

# resample 5m um_futures klines with 0 offset
python bhds.py generate resample-type um_futures 5m 0m

