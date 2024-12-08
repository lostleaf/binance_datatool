# BHDS


BHDS, short for Binance Historical Data Service, is an open-source project designed for downloading and locally maintaining historical market data from Binance. 

BHDS is primarily intended for cryptocurrency quantitative trading research using Python. It uses the open-source [Aria2](https://aria2.github.io/) downloader to retrieve historical market data, such as candlestick (K-line) and funding rates, from [Binance's official AWS historical data repository](https://data.binance.vision/). The data is then converted into a DataFrame using [Polars](https://pola.rs/) and stored in the Parquet format, facilitating efficient access for quantitative research.

This project is released under the **MIT License**.
