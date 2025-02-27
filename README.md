# BHDS
con
BHDS, short for Binance Historical Data Service, is an open-source project designed for downloading and locally maintaining historical market data from Binance. 

BHDS is primarily intended for cryptocurrency quantitative trading research using Python. It uses the open-source [Aria2](https://aria2.github.io/) downloader to retrieve historical market data, such as candlestick (K-line) and funding rates, from [Binance's official AWS historical data repository](https://data.binance.vision/). The data is then converted into a DataFrame using [Polars](https://pola.rs/) and stored in the Parquet format, facilitating efficient access for quantitative research.

This project is released under the **MIT License**.

Here's a refined version of your documentation:

## Dependencies

To set up the conda environment, refer to the `environment.yml` file.

By default, the BHDS service uses `$HOME/crypto_data` as the base directory, where all data is downloaded. To change this base directory, set the `CRYPTO_BASE_DIR` environment variable accordingly.

The BHDS service utilizes `aria2`, an efficient cross-platform command-line download utility, which is included in the `environment.yml` file.

## Usage

Run the scripts in order:

``` bash

./aws_download.sh
./aws_parse.sh
./api_download.sh
./gen_kline.sh
```
