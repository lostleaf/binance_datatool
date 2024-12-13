import polars as pl

from zipfile import ZipFile


def read_aws_kline_csv(p, eager=True):
    columns = [
        'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ]
    schema = {
        'candle_begin_time': pl.Int64,
        'open': pl.Float64,
        'high': pl.Float64,
        'low': pl.Float64,
        'close': pl.Float64,
        'volume': pl.Float64,
        'quote_volume': pl.Float64,
        'trade_num': pl.Int64,
        'taker_buy_base_asset_volume': pl.Float64,
        'taker_buy_quote_asset_volume': pl.Float64
    }
    with ZipFile(p) as f:
        filename = f.namelist()[0]
        lines = f.open(filename).readlines()

        if lines[0].decode().startswith('open_time'):
            # logger.warning(f'{p} skip header')
            lines = lines[1:]

    # Use Polars to read the CSV file
    df_lazy = pl.scan_csv(lines, has_header=False, new_columns=columns, schema_overrides=schema)

    # Remove useless columns
    df_lazy = df_lazy.drop('ignore', 'close_time')

    # Cast column types
    df_lazy = df_lazy.with_columns(pl.col('candle_begin_time').cast(pl.Datetime('ms')).dt.replace_time_zone('UTC'))

    if eager:
        return df_lazy.collect()

    return df_lazy


def read_aws_symbol_kline(aws_csv_paths, api_parquet_paths):
    df_aws = pl.concat(read_aws_kline_csv(p, eager=False) for p in aws_csv_paths)
    df_api = pl.read_parquet(api_parquet_paths, columns=df_aws.columns)

    df_lazy = pl.concat([df_aws.lazy(), df_api.lazy()])
    df_lazy = df_lazy.unique('candle_begin_time').sort('candle_begin_time')
    return df_lazy.collect()
