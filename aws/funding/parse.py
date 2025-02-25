import polars as pl
from zipfile import ZipFile


def read_funding_csv(funding_file):
    """
    Read and parse a funding rate CSV file from a zip archive.

    This function extracts data from a zipped CSV file containing funding rate information,
    processes it to the correct format, and returns a polars DataFrame.

    Args:
        funding_file (Path): Path to the zipped CSV file containing funding rate data.

    Returns:
        pl.DataFrame: A DataFrame containing the parsed funding rate data with properly
                     formatted timestamps and columns.
    """
    with ZipFile(funding_file) as f:
        filename = f.namelist()[0]
        lines = f.open(filename).readlines()
    if lines[0].decode().startswith("calc_time"):
        lines = lines[1:]

    columns = ["funding_time", "funding_interval_hours", "funding_rate"]
    schema = {
        "funding_time": pl.Int64,
        "funding_interval_hours": pl.Int64,
        "funding_rate": pl.Float64,
    }
    ldf = pl.scan_csv(lines, has_header=False, new_columns=columns, schema_overrides=schema)
    ldf = ldf.with_columns(
        (pl.col("funding_time") - pl.col("funding_time") % (60 * 60 * 1000)).alias("candle_begin_time")
    )
    ldf = ldf.with_columns(
        pl.col("candle_begin_time").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC"),
        pl.col("funding_time").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC"),
    )

    return ldf.collect()
