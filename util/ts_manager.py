import calendar
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

import polars as pl

from config import DataFrequency


def get_partition(dt: datetime, data_freq: DataFrequency) -> str:
    """
    Generate partition name based on given datetime and data frequency.

    Args:
        dt (datetime): Datetime object representing partition time.
        data_freq (DataFrequency): Data frequency enum (yearly, monthly, daily).

    Returns:
        str: Partition name in format YYYY (yearly), YYYYMM (monthly), or YYYYMMDD (daily).
    """
    match data_freq:
        case DataFrequency.yearly:
            # Yearly partition: format as YYYY
            return dt.strftime('%Y')
        case DataFrequency.monthly:
            # Monthly partition: format as YYYYMM
            return dt.strftime('%Y%m')
        case DataFrequency.daily:
            # Daily partition: format as YYYYMMDD
            return dt.strftime('%Y%m%d')


def get_partition_range(part_start: str, part_end: str, data_freq: DataFrequency) -> list[str]:
    """
    Generate list of partitions between start and end based on data frequency.

    Args:
        part_start (str): Start partition name (format matches frequency).
        part_end (str): End partition name (format matches frequency).
        data_freq (DataFrequency): Data frequency enum.

    Returns:
        list[str]: List of partition names in the specified range.
    """
    match data_freq:
        case DataFrequency.yearly:
            # Handle yearly partitions
            year_start, year_end = int(part_start), int(part_end)
            return [str(year) for year in range(year_start, year_end + 1)]

        case DataFrequency.monthly:
            # Handle monthly partitions
            date_format = "%Y%m"  # Format: YYYYMM
            freq = '1mo'  # Monthly interval

        case DataFrequency.daily:
            # Handle daily partitions
            date_format = "%Y%m%d"  # Format: YYYYMMDD
            freq = '1d'  # Daily interval

    # Common logic for monthly and daily partitions
    start_date = datetime.strptime(part_start, date_format)
    end_date = datetime.strptime(part_end, date_format)

    # Generate date range using polars
    date_range = pl.date_range(start_date, end_date, interval=freq, eager=True)

    return [date.strftime(date_format) for date in date_range]


def get_partition_start_end(partition_name: str, data_freq: DataFrequency) -> tuple[datetime, datetime]:
    """
    Get start and end datetime of a partition based on its name and frequency.

    Args:
        partition_name (str): Partition name in format YYYY, YYYYMM, or YYYYMMDD.
        data_freq (DataFrequency): Data frequency enum.

    Returns:
        tuple[datetime, datetime]: Start and end datetime of the partition.
    """
    year = int(partition_name[:4])

    if data_freq == DataFrequency.yearly:
        # Yearly partition bounds
        start = datetime(year=year, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
        end = datetime(year=year, month=12, day=31, hour=23, minute=59, second=59, tzinfo=timezone.utc)
        return start, end

    month = int(partition_name[4:6])

    if data_freq == DataFrequency.monthly:
        # Monthly partition bounds
        start = datetime(year=year, month=month, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
        _, end_day = calendar.monthrange(year, month)
        end = datetime(year=year, month=month, day=end_day, hour=23, minute=59, second=59, tzinfo=timezone.utc)
        return start, end

    # Daily partition bounds
    day = int(partition_name[6:8])
    start = datetime(year=year, month=month, day=day, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    end = datetime(year=year, month=month, day=day, hour=23, minute=59, second=59, tzinfo=timezone.utc)

    return start, end


class TSManager:
    """
    Time-series data manager for handling partitioned data storage.

    Attributes:
        data_dir (Path): Root directory for data storage.
        data_freq (DataFrequency): Partitioning frequency (yearly/monthly/daily).
        save_type (SaveType): Data storage format (Parquet/Pickle).
        time_key (str): Name of the timestamp column, defaults to 'candle_begin_time'.
    """

    def __init__(self,
                 data_dir: str | Path,
                 time_key: str = 'candle_begin_time',
                 data_freq: Literal['daily', 'monthly', 'yearly'] | DataFrequency = DataFrequency.monthly):
        """
        Initialize time-series data manager.

        Args:
            data_dir (str | Path): Path to data storage directory.
            time_key (str): Name of the timestamp column.
            data_freq (Literal | DataFrequency): Partition frequency, defaults to monthly.
        """
        self.data_dir = Path(data_dir)
        self.data_freq = DataFrequency(data_freq)
        self.time_key = time_key

        # Create data directory if not exists
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def format_data_file(self, partition_name: str) -> Path:
        """
        Generate full file path for a partition.

        Args:
            partition_name (str): Partition name.

        Returns:
            Path: Full path to the data file.
        """
        filename = f'{partition_name}.pqt'
        data_file = self.data_dir / filename
        return data_file

    def has_partition(self, partition_name: str) -> bool:
        data_file = self.format_data_file(partition_name)
        return data_file.exists()

    def read_partition(self, partition_name: str) -> pl.DataFrame | None:
        """
        Read data from specified partition.

        Args:
            partition_name (str): Name of the partition to read.

        Returns:
            pl.DataFrame | None: Loaded data or None if read fails.
        """

        if not self.has_partition(partition_name):
            return None

        try:
            data_file = self.format_data_file(partition_name)
            return pl.read_parquet(data_file)
        except:
            data_file.unlink()  # Remove corrupted file
            return None

    def write_partition(self, partition_name: str, df: pl.DataFrame):
        """
        Write data to specified partition.

        Args:
            partition_name (str): Partition name to write to.
            df (pl.DataFrame): Data to write.
        """
        data_file = self.format_data_file(partition_name)

        if df.is_empty() and data_file.exists():
            data_file.unlink()
            return

        df.write_parquet(data_file)

    def list_partitions(self) -> list[str]:
        """
        List all existing partitions.

        Returns:
            list[str]: Sorted list of partition names.
        """
        files = self.data_dir.glob(f'*.pqt')
        partition_names = sorted(f.stem for f in files)
        return partition_names

    def read_all(self) -> pl.DataFrame | None:
        """
        Read and merge data from all partitions.

        Returns:
            pl.DataFrame: Merged data sorted by time column.
        """
        partition_names = self.list_partitions()
        dfs = [self.read_partition(partition_name) for partition_name in partition_names]
        ldfs = [df.lazy() for df in dfs if df is not None]

        if not ldfs:
            return None

        ldf = pl.concat(ldfs)
        ldf = ldf.unique(self.time_key, keep='last')
        ldf = ldf.sort(self.time_key)
        return ldf.collect()

    def trim_partition(self, partition_name: str, dt_start: datetime, dt_end: datetime):
        """
        Trim partition data to keep only records within [dt_start, dt_end).

        Args:
            partition_name (str): Partition to trim.
            dt_start (datetime): Start time (inclusive).
            dt_end (datetime): End time (exclusive).
        """
        df_part = self.read_partition(partition_name)
        if df_part is None:
            return
        df_part = df_part.filter(pl.col(self.time_key).is_between(dt_start, dt_end, 'left'))
        self.write_partition(partition_name, df_part)

    def trim(self, dt_start: datetime, dt_end: datetime):
        """
        Trim all partitions to keep data within [dt_start, dt_end).

        Args:
            dt_start (datetime): Start time (inclusive).
            dt_end (datetime): End time (exclusive).
        """
        partition_names = self.list_partitions()
        for partition_name in partition_names:
            self.trim_partition(partition_name, dt_start, dt_end)

    def update_partition(self, partition_name: str, df: pl.DataFrame):
        """
        Update data in a specific partition.

        Args:
            partition_name (str): Partition to update.
            df (pl.DataFrame): New data containing updates.
        """
        part_start, part_end = get_partition_start_end(partition_name, self.data_freq)
        df_update = df.filter(pl.col(self.time_key).is_between(part_start, part_end, 'left'))

        if df_update.is_empty():
            return

        df_part = self.read_partition(partition_name)
        if df_part is None:
            self.write_partition(partition_name, df_update)
            return

        ldf = pl.concat([df_part.lazy(), df_update.lazy()])
        ldf = ldf.unique(self.time_key, keep='last')
        ldf = ldf.sort(self.time_key)
        self.write_partition(partition_name, ldf.collect())

    def update(self, df: pl.DataFrame):
        """
        Update all relevant partitions with new data.

        Args:
            df (pl.DataFrame): New data containing updates.
        """
        if df.is_empty():
            return

        dt_min = df[self.time_key].min()
        dt_max = df[self.time_key].max()

        partition_min = get_partition(dt_min, self.data_freq)
        partition_max = get_partition(dt_max, self.data_freq)

        partitions_update = get_partition_range(partition_min, partition_max, self.data_freq)

        for partition_name in partitions_update:
            self.update_partition(partition_name, df)

    def get_partition_row_count_per_date(self, partition_name):
        df = self.read_partition(partition_name)
        if df is None:
            return None
        result = df.group_by(pl.col(self.time_key).dt.date().alias('dt')).agg(row_count=pl.count())
        return result

    def get_row_count_per_date(self):
        partitions = self.list_partitions()

        dfs = [self.get_partition_row_count_per_date(part) for part in partitions]
        dfs = [df for df in dfs if df is not None]

        if not dfs:
            return None

        df_cnt = pl.concat(dfs).sort('dt')
        df_dt = pl.DataFrame({'dt': pl.date_range(df_cnt['dt'].min(), df_cnt['dt'].max(), '1D', eager=True)})
        df_cnt = df_cnt.join(df_dt, on='dt', how='full', maintain_order='right', coalesce=True)
        df_cnt = df_cnt.with_columns(pl.col('row_count').fill_null(0))
        return df_cnt
