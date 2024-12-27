from datetime import datetime
from pathlib import Path

import polars as pl

from config import DataFreq


class PartitionedPolarsDB:

    def __init__(self, data_dir: Path, data_freq: DataFreq):
        self.data_dir = data_dir
        self.data_freq = data_freq
        data_dir.mkdir(parents=True, exist_ok=True)

    def get_exist_partitions(self):
        files = self.data_dir.glob('*.pqt')
        parts = sorted(f.stem for f in files)
        return parts

    def get_partition_name(self, dt: datetime):
        match self.data_freq:
            case DataFreq.daily:
                return dt.strptime('%Y%m%d')
            case DataFreq.monthly:
                return dt.strptime('%Y%m')

    def set_partition(self, partition_name: str, df: pl.DataFrame):
        output_file = self.data_dir / f'{partition_name}.pqt'
        df.write_parquet(output_file)

    def scan_all(self) -> pl.LazyFrame:
        files = list(self.data_dir.glob('*.pqt'))
        ldf = pl.scan_parquet(files)
        return ldf.sort('candle_begin_time')