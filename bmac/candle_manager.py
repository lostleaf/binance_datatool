import os
import shutil
from glob import glob
import time
import pandas as pd

from util.time import now_time


class CandleFileManager:

    def __init__(self, base_dir, save_type):
        '''
        初始化，设定读写根目录
        '''
        self.base_dir = base_dir
        self.save_type = save_type
        if save_type == 'feather':
            self.ext = 'fea'
        elif save_type == 'parquet':
            self.ext = 'pqt'
        else:
            raise ValueError(f'Save type {save_type} not supported, can only accept feather and parquet')

    def clear_all(self):
        '''
        清空历史文件（如有），并创建根目录
        '''
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)

    def format_ready_file_path(self, symbol, run_time):
        '''
        获取 ready file 文件路径, ready file 为每周期 K线文件锁
        ready file 文件名形如 {symbol}_{runtime年月日}_{runtime_时分秒}.ready
        '''
        run_time_str = run_time.strftime('%Y%m%d_%H%M%S')
        name = f'{symbol}_{run_time_str}.ready'
        file_path = os.path.join(self.base_dir, name)
        return file_path

    def format_data_file_path(self, symbol):
        name = f'{symbol}.{self.ext}'
        file_path = os.path.join(self.base_dir, name)
        return file_path

    def save_data_file(self, symbol, df: pd.DataFrame):
        df_path = self.format_data_file_path(symbol)
        if self.save_type == 'feather':
            df = df.reset_index(drop=True)
            df.to_feather(df_path)
        elif self.save_type == 'parquet':
            df.to_parquet(df_path)

    def set_candle(self, symbol, run_time, df: pd.DataFrame):
        '''
        设置K线，首先将新的K线 DataFrame 写入 Feather，然后删除旧 ready file，并生成新 ready file
        '''
        self.save_data_file(symbol, df)

        old_ready_file_paths = glob(os.path.join(self.base_dir, f'{symbol}_*.ready'))
        for p in old_ready_file_paths:
            os.remove(p)

        if run_time is not None:
            ready_file_path = self.format_ready_file_path(symbol, run_time)
            with open(ready_file_path, 'w') as fout:
                fout.write(str(now_time()))

    def update_candle(self, symbol, run_time, df_new: pd.DataFrame, num_candles):
        '''
        使用新获取的K线，更新 symbol 对应K线 Feather，主要用于每周期K线更新
        '''
        if self.has_symbol(symbol):
            df_old = self.read_candle(symbol)
            df: pd.DataFrame = pd.concat([df_old, df_new])
        else:
            df = df_new
        df.sort_values('candle_begin_time', inplace=True)
        df.drop_duplicates(subset='candle_begin_time', keep='last', inplace=True)
        if num_candles is not None:
            df = df.iloc[-num_candles:]
        self.set_candle(symbol, run_time, df)

        return df

    def check_ready(self, symbol, run_time):
        '''
        检查 symbol 对应的 ready file 是否存在，如存在，则表示 run_time 周期 K线已获取并写入 Feather
        '''
        ready_file_path = self.format_ready_file_path(symbol, run_time)
        return os.path.exists(ready_file_path)

    def read_candle(self, symbol) -> pd.DataFrame:
        '''
        读取 symbol 对应的 K线
        '''
        df_path = self.format_data_file_path(symbol)
        if self.save_type == 'feather':
            df = pd.read_feather(df_path)
            return df
        elif self.save_type == 'parquet':
            return pd.read_parquet(df_path)

    def has_symbol(self, symbol) -> bool:
        '''
        检查某 symbol 数据文件是否存在
        '''
        df_path = self.format_data_file_path(symbol)
        return os.path.exists(df_path)

    def remove_symbol(self, symbol):
        '''
        移除 symbol，包括删除对应的数据文件和 ready file
        '''
        old_ready_file_paths = glob(os.path.join(self.base_dir, f'{symbol}_*.ready'))
        for p in old_ready_file_paths:
            os.remove(p)
        df_path = self.format_data_file_path(symbol)
        if os.path.exists(df_path):
            os.remove(df_path)

    def get_all_symbols(self):
        '''
        获取当前所有 symbol
        '''
        paths = glob(os.path.join(self.base_dir, f'*.{self.ext}'))
        return [os.path.splitext(os.path.basename(p))[0] for p in paths]
