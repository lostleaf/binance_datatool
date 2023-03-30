import os
import shutil
from glob import glob

import pandas as pd

from util import now_time
from market_api import BinanceMarketApi


class CandleFeatherManager:

    def __init__(self, base_dir):
        self.base_dir = base_dir

    def clear_all(self):
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)

    def format_ready_file_path(self, symbol, run_time):
        run_time_str = run_time.strftime('%Y%m%d_%H%M%S')
        name = f'{symbol}_{run_time_str}.ready'
        file_path = os.path.join(self.base_dir, name)
        return file_path

    def set_candle(self, symbol, run_time, df: pd.DataFrame):
        df_path = os.path.join(self.base_dir, f'{symbol}.fea')
        df.reset_index(drop=True, inplace=True)
        df.to_feather(df_path)

        old_ready_file_paths = glob(os.path.join(self.base_dir, f'{symbol}_*.ready'))
        for p in old_ready_file_paths:
            os.remove(p)

        ready_file_path = self.format_ready_file_path(symbol, run_time)
        with open(ready_file_path, 'w') as fout:
            fout.write(str(now_time()))

    def update_candle(self, symbol, run_time, df_new: pd.DataFrame):
        df_old = self.read_candle(symbol)
        df: pd.DataFrame = pd.concat([df_old, df_new]).drop_duplicates(subset='candle_begin_time', keep='last')
        df.sort_values('candle_begin_time', inplace=True)
        df = df.iloc[-BinanceMarketApi.MAX_ONCE_CANDLES:]
        self.set_candle(symbol, run_time, df)

    def check_ready(self, symbol, run_time):
        ready_file_path = self.format_ready_file_path(symbol, run_time)
        return os.path.exists(ready_file_path)

    def read_candle(self, symbol) -> pd.DataFrame:
        return pd.read_feather(os.path.join(self.base_dir, f'{symbol}.fea'))

    def remove_symbol(self, symbol):
        old_ready_file_paths = glob(os.path.join(self.base_dir, f'{symbol}_*.ready'))
        for p in old_ready_file_paths:
            os.remove(p)
        df_path = os.path.join(self.base_dir, f'{symbol}.fea')
        if os.path.exists(df_path):
            os.remove(df_path)

    def get_all_symbols(self):
        paths = glob(os.path.join(self.base_dir, '*.fea'))
        return [os.path.basename(p).rstrip('.fea') for p in paths]