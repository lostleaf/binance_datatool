from enum import Enum
from pathlib import Path
from typing import List, Dict, Tuple
import pandas as pd
import polars as pl
from datetime import datetime
import pickle
from abc import ABC, abstractmethod

from config import BINANCE_DATA_DIR, TradeType
from config.config import _BASE_DIR


class DataLoader(ABC):
    """数据加载器抽象基类"""

    @abstractmethod
    def get_data_dir(self, trade_type: TradeType) -> Path:
        """获取数据目录"""
        pass

    @abstractmethod
    def load_data(self, symbol: str, trade_type: TradeType) -> pd.DataFrame:
        """加载数据"""
        pass


class BHDSLoader(DataLoader):
    """BHDS数据加载器"""

    def get_data_dir(self, trade_type: TradeType) -> Path:
        return BINANCE_DATA_DIR / "results_data" / trade_type.value / "resampled_klines" / "1h" / "0m"

    def load_data(self, symbol: str, trade_type: TradeType) -> pd.DataFrame:
        data_dir = self.get_data_dir(trade_type)
        df = pl.read_parquet(data_dir / f"{symbol}.pqt").to_pandas()
        df["candle_begin_time"] = pd.to_datetime(df["candle_begin_time"])
        return df

    def get_all_symbols(self, trade_type: TradeType) -> List[str]:
        data_dir = self.get_data_dir(trade_type)
        return [f.stem for f in data_dir.glob("*.pqt")]


class QuantClassLoader(DataLoader):
    """Quantclass数据加载器"""

    def get_data_dir(self, trade_type: TradeType) -> Path:
        return _BASE_DIR / "quantclass_data" / "coin-binance-spot-swap-preprocess-pkl-1h"

    def load_data(self, symbol: str, trade_type: TradeType) -> pd.DataFrame:
        data_dir = self.get_data_dir(trade_type)
        pkl_file = "spot_dict.pkl" if trade_type == TradeType.spot else "swap_dict.pkl"
        data_dict = pickle.load(open(data_dir / pkl_file, "rb"))

        symbol_qtc = f"{symbol[:-4]}-{symbol[-4:]}"
        df = data_dict[symbol_qtc].copy()
        df["candle_begin_time"] = df["candle_begin_time"].dt.tz_localize("UTC")
        df.rename({"funding_fee": "funding_rate"}, axis=1, inplace=True)
        return df


class DataSource(str, Enum):
    """数据来源枚举"""

    BHDS = "bhds"  # BHDS数据（按symbol分开存储）
    QUANTCLASS = "qtc"  # Quantclass数据（所有symbol存储在一个pkl中）

    @property
    def loader(self) -> DataLoader:
        """获取对应的数据加载器"""
        loaders = {DataSource.BHDS: BHDSLoader(), DataSource.QUANTCLASS: QuantClassLoader()}
        return loaders[self]


class CompareField(str, Enum):
    """对比字段枚举"""

    OPEN = "open"  # 开盘价
    HIGH = "high"  # 最高价
    LOW = "low"  # 最低价
    CLOSE = "close"  # 收盘价
    VOLUME = "volume"  # 成交量
    QUOTE_VOLUME = "quote_volume"  # 成交额
    TRADE_NUM = "trade_num"  # 成交笔数
    TAKER_BUY_BASE = "taker_buy_base_asset_volume"  # 主动买入成交量
    TAKER_BUY_QUOTE = "taker_buy_quote_asset_volume"  # 主动买入成交额
    FUNDING_RATE = "funding_rate"  # 资金费率

    @property
    def diff_threshold(self) -> float:
        """获取差异阈值"""
        thresholds = {
            self.OPEN: 1e-4,
            self.HIGH: 1e-4,
            self.LOW: 1e-4,
            self.CLOSE: 1e-4,
            self.VOLUME: 1e-4,
            self.QUOTE_VOLUME: 1e-4,
            self.TRADE_NUM: 1e-4,
            self.TAKER_BUY_BASE: 1e-4,
            self.TAKER_BUY_QUOTE: 1e-4,
            self.FUNDING_RATE: 1e-6,
        }
        return thresholds[self]

    @classmethod
    def get_fields_by_trade_type(cls, trade_type: TradeType) -> List["CompareField"]:
        """根据交易类型获取需要对比的字段列表

        Args:
            trade_type: 交易类型

        Returns:
            需要对比的字段列表
        """
        base_fields = [
            cls.OPEN,
            cls.HIGH,
            cls.LOW,
            cls.CLOSE,
            cls.VOLUME,
            cls.QUOTE_VOLUME,
            cls.TRADE_NUM,
            cls.TAKER_BUY_BASE,
            cls.TAKER_BUY_QUOTE,
        ]

        # 只有合约才有资金费率
        if trade_type != TradeType.spot:
            base_fields.append(cls.FUNDING_RATE)

        return base_fields

    @classmethod
    def get_all_fields(cls) -> List[str]:
        """获取所有字段名称"""
        return [field.value for field in cls]


class CompareResult:
    """数据对比结果类"""

    def __init__(self, field: CompareField, max_diff: float, diff_num: int, diff_details: pd.DataFrame):
        self.field = field
        self.max_diff = max_diff
        self.diff_num = diff_num
        self.diff_details = diff_details

    def to_dict(self) -> dict:
        """转换为字典格式，方便序列化"""
        return {
            "field": self.field.value,
            "max_diff": self.max_diff,
            "diff_num": self.diff_num,
            "diff_details": self.diff_details,
        }

    def __str__(self) -> str:
        max_diff_pct = self.diff_details["diff_pct"].abs().max()
        return (
            f"字段: {self.field}, 最大差异: {self.max_diff}, 最大差异百分比: {max_diff_pct}, 差异数量: {self.diff_num}"
        )


class DataComparer:
    """数据对比器"""

    def __init__(self, symbol: str, trade_type: TradeType):
        self.symbol = symbol
        self.trade_type = trade_type

        # 加载数据
        self.df_bhds = DataSource.BHDS.loader.load_data(symbol, trade_type)
        self.df_qtc = DataSource.QUANTCLASS.loader.load_data(symbol, trade_type)

        # 对齐时间范围
        self._align_time_range()

    def _align_time_range(self):
        """对齐两个数据源的时间范围"""

        # 如果时间范围小于2019-01-01，则不对比
        begin_ts = max(
            self.df_bhds["candle_begin_time"].min(),
            self.df_qtc["candle_begin_time"].min(),
            pd.to_datetime("2019-01-01 00:00:00+00:00"),
        )
        end_ts = min(self.df_bhds["candle_begin_time"].max(), self.df_qtc["candle_begin_time"].max())
        self.df_bhds = self.df_bhds[self.df_bhds["candle_begin_time"].between(begin_ts, end_ts)]
        self.df_qtc = self.df_qtc[self.df_qtc["candle_begin_time"].between(begin_ts, end_ts)]

    def compare_field(self, field: CompareField) -> CompareResult:
        """对比单个字段"""
        df = self.df_bhds.merge(
            self.df_qtc[["candle_begin_time", field.value]], on="candle_begin_time", suffixes=("", "_qtc")
        )

        df["diff"] = df[field.value] - df[f"{field.value}_qtc"]
        df["diff_pct"] = df[f"{field.value}_qtc"] / df[field.value] - 1
        df["diff_abs"] = df["diff"].abs()

        max_diff = df["diff_abs"].max()
        diff_num = (df["diff_abs"] > field.diff_threshold).sum()

        diff_details = df[df["diff_abs"] > field.diff_threshold][
            ["candle_begin_time", field.value, f"{field.value}_qtc", "diff", "diff_pct"]
        ]

        return CompareResult(field, max_diff, diff_num, diff_details)

    def compare_all(self) -> Dict[CompareField, CompareResult]:
        """对比所有字段"""
        fields = CompareField.get_fields_by_trade_type(self.trade_type)
        return {field: self.compare_field(field) for field in fields}

    def get_time_range(self) -> Tuple[datetime, datetime]:
        """获取对比的时间范围"""
        return (self.df_bhds["candle_begin_time"].min(), self.df_bhds["candle_begin_time"].max())


def compare_symbol(symbol: str, trade_type: TradeType) -> Dict[CompareField, CompareResult]:
    """对比单个交易对的数据"""
    print(f"\n开始对比 {symbol} {trade_type.value} 的数据...")

    comparer = DataComparer(symbol, trade_type)
    begin_ts, end_ts = comparer.get_time_range()
    print(f"对比时间范围: {begin_ts} 到 {end_ts}")

    results = comparer.compare_all()

    # 打印对比结果
    has_diff = False
    for field, result in results.items():
        print(f"\n{result}")
        if result.diff_num > 0:
            has_diff = True
            # print("\n差异详情:")
            # print(result.diff_details.to_string())

    return results if has_diff else None


def main(symbols: List[str] | None = None, trade_type_list: List[TradeType] | None = None):
    """主函数"""
    # 设置显示选项
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pl.Config.set_tbl_rows(30)

    # 对比不同交易类型（只比较spot和um_futures）
    if trade_type_list is None:
        trade_type_list = [TradeType.spot, TradeType.um_futures]

    # 用于存储所有差异结果
    diff_results = {}

    for trade_type in trade_type_list:
        print(f"\n\n{'='*50}")
        print(f"开始对比 {trade_type.value} 数据")
        print("=" * 50)

        if symbols is None:
            symbols = DataSource.BHDS.loader.get_all_symbols(trade_type)

        print(f"开始处理 {len(symbols)} 个交易对")

        # 该交易类型下的差异结果
        trade_type_results = {}

        for symbol in symbols:
            try:
                results = compare_symbol(symbol, trade_type)
                if results is not None:
                    # 将结果转换为可序列化的格式
                    trade_type_results[symbol] = {
                        field.value: result.to_dict() for field, result in results.items() if result.diff_num > 0
                    }
            except Exception as e:
                print(f"对比 {symbol} {trade_type.value} 时发生错误: {str(e)}")

        if trade_type_results:
            diff_results[trade_type.value] = trade_type_results

    # 如果有差异结果，保存到文件
    if diff_results:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"diff_results_{timestamp}.pkl"

        with open(output_file, "wb") as f:
            pickle.dump(diff_results, f)
        print(f"\n差异结果已保存到: {output_file}")
    else:
        print("\n未发现任何差异")


if __name__ == "__main__":
    main(["BTCUSDT", "ETHUSDT", "BNBUSDT"], [TradeType.spot])
