"""Data completion API module

Provides unified data completion interface supporting intelligent completion strategies for different data types.
"""

from .base import BaseCompletion
from .kline import DailyKlineCompletion
from .funding import RecentFundingCompletion


__all__ = [
    # Core framework
    "BaseCompletion",
    # K-line completion strategies
    "DailyKlineCompletion",
    # Funding rate completion strategies
    "RecentFundingCompletion",
]
