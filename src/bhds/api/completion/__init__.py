"""Data completion API

Provides independent detectors and executors for data completion tasks."""

from .detector import DailyKlineDetector, FundingRateDetector
from .executor import DataExecutor
from .task import CompletionOperation, CompletionTask

__all__ = [
    "DailyKlineDetector",
    "FundingRateDetector",
    "DataExecutor",
    "CompletionOperation",
    "CompletionTask",
]
