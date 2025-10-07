"""Task abstractions for data completion workflows."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from bdt_common.rest_api.fetcher import BinanceFetcher


class CompletionOperation(str, Enum):
    """Supported operations for completion tasks."""

    GET_KLINE_DF_OF_DAY = "get_kline_df_of_day"
    GET_HIST_FUNDING_RATE = "get_hist_funding_rate"

    def __str__(self) -> str:  # pragma: no cover - convenience for logging
        return self.value


@dataclass(frozen=True, slots=True)
class CompletionTask:
    """Data completion task with deferred execution."""

    operation: CompletionOperation
    params: Mapping[str, Any]
    save_path: Path

    async def execute(self, fetcher: BinanceFetcher) -> Any:
        """Execute the task using the provided fetcher."""

        operation = getattr(fetcher, self.operation.value)
        result = operation(**self.params)
        if inspect.isawaitable(result):
            return await result
        return result

    @property
    def description(self) -> str:
        """Return a human-readable description of the task."""

        return f"{self.operation}({', '.join(f'{k}={v!r}' for k, v in self.params.items())})"
