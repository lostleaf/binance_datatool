"""Data completion strategy framework

Provides unified data completion interface supporting intelligent completion strategies for different data types.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import polars as pl

from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import logger
from bdt_common.rest_api.fetcher import BinanceFetcher
from bdt_common.time import async_sleep_until_run_time, next_run_time
from bhds.aws.local import LocalAwsClient


class BaseCompletion(ABC):
    """Abstract base class for data completion strategies"""

    def __init__(
        self,
        trade_type: TradeType,
        data_type: DataType,
        data_freq: DataFrequency,
        local_client: LocalAwsClient,
        fetcher: BinanceFetcher,
        interval: Optional[str] = None,
    ):
        self.trade_type = trade_type
        self.data_type = data_type
        self.data_frequency = data_freq
        self.local_client = local_client
        self.fetcher = fetcher
        self.interval = interval

    @abstractmethod
    def get_missings(self, symbols: List[str]) -> List[Tuple[Callable[..., Awaitable], Dict, Path]]:
        """Get missing data information for symbols

        Args:
            symbols: Trading symbol list

        Returns:
            List of tuples containing:
            - Fetcher async function to call
            - Keyword arguments dict for the function
            - File path where to save the result
        """
        pass

    async def complete_multiple_symbols(self, symbols: List[str], batch_size: int = 40, **kwargs) -> dict:
        """Complete data for multiple symbols

        Args:
            symbols: Trading symbol list
            batch_size: Batch size
            **kwargs: Strategy-specific parameters

        Returns:
            dict: Completion result
        """
        logger.info(f"Starting data completion for {len(symbols)} symbols")

        # Get missing data information
        missing_tasks = self.get_missings(symbols, **kwargs)
        logger.info(f"Found {len(missing_tasks)} missing data tasks")

        if not missing_tasks:
            logger.info("No missing data found, completion finished")
            return []

        # Create batches of tasks
        batches = [missing_tasks[i : i + batch_size] for i in range(0, len(missing_tasks), batch_size)]
        logger.info(f"Divided into {len(batches)} batches for execution")

        # Execute all batches
        total_count = 0
        success_count = 0

        for i, batch in enumerate(batches, 1):
            # Check API rate limits before executing batch
            server_time, current_weight = await self.fetcher.get_time_and_weight()
            max_minute_weight, _ = self.fetcher.get_api_limits()
            logger.debug(
                f"batch {i}/{len(batches)} with {len(batch)} tasks, "
                f"server_time={server_time}, weight_used={current_weight}"
            )

            # If weight is close to limit, sleep until next minute
            if current_weight > max_minute_weight - 480:
                logger.info(f"Weight {current_weight} almost reaches the maximum limit, sleep until next minute")
                await async_sleep_until_run_time(next_run_time("1m"))
                continue

            # Create async tasks for the batch
            async_tasks = []
            for fetch_func, kwargs_dict, save_path in batch:
                async_tasks.append(self._execute_fetch_task(fetch_func, kwargs_dict, save_path))

            # Execute batch concurrently
            batch_results = await asyncio.gather(*async_tasks, return_exceptions=True)

            # Count results
            for result in batch_results:
                total_count += 1
                if result["success"]:
                    success_count += 1
                else:
                    logger.error(f"Task failed: {result['error']} {result['save_path']}")

        logger.info(f"Data completion finished: {success_count}/{total_count} successful")

        return {"total": total_count, "success": success_count}

    async def _execute_fetch_task(
        self, fetch_func: Callable[..., Awaitable], kwargs_dict: Dict, save_path: Path
    ) -> Dict[str, Any]:
        """Execute a single fetch task

        Args:
            fetch_func: Async function to call
            kwargs_dict: Keyword arguments for the function
            save_path: Path to save the result

        Returns:
            Dict[str, Any]: Task execution result
        """
        try:
            # Call the fetch function
            data = await fetch_func(**kwargs_dict)

            # Check if data is None or not a polars DataFrame
            if data is None:
                return {"success": False, "error": "No data returned from fetcher", "save_path": save_path}

            if not isinstance(data, pl.DataFrame):
                return {"success": False, "error": f"Unsupported data type: {type(data)}", "save_path": save_path}

            # Ensure parent directory exists
            save_path.parent.mkdir(parents=True, exist_ok=True)

            # Save DataFrame (even if empty)
            data.write_parquet(save_path)

            return {"success": True, "save_path": save_path}

        except Exception as e:
            logger.error(f"Failed to execute fetch task for {save_path}: {e}")
            return {"success": False, "error": str(e), "save_path": save_path}
