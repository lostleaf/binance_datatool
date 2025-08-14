"""Data executor

Provides independent executor class for executing detected data completion tasks with full error handling and rate limit management.
"""

import asyncio
from typing import Callable, Dict, List, Tuple
from pathlib import Path

import polars as pl

from bdt_common.log_kit import logger
from bdt_common.rest_api.fetcher import BinanceFetcher
from bdt_common.time import async_sleep_until_run_time, next_run_time


class DataExecutor:
    """Data completion task executor

    Responsible for executing detected data completion tasks with full error handling and rate limit management.
    """

    def __init__(self, fetcher: BinanceFetcher):
        """Initialize with BinanceFetcher instance"""
        self.fetcher = fetcher

    async def execute(self, tasks: List[Tuple[Callable, Dict, Path]], batch_size: int = 40) -> Dict:
        """Execute data completion tasks with full error handling and rate limit management

        Args:
            tasks: List of tasks, each task is (unbound method, parameter dict, save path)
            batch_size: Number of tasks per batch

        Returns:
            Dict: Execution result statistics {"total": total, "success": successful}
        """
        if not tasks:
            logger.info("No tasks to execute")
            return {"total": 0, "success": 0}

        logger.info(f"Starting task execution for {len(tasks)} tasks")

        # Batch processing
        batches = [tasks[i : i + batch_size] for i in range(0, len(tasks), batch_size)]
        total_count = 0
        success_count = 0

        for batch_num, batch in enumerate(batches, 1):
            # Rate limit check (retain original logic)
            try:
                server_time, current_weight = await self.fetcher.get_time_and_weight()
                max_minute_weight, _ = self.fetcher.get_api_limits()

                logger.debug(
                    f"Batch {batch_num}/{len(batches)} with {len(batch)} tasks, "
                    f"server_time={server_time}, weight_used={current_weight}"
                )

                # Sleep to next minute if approaching weight limit
                if current_weight > max_minute_weight - 480:
                    logger.info(
                        f"Weight {current_weight} approaching maximum limit "
                        f"{max_minute_weight}, sleeping until next minute"
                    )
                    await async_sleep_until_run_time(next_run_time("1m"))
                    continue

            except Exception as e:
                logger.warning(f"Failed to check rate limit: {e}")

            # Execute current batch
            async_tasks = [self._execute_single_task(method, kwargs, save_path) for method, kwargs, save_path in batch]

            # Concurrent batch execution
            batch_results = await asyncio.gather(*async_tasks, return_exceptions=True)

            # Process results
            for result in batch_results:
                total_count += 1
                if isinstance(result, dict) and result.get("success"):
                    success_count += 1
                else:
                    # Handle exceptions or failed tasks
                    error_msg = str(result) if not isinstance(result, dict) else result.get("error", "Unknown error")
                    logger.error(f"Task failed: {error_msg}")

        logger.info(f"Data completion finished: {success_count}/{total_count} successful")
        return {"total": total_count, "success": success_count}

    async def _execute_single_task(
        self,
        method: Callable,
        kwargs: Dict,
        save_path: Path,
    ) -> Dict:
        """Execute single data completion task with full error handling logic

        Args:
            method: Unbound method
            kwargs: Method parameters
            save_path: Save path

        Returns:
            Dict: Task execution result
        """
        try:
            # Call unbound method, pass fetcher instance as first parameter
            data = await method(self.fetcher, **kwargs)

            # Check returned data (retain original logic)
            if data is None:
                logger.warning(f"No data returned for {save_path}")
                return {"success": False, "error": "No data returned", "save_path": save_path}

            if not isinstance(data, pl.DataFrame):
                logger.error(f"Unsupported data type: {type(data)} for {save_path}")
                return {"success": False, "error": f"Unsupported data type: {type(data)}", "save_path": save_path}

            # Ensure directory exists
            save_path.parent.mkdir(parents=True, exist_ok=True)

            # Save data (including empty DataFrames, retain original logic)
            data.write_parquet(save_path)

            return {"success": True, "save_path": save_path}

        except Exception as e:
            logger.error(f"Failed to execute task for {save_path}: {e}")
            return {"success": False, "error": str(e), "save_path": save_path}
