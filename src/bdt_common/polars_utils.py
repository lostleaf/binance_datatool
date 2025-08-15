import os
import time

import polars as pl
from tqdm import tqdm

from bdt_common.log_kit import logger


def polars_mp_env():
    """
    Set environment variables for Polars to run in multiprocessing mode.

    Used as ProcessPoolExecutor(initializer=polars_mp_env, ...)
    """
    os.environ["POLARS_MAX_THREADS"] = "1"
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"
    os.environ["NUMBA_NUM_THREADS"] = "1"
    os.environ["VECLIB_MAXIMUM_THREADS"] = "1"


def execute_polars_batch(
    tasks: list[pl.LazyFrame], desc: str, batch_size: int = 32, return_results: bool = False
) -> list[pl.DataFrame] | None:
    """
    Execute a list of Polars LazyFrame tasks in batches with progress tracking.

    Args:
        tasks: List of Polars LazyFrame tasks to execute
        desc: Description for the progress bar
        batch_size: Number of tasks to execute in each batch
        return_results: Whether to return collected results

    Returns:
        List of collected DataFrames if return_results=True, otherwise None
    """
    results = [] if return_results else None

    if not tasks:
        logger.warning(f"No tasks to execute for {desc}")
        return results

    logger.info(f"Executing {len(tasks)} {desc}")
    t_start = time.perf_counter()

    with tqdm(total=len(tasks), desc=desc, unit="task") as pbar:
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            batch_results = pl.collect_all(batch)
            pbar.update(len(batch))
            if return_results:
                results.extend(batch_results)

    time_elapsed = time.perf_counter() - t_start
    time_elapsed = f"{time_elapsed:.2f}s" if time_elapsed < 60 else f"{time_elapsed / 60:.2f}mins"
    logger.ok(f"{desc} completed in {time_elapsed}")

    return results
