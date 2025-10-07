#!/usr/bin/env python3
"""
Test script for DailyKlineDetector and DataExecutor

Test symbols: LUNAUSDT, FTTUSDT
Test data: 1m spot klines in daily frequency
Tests both detection of missing data and execution of completion tasks
"""

import asyncio
import os
import random
import tempfile
from pathlib import Path

import polars as pl
from test_utils import output_directory_structure

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import divider, logger
from bdt_common.network import create_aiohttp_session
from bdt_common.rest_api.fetcher import BinanceFetcher
from bhds.api.completion import CompletionOperation, CompletionTask, DailyKlineDetector, DataExecutor
from bhds.aws.csv_conv import AwsCsvToParquetConverter
from bhds.aws.local import LocalAwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder
from bhds.tasks.common import get_bhds_home


def prepare_kline_data(temp_dir: Path, symbols: list[str], interval: str = "1m"):
    """Prepare kline data by converting CSV to Parquet"""
    divider("Preparing kline data...", sep="-")

    # AWS data directory
    aws_data_dir = get_bhds_home(None) / "aws_data"

    # Create path builder for spot 1m klines
    path_builder = AwsKlinePathBuilder(trade_type=TradeType.spot, data_freq=DataFrequency.daily, time_interval=interval)

    # Create local AWS client
    local_aws_client = LocalAwsClient(base_dir=aws_data_dir, path_builder=path_builder)

    # Create CSV to Parquet converter
    processor = AwsCsvToParquetConverter(
        local_aws_client=local_aws_client,
        data_type=DataType.kline,
        output_base_dir=temp_dir,
        force_update=True,
        verbose=False,
    )

    # Process symbols
    results = processor.process_symbols(symbols)

    # Print results summary
    for symbol, result in results.items():
        logger.debug(
            f"  {symbol}: {result['processed_files']} processed, "
            f"{result['skipped_files']} skipped, {result['failed_files']} failed"
        )

    return temp_dir


def test_detection_only(data_dir: Path, symbols: list[str], interval: str = "1m"):
    """Test DailyKlineDetector.detect method (offline)"""
    # Create detector instance (no fetcher needed!)
    detector = DailyKlineDetector(
        trade_type=TradeType.spot,
        interval=interval,
        base_dir=data_dir,
    )

    # Test detect method
    tasks = detector.detect(symbols)
    random.shuffle(tasks)

    logger.info(f"Found {len(tasks)} missing data tasks for {symbols}:")
    for i, task in enumerate(tasks[:3], 1):
        logger.debug(f"  Task {i}:")
        logger.debug(f"    Operation: {task.operation.value}")
        logger.debug(f"    Arguments: {dict(task.params)}")
        logger.debug(f"    Output path: {task.save_path}")

        # Convert to Path object if needed
        output_path = Path(task.save_path)
        logger.debug(f"    Output path exists: {output_path.exists()}")

        # Check if parent directory exists
        parent_dir = output_path.parent
        logger.debug(f"    Parent directory: {parent_dir}")
        logger.debug(f"    Parent directory exists: {parent_dir.exists()}")

        if parent_dir.exists():
            # List files in parent directory
            files = list(parent_dir.glob("*.parquet"))
            logger.debug(f"    Existing parquet files in parent: {len(files)}")
            for file in files[:3]:  # Show first 3 files
                logger.debug(f"      - {file.name}")
            if len(files) > 3:
                logger.debug(f"      ... and {len(files) - 3} more files")

    return tasks


async def test_data_executor(data_dir: Path, symbols: list[str], interval: str = "1m"):
    """Test DataExecutor with real API calls"""
    # Create detector and detect missing tasks
    detector = DailyKlineDetector(
        trade_type=TradeType.spot,
        interval=interval,
        base_dir=data_dir,
    )

    tasks = detector.detect(symbols)

    if not tasks:
        logger.info("No missing data found, creating artificial task for testing")
        # Create a test task
        test_date = "2023-01-01"
        symbol_dir = Path(data_dir) / "parsed_data" / "klines" / "spot" / "1m" / symbols[0]
        symbol_dir.mkdir(parents=True, exist_ok=True)
        save_path = symbol_dir / f"{symbols[0]}-{interval}-{test_date}.parquet"
        tasks = [
            CompletionTask(
                operation=CompletionOperation.GET_KLINE_DF_OF_DAY,
                params={"symbol": symbols[0], "interval": interval, "dt": test_date},
                save_path=save_path,
            )
        ]

    # Step 3: Create executor and execute tasks
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type=TradeType.spot, session=session, http_proxy=http_proxy)
        executor = DataExecutor(fetcher)

        logger.info("Executing %d tasks with http_proxy: %s", len(tasks), http_proxy)
        result = await executor.execute(tasks)

        # Check created files
        random.shuffle(tasks)
        for task in tasks[:10]:
            save_path = task.save_path
            if save_path.exists():
                try:
                    df = pl.read_parquet(save_path)
                    logger.debug(f"{save_path.name} created, Shape: {df.shape}, Columns: {len(df.columns)}")
                    if len(df) > 0:
                        logger.debug(f"Date range: {df['candle_begin_time'].min()} ~ {df['candle_begin_time'].max()}")
                except Exception as e:
                    logger.error(f"Error reading file: {e}")
            else:
                logger.error(f"{save_path.name} not found")

        return result


async def test_full_workflow(data_dir: Path, symbols: list[str], interval: str = "1m"):
    """Test complete workflow: detection + execution"""
    # Step 1: Detection
    logger.info("Step 1: Detecting missing data...")
    tasks = test_detection_only(data_dir, symbols, interval)

    if not tasks:
        logger.info("No missing data detected, workflow complete")
        return {"detected": 0, "executed": 0, "successful": 0}

    # Step 2: Execution
    logger.info("Step 2: Executing completion tasks...")
    result = await test_data_executor(data_dir, symbols, interval)

    return {"detected": len(tasks), "executed": result["total"], "successful": result["success"]}


async def test_api_kline_detector():
    """Main test function for API kline detector and executor modules"""
    # Test configuration
    symbols = ["LUNAUSDT", "FTTUSDT"]
    interval = "1m"

    divider("Testing API Kline Detector and Executor Modules", sep="=")
    logger.info(f"Test symbols: {symbols}, interval: {interval}, trade type: {TradeType.spot.value}")

    # Create temporary directory for test data
    with tempfile.TemporaryDirectory(prefix="api_kline_detector_test_") as temp_dir:
        temp_path = Path(temp_dir)
        logger.info(f"Temp directory: {temp_path}, will be cleaned up automatically after test")

        try:
            # Step 1: Prepare kline data
            data_dir = prepare_kline_data(temp_path, symbols, interval)

            # Print directory structure
            divider("Data Directory Structure", sep="-")
            output_directory_structure(data_dir, max_depth=10)

            # Step 2: Test detection only (offline)
            divider("TEST 1: Detection Only (Offline)", sep="-")
            tasks = test_detection_only(data_dir, symbols, interval)
            logger.ok(f"Detection test completed - {len(tasks)} tasks found")

            # Step 3: Test execution only
            divider("TEST 2: Execution Only (Online)", sep="-")
            exec_result = await test_data_executor(data_dir, symbols, interval)
            logger.ok(f"Execution test completed - {exec_result['success']}/{exec_result['total']} successful")

            # Step 4: Test full workflow
            divider("TEST 3: Full Workflow (Detection + Execution)", sep="-")
            workflow_result = await test_full_workflow(data_dir, symbols, interval)
            logger.ok(
                f"Full workflow completed - "
                f"detected: {workflow_result['detected']}, "
                f"executed: {workflow_result['executed']}, "
                f"successful: {workflow_result['successful']}"
            )

        except Exception as e:
            logger.exception(f"Error during testing: {e}")

        logger.info(f"Temp directory {temp_path} will be cleaned up automatically")

    divider("API Kline Detector and Executor Test completed", sep="=")


if __name__ == "__main__":
    asyncio.run(test_api_kline_detector())
