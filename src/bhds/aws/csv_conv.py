#!/usr/bin/env python3
"""
AWS CSV to Parquet Processor

This module provides a unified processor for converting Binance AWS CSV data files
to Parquet format while maintaining the original directory structure.
"""

import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

from tqdm import tqdm

from bdt_common.enums import DataType
from bdt_common.env import polars_mp_env
from bdt_common.log_kit import logger
from bhds.aws.local import LocalAwsClient
from bhds.aws.parser import create_aws_parser


class AwsCsvToParquetConverter:
    """AWS CSV to Parquet format processor

    This class is responsible for converting verified AWS CSV zip files to Parquet format,
    maintaining the original directory structure to avoid conflicts between different
    trade_type and data_freq data.
    """

    def __init__(
        self,
        local_aws_client: LocalAwsClient,
        data_type: DataType,
        output_base_dir: Path,
        force_update: bool = False,
        max_workers: Optional[int] = None,
        verbose: bool = False,
    ):
        """
        Initialize the processor

        Args:
            local_aws_client: Local AWS client for getting verified files
            data_type: Data type (e.g., kline, funding_rate)
            output_base_dir: Output base directory
            force_update: Whether to force update existing parquet files
            max_workers: Maximum number of concurrent worker processes
            verbose: Whether to print logs below warning level
        """
        self.local_aws_client = local_aws_client
        self.data_type = data_type
        self.output_base_dir = Path(output_base_dir)
        self.force_update = force_update
        self.max_workers = max_workers if max_workers is not None else max(1, os.cpu_count() - 2)
        self.verbose = verbose

        # Create corresponding CSV parser
        self.parser = create_aws_parser(data_type)

        # Ensure output directory exists
        self.output_base_dir.mkdir(parents=True, exist_ok=True)

        if self.verbose:
            logger.info(
                f"Initialized AWS CSV processor: data_type={data_type.value}, "
                f"output_dir={output_base_dir}, force_update={force_update}"
            )

    def get_output_path(self, zip_file: Path) -> Path:
        """
        Generate corresponding parquet output path based on input zip file path

        Maintains the original relative path structure, only replacing .zip extension with .parquet

        Args:
            zip_file: Input zip file path

        Returns:
            Corresponding parquet file output path
        """
        # Get relative path from aws data base directory
        relative_path = zip_file.relative_to(self.local_aws_client.base_dir)

        # Replace .zip extension with .parquet
        parquet_relative_path = relative_path.with_suffix(".parquet")

        # Build complete output path
        return self.output_base_dir / parquet_relative_path

    def should_skip_file(self, zip_file: Path, output_file: Path) -> bool:
        """
        Determine whether to skip processing a file

        Args:
            zip_file: Input zip file
            output_file: Corresponding output parquet file

        Returns:
            True means should skip, False means needs processing
        """
        if self.force_update:
            return False

        if not output_file.exists():
            return False

        # Check if output file is newer than input file
        return output_file.stat().st_mtime >= zip_file.stat().st_mtime

    def process_single_file_with_symbol(self, file_info: tuple) -> Dict[str, any]:
        """
        Process a single file with symbol information

        Args:
            file_info: Tuple of (zip_file_path, symbol)

        Returns:
            Processing result dictionary
        """
        zip_file, symbol = file_info
        output_file = self.get_output_path(zip_file)

        result = {
            "symbol": symbol,
            "zip_file": str(zip_file),
            "output_file": str(output_file),
            "status": "unknown",
            "error": None,
        }

        try:
            # Check if should skip
            if self.should_skip_file(zip_file, output_file):
                result["status"] = "skipped"
                return result

            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Use parser to read CSV data
            df = self.parser.read_csv_from_zip(zip_file)

            # Save as parquet format
            df.write_parquet(output_file)

            result["status"] = "processed"

        except Exception as e:
            result["status"] = "failed"
            result["error"] = f"Exception: {str(e)}"
            logger.error(f"Failed to process file {zip_file}: {str(e)}")

        return result

    def process_symbols(self, symbols: List[str]) -> Dict[str, Dict[str, any]]:
        """
        Process data for multiple symbols concurrently by processing individual files in parallel

        Args:
            symbols: List of symbols to process

        Returns:
            Dictionary of processing results for each symbol
        """
        if not symbols:
            logger.warning("No symbols to process")
            return {}

        # Collect all verified files from all symbols
        all_file_tasks = []
        symbol_file_counts = {}

        for symbol in symbols:
            file_status = self.local_aws_client.get_symbol_file_status(symbol)
            verified_files = file_status["verified"]
            symbol_file_counts[symbol] = len(verified_files)

            if not verified_files:
                logger.warning(f"Symbol {symbol} has no verified files")
                continue

            # Create file tasks with symbol information
            for zip_file in verified_files:
                all_file_tasks.append((zip_file, symbol))

        if not all_file_tasks:
            logger.warning("No verified files found for any symbol")
            return {
                symbol: {
                    "symbol": symbol,
                    "total_files": 0,
                    "processed_files": 0,
                    "failed_files": 0,
                    "skipped_files": 0,
                    "errors": [],
                }
                for symbol in symbols
            }

        if self.verbose:
            logger.info(f"Start process {len(all_file_tasks)} files from {len(symbols)} symbols")

        # Process files in parallel
        file_results = []
        with ProcessPoolExecutor(
            max_workers=self.max_workers, mp_context=mp.get_context("spawn"), initializer=polars_mp_env
        ) as executor:
            # Submit all file tasks
            future_to_task = {
                executor.submit(self.process_single_file_with_symbol, task): task for task in all_file_tasks
            }

            # Collect results with progress bar
            with tqdm(total=len(all_file_tasks), desc="Processing files", unit="file") as pbar:
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    zip_file, symbol = task
                    try:
                        result = future.result()
                        file_results.append(result)
                        pbar.set_postfix(symbol=symbol, file=zip_file.name, status=result["status"])
                    except Exception as e:
                        logger.error(
                            f"Exception occurred while processing file {zip_file} for symbol {symbol}: {str(e)}"
                        )
                        error_result = {
                            "symbol": symbol,
                            "zip_file": str(zip_file),
                            "output_file": str(self.get_output_path(zip_file)),
                            "status": "failed",
                            "error": f"Process exception: {str(e)}",
                        }
                        file_results.append(error_result)
                        pbar.set_postfix(symbol=symbol, file=zip_file.name, status="failed")
                    finally:
                        pbar.update(1)

        # Aggregate results by symbol
        results = {}
        for symbol in symbols:
            symbol_results = [r for r in file_results if r["symbol"] == symbol]

            result = {
                "symbol": symbol,
                "total_files": symbol_file_counts.get(symbol, 0),
                "processed_files": len([r for r in symbol_results if r["status"] == "processed"]),
                "failed_files": len([r for r in symbol_results if r["status"] == "failed"]),
                "skipped_files": len([r for r in symbol_results if r["status"] == "skipped"]),
                "errors": [r["error"] for r in symbol_results if r["error"] is not None],
            }
            results[symbol] = result

        # Output overall statistics
        self._log_summary(results)
        return results

    def _log_summary(self, results: Dict[str, Dict[str, any]]) -> None:
        """
        Output overall statistics of processing results

        Args:
            results: Processing results for all symbols
        """
        total_files = sum(r["total_files"] for r in results.values())
        total_processed = sum(r["processed_files"] for r in results.values())
        total_failed = sum(r["failed_files"] for r in results.values())
        total_skipped = sum(r["skipped_files"] for r in results.values())

        if self.verbose:
            logger.info(
                f"Processing completed statistics: "
                f"total_files={total_files}, "
                f"processed={total_processed}, "
                f"failed={total_failed}, "
                f"skipped={total_skipped}"
            )

        # Output detailed information about failures
        failed_symbols = [symbol for symbol, result in results.items() if result["failed_files"] > 0]
        if failed_symbols:
            logger.warning(f"{len(failed_symbols)} symbols encountered errors during processing: {failed_symbols}")
