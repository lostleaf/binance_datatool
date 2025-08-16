#!/usr/bin/env python3
"""
Holo K-line Resample Task

Resamples 1m holographic klines to higher time frames using HoloKlineResampler.
Supports both single offset and multiple offset generation based on configuration.
"""
import shutil
from pathlib import Path
from typing import Any, Dict, List

import polars as pl

from bdt_common.enums import TradeType
from bdt_common.log_kit import divider, logger
from bdt_common.polars_utils import execute_polars_batch
from bhds.holo_kline.resampler import HoloKlineResampler
from bhds.tasks.common import create_symbol_filter_from_config, get_data_directory, load_config


class HoloResampleTask:
    """Task for resampling holographic 1-minute klines to higher time frames."""

    def __init__(self, config: str | dict | Path):
        if isinstance(config, str) or isinstance(config, Path):
            self.config = load_config(str(config))
            logger.info(f"Loaded configuration from: {config}")
        else:
            self.config = config

        # Get trade type
        if "trade_type" not in self.config:
            raise KeyError("Missing 'trade_type' in config")
        self.trade_type = TradeType(self.config["trade_type"])

        # Get resample interval
        if "resample_interval" not in self.config:
            raise KeyError("Missing 'resample_interval' in config")
        self.resample_interval = str(self.config["resample_interval"])

        # Get base offset and determine if we should generate multiple offsets
        self.base_offset = self.config.get("base_offset")
        self.generate_offsets = self.base_offset is not None and self.base_offset != "0m"

        # Initialize resampler
        self.resampler = HoloKlineResampler(self.resample_interval)

        # Get input directory (holo_1m output)
        holo_1m_dir = get_data_directory(self.config.get("input_dir"), "holo_1m_klines")
        self.input_dir = holo_1m_dir / str(self.trade_type.value).replace("/", "_")

        # Get output directory
        resample_base_dir = get_data_directory(self.config.get("output_dir"), "resampled_klines")
        trade_type_str = str(self.trade_type.value).replace("/", "_")
        self.output_base_dir = resample_base_dir / trade_type_str / self.resample_interval

        logger.info(
            f"TradeType: {self.trade_type}, ResampleInterval: {self.resample_interval}, "
            f"Offsets: {self.generate_offsets}, Base offset: {self.base_offset}"
        )
        logger.info(f"Input directory: {self.input_dir}")
        logger.info(f"Output base directory: {self.output_base_dir}")

    def _apply_symbol_filter(self, all_symbols: List[str]) -> List[str]:
        """Apply symbol filtering based on config."""
        symbols = self.config.get("symbols")
        if symbols:
            valid = sorted(set(symbols).intersection(set(all_symbols)))
            logger.info(f"Using {len(valid)} user-specified symbols")
            return valid

        # Fallback to filter logic
        filter_cfg = self.config.get("symbol_filter")
        if filter_cfg is None or not filter_cfg:
            logger.info("No symbol filtering applied, using all symbols")
            return all_symbols

        symbol_filter = create_symbol_filter_from_config(self.trade_type, filter_cfg)
        filtered_symbols = symbol_filter(all_symbols)
        logger.info(f"Found {len(all_symbols)} total symbols, filtered to {len(filtered_symbols)} symbols")
        return filtered_symbols

    def _get_available_symbols(self) -> List[str]:
        """Get available symbols from input directory."""
        if not self.input_dir.exists():
            logger.warning(f"Input directory does not exist: {self.input_dir}")
            return []

        # Get all parquet files
        parquet_files = list(self.input_dir.glob("*.parquet"))
        symbols = [f.stem for f in parquet_files]

        return symbols

    def _resample_symbol(self, symbol: str) -> Dict[str, pl.LazyFrame]:
        """Resample a single symbol to target interval(s)."""
        input_file = self.input_dir / f"{symbol}.parquet"
        if not input_file.exists():
            logger.warning(f"Input file not found: {input_file}")
            return {}

        # Load the 1m holo kline data
        ldf = pl.scan_parquet(input_file)

        # Get schema for performance
        schema = ldf.collect_schema()

        if self.generate_offsets:
            # Generate multiple offset files
            return self.resampler.resample_offsets(ldf, self.base_offset, schema)
        else:
            # Generate single file with 0m offset
            return {"0m": self.resampler.resample(ldf, "0m", schema)}

    def _prepare_write_tasks(self, symbol: str, resampled_data: Dict[str, pl.LazyFrame]) -> List[pl.LazyFrame]:
        """Prepare lazy write tasks for resampled data."""
        write_tasks = []

        for offset, ldf in resampled_data.items():
            # Create output directory for this offset
            output_dir = self.output_base_dir / offset
            output_dir.mkdir(parents=True, exist_ok=True)

            # Define output file path
            output_file = output_dir / f"{symbol}.parquet"

            # Create lazy write task
            write_task = ldf.sink_parquet(output_file, lazy=True)
            write_tasks.append(write_task)

        return write_tasks

    def _resample_all_symbols(self, symbols: List[str]) -> Dict[str, bool]:
        """Resample all symbols and save results."""
        logger.debug(f"Resampling {len(symbols)} symbols to {self.resample_interval}...")

        all_write_tasks = []
        symbol_file_map = {}  # Track which files belong to which symbol

        # Prepare all write tasks
        for symbol in symbols:
            # Resample this symbol
            resampled_data = self._resample_symbol(symbol)
            if not resampled_data:
                logger.warning(f"No resampled data for symbol: {symbol}")
                continue

            # Prepare write tasks for this symbol
            write_tasks = self._prepare_write_tasks(symbol, resampled_data)
            all_write_tasks.extend(write_tasks)
            
            # Track file mapping
            for offset in resampled_data.keys():
                symbol_file_map[f"{offset}/{symbol}"] = True

        # Execute all write tasks in batch
        if all_write_tasks:
            execute_polars_batch(all_write_tasks, "Writing resampled files")
        else:
            logger.warning("No write tasks to execute")

        return symbol_file_map

    def run(self):
        """Execute the complete resample task."""
        divider(f"BHDS: Start Holo Resample ({self.trade_type}, {self.resample_interval})", with_timestamp=True)

        try:
            # Step 1: Get available symbols
            all_symbols = self._get_available_symbols()
            if not all_symbols:
                logger.warning("No symbols found in input directory")
                return

            # Step 2: Apply symbol filtering
            target_symbols = self._apply_symbol_filter(all_symbols)
            if not target_symbols:
                logger.warning("No symbols to process after filtering")
                return

            # Step 3: Clean output directory if it exists
            if self.output_base_dir.exists():
                logger.warning(f"Output directory already exists: {self.output_base_dir}, removing...")
                shutil.rmtree(self.output_base_dir)
            self.output_base_dir.mkdir(parents=True)

            # Step 4: Resample all symbols
            success_map = self._resample_all_symbols(target_symbols)

            # Final summary
            total_symbols = len(target_symbols)
            total_files = len(success_map)
            successful_files = sum(success_map.values())

            divider("BHDS: Task Summary", with_timestamp=True)
            logger.ok(f"Holo resample complete:")
            logger.debug(f"  Total symbols: {total_symbols}")
            logger.debug(f"  Total files: {total_files}")
            logger.debug(f"  Successfully resampled: {successful_files}")
            logger.debug(f"  Resample interval: {self.resample_interval}")
            if self.generate_offsets:
                logger.debug(f"  Base offset: {self.base_offset}")
            logger.debug(f"  Output directory: {self.output_base_dir}")

        except Exception as e:
            logger.exception(f"Error during holo resample: {e}")
            raise

        divider(f"BHDS: Holo Resample ({self.trade_type}, {self.resample_interval}) Completed", with_timestamp=True)
