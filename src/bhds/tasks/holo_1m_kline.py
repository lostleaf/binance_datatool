#!/usr/bin/env python3
"""
Generate Holo 1m K-line Task

Synthesizes holographic 1-minute klines from parsed AWS data with VWAP and funding rates
support. Includes gap detection and automatic file splitting when gaps are detected.
"""
import shutil
from pathlib import Path
from typing import Any, Dict, List

from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.log_kit import divider, logger
from bdt_common.polars_utils import execute_polars_batch
from bhds.tasks.common import create_symbol_filter_from_config
from bhds.aws.path_builder import create_path_builder
from bhds.holo_kline.gap_detector import HoloKlineGapDetector
from bhds.holo_kline.merger import Holo1mKlineMerger
from bhds.holo_kline.splitter import HoloKlineSplitter
from bhds.tasks.common import get_data_directory, load_config


class GenHolo1mKlineTask:
    """Task for synthesizing holographic 1-minute klines from parsed AWS data."""

    def __init__(self, config: str | dict | Path):
        if isinstance(config, str) or isinstance(config, Path):
            self.config = load_config(str(config))
            logger.info(f"Loaded configuration from: {config}")
        else:
            self.config = config

        # Get base data directory
        self.input_dir = get_data_directory(self.config.get("input_dir"), "parsed_data")

        if "trade_type" not in self.config:
            raise KeyError("Missing 'trade_type' in config")
        self.trade_type = TradeType(self.config["trade_type"])
        self.data_freq = DataFrequency.daily

        holo_kline_dir = get_data_directory(self.config.get("output_dir"), "holo_1m_klines")
        self.output_dir = holo_kline_dir / str(self.trade_type.value).replace("/", "_")

        # Create path builders for different data types
        self.kline_path_builder = create_path_builder(
            trade_type=self.trade_type, data_freq=self.data_freq, data_type=DataType.kline, time_interval="1m"
        )

        # Get features configuration
        features = self.config.get("features", {})
        self.include_vwap = features.get("include_vwap", False)
        self.include_funding = features.get("include_funding", False)

        logger.info(f"Trade type: {self.trade_type}")
        logger.info(f"Input directory: {self.input_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Include VWAP: {self.include_vwap}, Include Funding: {self.include_funding}")

        if self.include_funding and self.trade_type == TradeType.spot:
            self.include_funding = False
            logger.warning("Funding data is not available for spot trading, setting include_funding to False")

        self.funding_path_builder = None
        if self.include_funding:
            self.funding_path_builder = create_path_builder(
                trade_type=self.trade_type, data_freq=self.data_freq, data_type=DataType.funding_rate
            )

        # Get gap detection configuration - only enable if explicitly provided
        gap_detection = self.config.get("gap_detection")
        self.enable_gap_detection = gap_detection is not None
        if self.enable_gap_detection:
            self.min_days = gap_detection.get("min_days", 1)
            self.min_price_change = gap_detection.get("min_price_change", 0.1)
            self.detector = HoloKlineGapDetector(self.min_days, self.min_price_change)
            self.splitter = HoloKlineSplitter()
        else:
            self.detector = None
            self.splitter = None

        # Initialize merger
        self.merger = Holo1mKlineMerger(
            trade_type=self.trade_type,
            base_dir=self.input_dir,
            include_vwap=self.include_vwap,
            include_funding=self.include_funding,
        )

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
        """Get available symbols from kline directory using path builder."""
        # Get kline directory using path builder
        kline_dir = self.input_dir / self.kline_path_builder.base_dir

        if not kline_dir.exists():
            logger.warning(f"Kline directory does not exist: {kline_dir}")
            return []

        # Get all symbol directories
        symbol_dirs = [d for d in kline_dir.iterdir() if d.is_dir()]
        symbols = [d.name for d in symbol_dirs]

        return symbols

    def _generate_holo_klines(self, symbols: List[str]) -> Dict[str, bool]:
        """Generate holographic 1-minute klines for specified symbols."""
        logger.debug(f"Generating holo klines for {len(symbols)} symbols...")

        # Generate klines for all symbols
        lazy_frames = self.merger.generate_all(self.output_dir, symbols)
        if not lazy_frames:
            logger.warning("No symbols found to process")
            return {}

        # Execute batch processing
        execute_polars_batch(lazy_frames, "Gen Holo Klines")

        # Track successful generations
        success_map = {}
        for symbol in symbols:
            expected_file = self.output_dir / f"{symbol}.parquet"
            success_map[symbol] = expected_file.exists()

        successful = sum(success_map.values())
        logger.ok(f"Generated {successful}/{len(symbols)} holo kline files")
        return success_map

    def _detect_and_split_gaps(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Detect gaps and split files when gaps are found."""
        if not self.enable_gap_detection:
            logger.info("Gap detection is disabled")
            return {}

        logger.debug("Detecting gaps in generated holo klines...")

        results = {}
        total_symbols = len(symbols)
        symbols_with_gaps = 0
        total_splits = 0

        # Get generated files
        generated_files = [self.output_dir / f"{symbol}.parquet" for symbol in symbols]
        generated_files = [f for f in generated_files if f.exists()]

        if not generated_files:
            logger.warning("No generated files found for gap detection")
            return results

        # Generate gap detection tasks
        gap_tasks = [self.detector.detect(file_path) for file_path in generated_files]
        gap_results = execute_polars_batch(gap_tasks, "Detecting gaps", return_results=True)

        if not gap_results:
            logger.ok("No gap results returned")
            return results

        # Process gap results and split files
        for file_path, gaps_df in zip(generated_files, gap_results):
            symbol = file_path.stem

            if len(gaps_df) > 0:
                symbols_with_gaps += 1

                # Split the file based on gaps
                split_files = self.splitter.split_file(file_path, gaps_df)
                split_count = len(split_files)
                total_splits += split_count

                # Remove original file after splitting
                if split_count > 0:
                    logger.info(f"{symbol} - {len(gaps_df)} gap(s) detected, " f"Split {split_count} files")

                results[symbol] = {
                    "gaps": len(gaps_df),
                    "splits": split_count,
                    "split_files": [str(f) for f in split_files],
                }
            else:
                results[symbol] = {"gaps": 0, "splits": 0, "split_files": []}

        logger.ok(
            f"Gap detection complete: {symbols_with_gaps}/{total_symbols} symbols have gaps, {total_splits} split files generated"
        )
        return results

    def run(self):
        """Execute the complete generate holo 1m kline task."""
        divider(f"BHDS: Start Generate Holo 1m K-line ({self.trade_type})", with_timestamp=True)

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

            # Step 3: Generate holo klines
            if self.output_dir.exists():
                logger.warning(f"Output directory already exists: {self.output_dir}, removing...")
                shutil.rmtree(self.output_dir)
            self.output_dir.mkdir(parents=True)
            success_map = self._generate_holo_klines(target_symbols)

            # Step 4: Detect gaps and split files (only if gap_detection is configured)
            gap_results = self._detect_and_split_gaps(target_symbols)

            # Final summary
            successful_generation = sum(success_map.values())
            total_symbols = len(target_symbols)
            symbols_with_gaps = sum(1 for r in gap_results.values() if r["gaps"] > 0) if gap_results else 0
            total_splits = sum(r["splits"] for r in gap_results.values()) if gap_results else 0

            divider("BHDS: Task Summary", with_timestamp=True)
            logger.ok(f"Holo 1m kline synthesis complete:")
            logger.debug(f"  Total symbols: {total_symbols}")
            logger.debug(f"  Successfully generated: {successful_generation}")
            if self.enable_gap_detection:
                logger.debug(f"  Symbols with gaps: {symbols_with_gaps}")
                logger.debug(f"  Split files created: {total_splits}")
            logger.debug(f"  Output directory: {self.output_dir}")

        except Exception as e:
            logger.exception(f"Error during holo kline synthesis: {e}")
            raise

        divider(f"BHDS: GenerateHolo 1m K-line ({self.trade_type}) Completed", with_timestamp=True)
