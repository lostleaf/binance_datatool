#!/usr/bin/env python3
"""
Common utilities for BHDS tasks.

Provides shared functionality for configuration loading and directory management
across different task modules.
"""
import os
from pathlib import Path
from typing import Optional

import yaml


def load_config(config_path: str) -> dict:
    """Load YAML configuration from file path."""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    with open(config_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_data_directory(data_dir_cfg: Optional[str], default_subdir: str) -> Path:
    """Get data directory path from config or use default location.
    
    Args:
        data_dir_cfg: Optional directory path from config
        default_subdir: Default subdirectory name under binance_data
        
    Returns:
        Path object for the data directory
    """
    if data_dir_cfg is None:
        default_base = os.path.join(os.path.expanduser("~"), "crypto_data")
        base_dir = Path(os.getenv("CRYPTO_BASE_DIR", default_base))
        data_dir = base_dir / "binance_data" / default_subdir
    else:
        data_dir = Path(data_dir_cfg)
    # Ensure base data directory exists
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir