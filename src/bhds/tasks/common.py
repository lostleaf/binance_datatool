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

from bdt_common.enums import ContractType
from bdt_common.symbol_filter import create_symbol_filter


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


def create_symbol_filter_from_config(trade_type, filter_config: dict):
    """
    Create symbol filter based on trade type and filter configuration.

    Args:
        trade_type: TradeType enum value (spot, futures/um, futures/cm)
        filter_config: Dictionary containing filter configuration

    Returns:
        Appropriate symbol filter instance

    Raises:
        ValueError: If trade type is not supported for filtering
    """
    quote = filter_config.get("quote")
    contract_type = filter_config.get("contract_type")
    contract_type = ContractType(contract_type) if contract_type else None
    stable_pairs = filter_config.get("stable_pairs", True)
    leverage_tokens = filter_config.get("leverage_tokens", False)
    
    return create_symbol_filter(
        trade_type=trade_type,
        quote=quote,
        contract_type=contract_type,
        stable_pairs=stable_pairs,
        leverage_tokens=leverage_tokens
    )