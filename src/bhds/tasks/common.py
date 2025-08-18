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

from bdt_common.enums import ContractType, TradeType
from bdt_common.symbol_filter import BaseSymbolFilter, create_symbol_filter


def load_config(config_path: str | Path) -> dict:
    """Load YAML configuration from file path."""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    with open(config_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_bhds_home(bhds_home_cfg: Optional[str] = None) -> Path:
    """Get BHDS home directory path from config or environment.

    Args:
        bhds_home_cfg: Optional BHDS home directory path from config

    Returns:
        Path object for the BHDS home directory
    """
    if bhds_home_cfg is not None:
        bhds_home = Path(bhds_home_cfg)
    else:
        default_home = Path.home() / "crypto_data" / "bhds"
        bhds_home = Path(os.getenv("BHDS_HOME", default_home))

    # Ensure BHDS home directory exists
    bhds_home.mkdir(parents=True, exist_ok=True)
    return bhds_home


def create_symbol_filter_from_config(trade_type: TradeType, filter_config: dict) -> BaseSymbolFilter:
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
        leverage_tokens=leverage_tokens,
    )
