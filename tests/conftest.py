"""Shared test fixtures and pytest configuration."""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register local pytest options."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run tests that make real network requests to data.binance.vision.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip integration tests unless explicitly requested."""
    if config.getoption("--run-integration"):
        return

    skip_integration = pytest.mark.skip(
        reason="use --run-integration to run network integration tests"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
