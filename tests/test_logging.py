"""Tests for CLI logging configuration."""

from __future__ import annotations

from loguru import logger

from binance_datatool.common.logging import configure_cli_logging


def test_configure_cli_logging_warning_level(capsys) -> None:
    """Default verbosity should suppress info logs and print warnings."""
    configure_cli_logging(0)

    logger.info("hidden info")
    logger.warning("shown warning")

    captured = capsys.readouterr()
    assert "hidden info" not in captured.err
    assert "shown warning" in captured.err


def test_configure_cli_logging_info_level(capsys) -> None:
    """Single verbose flag should enable INFO logs."""
    configure_cli_logging(1)

    logger.info("shown info")

    captured = capsys.readouterr()
    assert "shown info" in captured.err


def test_configure_cli_logging_debug_format(capsys) -> None:
    """Double verbose flags should enable DEBUG output with rich metadata."""
    configure_cli_logging(2)

    logger.debug("debug message")

    captured = capsys.readouterr()
    assert "DEBUG" in captured.err
    assert "debug message" in captured.err
    assert "test_logging" in captured.err
    assert "test_configure_cli_logging_debug_format" not in captured.err
