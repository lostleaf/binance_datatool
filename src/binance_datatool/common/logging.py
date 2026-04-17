"""CLI logging helpers."""

from __future__ import annotations

import sys

from loguru import logger

_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level}</level> | "
    "<cyan>{module}</cyan> - "
    "{message}"
)


def configure_cli_logging(verbosity: int) -> None:
    """Configure loguru for a CLI invocation.

    Args:
        verbosity: 0 = WARNING, 1 = INFO, 2+ = DEBUG.
    """
    logger.remove()

    if verbosity >= 2:
        level = "DEBUG"
    elif verbosity == 1:
        level = "INFO"
    else:
        level = "WARNING"

    logger.add(sys.stderr, level=level, format=_FORMAT, colorize=sys.stderr.isatty())
