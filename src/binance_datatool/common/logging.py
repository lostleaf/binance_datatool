"""CLI logging helpers."""

from __future__ import annotations

import sys

from loguru import logger

_FORMAT_MESSAGE = "<level>{level}</level>: {message}"
_FORMAT_DEBUG = (
    "<green>{time:HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> - {message}"
)


def configure_cli_logging(verbosity: int) -> None:
    """Configure loguru for a CLI invocation.

    Args:
        verbosity: 0 = WARNING, 1 = INFO, 2+ = DEBUG.
    """
    logger.remove()

    if verbosity >= 2:
        level, fmt = "DEBUG", _FORMAT_DEBUG
    elif verbosity == 1:
        level, fmt = "INFO", _FORMAT_MESSAGE
    else:
        level, fmt = "WARNING", _FORMAT_MESSAGE

    logger.add(sys.stderr, level=level, format=fmt, colorize=sys.stderr.isatty())
