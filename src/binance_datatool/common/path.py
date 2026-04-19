"""Local path helpers for archive-managed data."""

from __future__ import annotations

import os
from pathlib import Path

ARCHIVE_HOME_ENV_VAR = "BINANCE_DATATOOL_ARCHIVE_HOME"


class ArchiveHomeNotConfiguredError(ValueError):
    """Raised when a command needs an archive home but none was configured."""


def resolve_archive_home(override: str | Path | None = None) -> Path:
    """Resolve the local archive home from CLI override or environment.

    Args:
        override: Optional CLI-provided override path.

    Returns:
        Expanded filesystem path for the local archive home directory.

    Raises:
        ArchiveHomeNotConfiguredError: If neither ``override`` nor the
            ``BINANCE_DATATOOL_ARCHIVE_HOME`` environment variable is set.
    """
    if override is not None:
        return Path(override).expanduser()

    env_value = os.getenv(ARCHIVE_HOME_ENV_VAR)
    if env_value:
        return Path(env_value).expanduser()

    msg = (
        "BINANCE_DATATOOL_ARCHIVE_HOME not configured.\n"
        "Set the BINANCE_DATATOOL_ARCHIVE_HOME environment variable or use --archive-home to specify "
        "where data files should be stored."
    )
    raise ArchiveHomeNotConfiguredError(msg)
