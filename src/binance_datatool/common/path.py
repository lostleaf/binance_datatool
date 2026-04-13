"""Local path helpers for BHDS-managed data."""

from __future__ import annotations

import os
from pathlib import Path

BHDS_HOME_ENV_VAR = "BHDS_HOME"


class BhdsHomeNotConfiguredError(ValueError):
    """Raised when a command needs BHDS_HOME but none was configured."""


def resolve_bhds_home(override: str | Path | None = None) -> Path:
    """Resolve the BHDS home directory from CLI override or environment.

    Args:
        override: Optional CLI-provided override path.

    Returns:
        Expanded filesystem path for the BHDS home directory.

    Raises:
        BhdsHomeNotConfiguredError: If neither ``override`` nor the
            ``BHDS_HOME`` environment variable is set.
    """
    if override is not None:
        return Path(override).expanduser()

    env_value = os.getenv(BHDS_HOME_ENV_VAR)
    if env_value:
        return Path(env_value).expanduser()

    msg = (
        "BHDS_HOME not configured.\n"
        "Set the BHDS_HOME environment variable or use --bhds-home to specify "
        "where data files should be stored."
    )
    raise BhdsHomeNotConfiguredError(msg)
