"""Tests for BHDS path resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from binance_datatool.common.path import BhdsHomeNotConfiguredError, resolve_bhds_home


def test_resolve_bhds_home_prefers_cli_override(monkeypatch) -> None:
    """CLI overrides should win over the environment."""
    monkeypatch.setenv("BHDS_HOME", "/env/home")

    result = resolve_bhds_home(Path("~/cli-home"))

    assert result == Path("~/cli-home").expanduser()


def test_resolve_bhds_home_falls_back_to_environment(monkeypatch) -> None:
    """The environment should be used when no override is supplied."""
    monkeypatch.setenv("BHDS_HOME", "~/env-home")

    result = resolve_bhds_home()

    assert result == Path("~/env-home").expanduser()


def test_resolve_bhds_home_raises_when_unconfigured(monkeypatch) -> None:
    """A descriptive error should be raised when BHDS_HOME is missing."""
    monkeypatch.delenv("BHDS_HOME", raising=False)

    with pytest.raises(BhdsHomeNotConfiguredError, match="BHDS_HOME not configured"):
        resolve_bhds_home()
