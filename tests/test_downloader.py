"""Tests for the aria2 archive downloader."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from binance_datatool.bhds.archive.downloader import (
    Aria2NotFoundError,
    DownloadRequest,
    download_archive_files,
)


def test_download_archive_files_raises_when_aria2_is_missing(monkeypatch, tmp_path) -> None:
    """A dedicated error should be raised when aria2c is unavailable."""
    monkeypatch.setattr("shutil.which", lambda name: None)

    with pytest.raises(Aria2NotFoundError, match="aria2c executable not found"):
        download_archive_files(
            [
                DownloadRequest(
                    url="https://data.binance.vision/data/a.zip", local_path=tmp_path / "a.zip"
                )
            ],
            inherit_proxy=False,
        )


def test_download_archive_files_strips_proxy_env_by_default(monkeypatch, tmp_path) -> None:
    """Default aria2 runs should remove proxy-related environment variables."""
    commands: list[tuple[list[str], dict[str, str] | None]] = []

    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example")
    monkeypatch.setenv("HTTPS_PROXY", "https://proxy.example")
    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/aria2c")

    def fake_run(cmd, check, env):  # noqa: ANN001
        commands.append((cmd, env))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)

    result = download_archive_files(
        [
            DownloadRequest(
                url="https://data.binance.vision/data/spot/file.zip",
                local_path=tmp_path / "nested" / "file.zip",
            )
        ],
        inherit_proxy=False,
    )

    assert result.succeeded == 1
    assert commands
    cmd, env = commands[0]
    assert "--allow-overwrite=true" in cmd
    assert "--auto-file-renaming=false" in cmd
    assert env is not None
    assert "HTTP_PROXY" not in env
    assert "HTTPS_PROXY" not in env
    assert (tmp_path / "nested").exists()


def test_download_archive_files_can_inherit_proxy_env(monkeypatch, tmp_path) -> None:
    """The proxy flag should allow aria2 to inherit the process environment."""
    seen_envs: list[dict[str, str] | None] = []

    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/aria2c")

    def fake_run(cmd, check, env):  # noqa: ANN001
        del cmd, check
        seen_envs.append(env)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)

    download_archive_files(
        [
            DownloadRequest(
                url="https://data.binance.vision/data/spot/file.zip",
                local_path=tmp_path / "file.zip",
            )
        ],
        inherit_proxy=True,
    )

    assert seen_envs == [None]


def test_download_archive_files_splits_batches_and_retries(monkeypatch, tmp_path) -> None:
    """Failed batches should be retried as whole batches."""
    call_count = 0

    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/aria2c")

    def fake_run(cmd, check, env):  # noqa: ANN001
        nonlocal call_count
        del cmd, check, env
        call_count += 1
        return SimpleNamespace(returncode=1 if call_count == 1 else 0)

    monkeypatch.setattr("subprocess.run", fake_run)

    requests = [
        DownloadRequest(
            url=f"https://data.binance.vision/data/spot/file-{index}.zip",
            local_path=tmp_path / f"file-{index}.zip",
        )
        for index in range(3)
    ]
    result = download_archive_files(
        requests,
        inherit_proxy=False,
        batch_size=2,
        max_tries=3,
    )

    assert call_count == 3
    assert result.succeeded == 3
    assert result.failed_requests == []


def test_download_archive_files_reports_round_progress(monkeypatch, tmp_path) -> None:
    """Each retry round should create one reporter and tick it per processed batch."""
    returncodes = [1, 0, 0]
    captured_reporters: list[dict[str, object]] = []

    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/aria2c")

    def fake_run(cmd, check, env):  # noqa: ANN001
        del cmd, check, env
        return SimpleNamespace(returncode=returncodes.pop(0))

    class FakeReporter:
        def __init__(self, metadata: dict[str, object]) -> None:
            self.metadata = metadata

        def __enter__(self) -> FakeReporter:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def tick(self, event) -> None:  # noqa: ANN001
            self.metadata["events"].append((event.name, event.ok, event.count))

    def fake_make_reporter(progress_bar: bool, *, total: int, description: str) -> FakeReporter:
        metadata: dict[str, object] = {
            "progress_bar": progress_bar,
            "total": total,
            "description": description,
            "events": [],
        }
        captured_reporters.append(metadata)
        return FakeReporter(metadata)

    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr(
        "binance_datatool.bhds.archive.downloader.make_reporter", fake_make_reporter
    )

    requests = [
        DownloadRequest(
            url=f"https://data.binance.vision/data/spot/file-{index}.zip",
            local_path=tmp_path / f"file-{index}.zip",
        )
        for index in range(3)
    ]
    result = download_archive_files(
        requests,
        inherit_proxy=False,
        batch_size=2,
        max_tries=2,
        progress_bar=True,
    )

    assert result.failed_requests == []
    assert captured_reporters == [
        {
            "progress_bar": True,
            "total": 3,
            "description": "download",
            "events": [("batch 1/2", False, 2), ("batch 2/2", True, 1)],
        },
        {
            "progress_bar": True,
            "total": 2,
            "description": "download retry 1",
            "events": [("batch 1/1", True, 2)],
        },
    ]
