"""Aria2-backed archive download helpers."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from binance_datatool.common.progress import ProgressEvent, make_reporter

if TYPE_CHECKING:
    from collections.abc import Sequence

PROXY_ENV_VARS = (
    "http_proxy",
    "https_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "all_proxy",
    "ALL_PROXY",
    "no_proxy",
    "NO_PROXY",
)


class Aria2NotFoundError(FileNotFoundError):
    """Raised when ``aria2c`` is not available in ``PATH``."""


@dataclass(slots=True, frozen=True)
class DownloadRequest:
    """Single direct-download request for aria2."""

    url: str
    local_path: Path


@dataclass(slots=True)
class Aria2DownloadResult:
    """Aggregated result for one aria2 download run."""

    requested: int
    failed_requests: list[DownloadRequest]

    @property
    def succeeded(self) -> int:
        """Return the number of successful requests."""
        return self.requested - len(self.failed_requests)


def _find_aria2c() -> str:
    """Return the ``aria2c`` executable path or raise a dedicated error."""
    executable = shutil.which("aria2c")
    if executable is None:
        raise Aria2NotFoundError("aria2c executable not found in PATH.")
    return executable


def _chunk_requests(
    requests: Sequence[DownloadRequest],
    batch_size: int,
) -> list[list[DownloadRequest]]:
    """Split requests into fixed-size batches."""
    return [list(requests[i : i + batch_size]) for i in range(0, len(requests), batch_size)]


def _build_env(*, inherit_proxy: bool) -> dict[str, str] | None:
    """Build the subprocess environment for aria2."""
    if inherit_proxy:
        return None

    env = os.environ.copy()
    for variable in PROXY_ENV_VARS:
        env.pop(variable, None)
    return env


def _write_input_file(requests: Sequence[DownloadRequest]) -> Path:
    """Create an aria2 input file for one batch."""
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        prefix="bhds_aria2_",
    ) as input_file:
        for request in requests:
            request.local_path.parent.mkdir(parents=True, exist_ok=True)
            input_file.write(f"{request.url}\n")
            input_file.write(f"  dir={request.local_path.parent}\n")
        return Path(input_file.name)


def _run_batch(
    executable: str,
    requests: Sequence[DownloadRequest],
    *,
    inherit_proxy: bool,
) -> int:
    """Run aria2 for a single batch and return its exit code."""
    input_file_path = _write_input_file(requests)
    try:
        cmd = [
            executable,
            "-i",
            str(input_file_path),
            "-j32",
            "-x4",
            "-q",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
        ]
        completed = subprocess.run(
            cmd,
            check=False,
            env=_build_env(inherit_proxy=inherit_proxy),
        )
    finally:
        input_file_path.unlink(missing_ok=True)

    return completed.returncode


def download_archive_files(
    requests: Sequence[DownloadRequest],
    *,
    inherit_proxy: bool,
    batch_size: int = 4096,
    max_tries: int = 3,
    progress_bar: bool = False,
) -> Aria2DownloadResult:
    """Download requests with aria2 using batch retry semantics.

    Args:
        requests: Direct download requests to execute.
        inherit_proxy: Whether aria2 should inherit proxy-related env vars.
        batch_size: Number of files per aria2 batch.
        max_tries: Maximum retry rounds for failed batches.
        progress_bar: Whether to render an interactive tqdm progress bar.

    Returns:
        Aggregated aria2 result.
    """
    if not requests:
        return Aria2DownloadResult(requested=0, failed_requests=[])

    executable = _find_aria2c()
    pending_batches = _chunk_requests(list(requests), batch_size=batch_size)

    for attempt in range(1, max_tries + 1):
        total_this_round = sum(len(batch) for batch in pending_batches)
        description = "download" if attempt == 1 else f"download retry {attempt - 1}"
        logger.info(
            "{}: {} file(s) in {} batch(es)",
            description,
            total_this_round,
            len(pending_batches),
        )
        failed_batches: list[list[DownloadRequest]] = []

        with make_reporter(
            progress_bar,
            total=total_this_round,
            description=description,
        ) as reporter:
            for batch_index, batch in enumerate(pending_batches, start=1):
                returncode = _run_batch(executable, batch, inherit_proxy=inherit_proxy)
                ok = returncode == 0
                reporter.tick(
                    ProgressEvent(
                        name=f"batch {batch_index}/{len(pending_batches)}",
                        ok=ok,
                        count=len(batch),
                    )
                )
                if not ok:
                    failed_batches.append(batch)

        if not failed_batches:
            return Aria2DownloadResult(requested=len(requests), failed_requests=[])

        pending_batches = failed_batches

    return Aria2DownloadResult(
        requested=len(requests),
        failed_requests=[request for batch in pending_batches for request in batch],
    )
