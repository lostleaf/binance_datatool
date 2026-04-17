"""Tests for shared progress reporters."""

from __future__ import annotations

from binance_datatool.common.progress import (
    LogReporter,
    ProgressEvent,
    TqdmReporter,
    make_reporter,
)


def test_log_reporter_emits_every_tick_when_step_is_one(monkeypatch) -> None:
    """Small totals should emit one sampled line per tick."""
    reporter = LogReporter(total=3, description="Verify")
    lines: list[str] = []
    monkeypatch.setattr(reporter, "_emit", lines.append)

    with reporter:
        reporter.tick(ProgressEvent(name="a.zip", ok=True))
        reporter.tick(ProgressEvent(name="b.zip", ok=False))
        reporter.tick(ProgressEvent(name="c.zip", ok=True))

    assert reporter.done == 3
    assert reporter.ok == 2
    assert reporter.fail == 1
    assert lines == [
        "[Verify] 33% (1/3) ok=1 fail=0 last=a.zip",
        "[Verify] 67% (2/3) ok=1 fail=1 last=b.zip",
        "[Verify] 100% (3/3) ok=2 fail=1 last=c.zip",
        "[Verify] complete: 3/3 ok=2 fail=1",
    ]


def test_log_reporter_emits_on_threshold_crossings(monkeypatch) -> None:
    """Larger totals should emit only when progress crosses the next threshold."""
    reporter = LogReporter(total=100, description="download")
    lines: list[str] = []
    monkeypatch.setattr(reporter, "_emit", lines.append)

    with reporter:
        reporter.tick(ProgressEvent(name="batch 1/20", ok=True, count=4))
        reporter.tick(ProgressEvent(name="batch 2/20", ok=True, count=1))
        reporter.tick(ProgressEvent(name="batch 3/20", ok=False, count=10))

    assert reporter.done == 15
    assert reporter.ok == 5
    assert reporter.fail == 10
    assert lines == [
        "[download] 5% (5/100) ok=5 fail=0 last=batch 2/20",
        "[download] 15% (15/100) ok=5 fail=10 last=batch 3/20",
        "[download] complete: 15/100 ok=5 fail=10",
    ]


def test_log_reporter_emits_aborted_summary(monkeypatch) -> None:
    """Exceptional exits should emit an aborted summary."""
    reporter = LogReporter(total=1, description="Verify")
    lines: list[str] = []
    monkeypatch.setattr(reporter, "_emit", lines.append)

    try:
        with reporter:
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    assert lines == ["[Verify] aborted: 0/1 ok=0 fail=0"]


def test_make_reporter_returns_requested_type() -> None:
    """Reporter factory should choose between log and tqdm implementations."""
    assert isinstance(make_reporter(False, total=1, description="x"), LogReporter)
    assert isinstance(make_reporter(True, total=1, description="x"), TqdmReporter)


def test_tqdm_reporter_rejects_nested_instances() -> None:
    """Nested tqdm reporters in one process should fail fast."""
    outer = TqdmReporter(total=1, description="outer")
    inner = TqdmReporter(total=1, description="inner")

    with outer:
        try:
            with inner:
                raise AssertionError("unreachable")
        except RuntimeError as exc:
            assert "does not support nested" in str(exc)
