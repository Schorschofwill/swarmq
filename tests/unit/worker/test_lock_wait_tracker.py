"""Gauges der Lock-Wartezeit je Worker (Plan 0.12.0, MR 13, BZ-05, R11):
wie viele Nachrichten dieser Worker zuletzt wegen eines Lock-Konflikts
zurueckgestellt hat und wie lange die aelteste schon wartet.

Test-Liste:
- [x] ein Konflikt zaehlt; derselbe Task zaehlt einmal; Alter ab dem ersten
      Konflikt
- [x] ``forget`` (Lock erworben, DLQ) nimmt ihn heraus
- [x] ein Eintrag, der laenger als ``LOCK_WAIT_STALE_S`` nicht gesehen wurde,
      faellt heraus (die Nachricht ging an einen anderen Worker)
- [x] hoechstens ``LOCK_WAIT_MAX_TRACKED`` Eintraege (der aelteste faellt)
- [x] ohne Eintraege: 0 und 0
"""

from __future__ import annotations

import pytest

from swarmq.worker.lock_wait_tracker import LOCK_WAIT_STALE_S, LockWaitTracker

pytestmark = pytest.mark.timeout(5)


def test_conflicts_count_once_per_task_and_age_from_the_first_conflict() -> None:
    tracker = LockWaitTracker()
    tracker.record("t1", wait_since_ms=1_000_000, now_ms=1_030_000)
    tracker.record("t2", wait_since_ms=1_020_000, now_ms=1_030_000)
    tracker.record("t1", wait_since_ms=1_000_000, now_ms=1_040_000)

    assert tracker.snapshot(now_ms=1_040_000) == (2, 40.0)


def test_forget_removes_the_wait() -> None:
    tracker = LockWaitTracker()
    tracker.record("t1", wait_since_ms=0, now_ms=1_000)
    tracker.forget("t1")
    tracker.forget("unknown")
    assert tracker.snapshot(now_ms=1_000) == (0, 0.0)


def test_a_wait_not_seen_for_a_while_drops_out() -> None:
    tracker = LockWaitTracker()
    tracker.record("t1", wait_since_ms=0, now_ms=10_000)
    tracker.record("t2", wait_since_ms=5_000, now_ms=100_000)
    stale = 10_000 + int(LOCK_WAIT_STALE_S * 1000) + 1

    assert tracker.snapshot(now_ms=stale) == (1, (stale - 5_000) / 1000)


def test_the_number_of_tracked_waits_is_bounded(monkeypatch) -> None:
    from swarmq.worker import lock_wait_tracker

    monkeypatch.setattr(lock_wait_tracker, "LOCK_WAIT_MAX_TRACKED", 2)
    tracker = LockWaitTracker()
    for i, task_id in enumerate(["old", "mid", "new"]):
        tracker.record(task_id, wait_since_ms=i, now_ms=10)
    assert tracker.snapshot(now_ms=10) == (2, (10 - 1) / 1000)


def test_empty_is_zero() -> None:
    assert LockWaitTracker().snapshot(now_ms=5) == (0, 0.0)
