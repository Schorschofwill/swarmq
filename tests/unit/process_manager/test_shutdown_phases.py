"""Phasen des Supervisor-Shutdowns im Gesamtbudget (BZ-18, Plan 0.12.0, MR 17).

Virtuelle Uhr wie im Nachweis ``test_befund_bz18_shutdown_budget.py``; die
Worker sind Fakes.

Test-Liste:
- [x] SIGTERM genau bei 23 s, SIGKILL genau bei 28 s (Default 30 s), ohne Ueberschiessen
- [x] mehrere Worker, die selbst SIGKILL ueberleben (D-Zustand): Einsammeln inkl.
      ``_collect_zombies`` endet bei 30 s, nicht 2 s + 1 s je Worker spaeter
- [x] Worker, die im Drain sauber enden, bekommen kein Signal
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from swarmq.process_manager import ProcessManager, WorkerInfo
from tests.unit.process_manager._helpers import _entry_stub


class _VirtualTime:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _worker(
    clock: _VirtualTime,
    pid: int,
    events: list[tuple[str, int, float]],
    *,
    exits_at: float | None = None,
    survives_kill: bool = False,
) -> MagicMock:
    proc = MagicMock()
    proc.pid = pid
    state = {"killed": False}

    def _alive() -> bool:
        if exits_at is not None and clock.now >= exits_at:
            return False
        return survives_kill or not state["killed"]

    def _kill() -> None:
        events.append(("sigkill", pid, clock.now))
        state["killed"] = True

    def _join(timeout: float | None = None) -> None:
        if _alive() and timeout is not None:
            clock.now += timeout

    proc.is_alive.side_effect = _alive
    proc.terminate.side_effect = lambda: events.append(("sigterm", pid, clock.now))
    proc.kill.side_effect = _kill
    proc.join.side_effect = _join
    return proc


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _VirtualTime:
    c = _VirtualTime()
    monkeypatch.setattr(
        "swarmq.process_manager.supervisor.time",
        SimpleNamespace(monotonic=c.monotonic, sleep=c.sleep),
    )
    return c


def _pm(clock: _VirtualTime, *procs: MagicMock) -> ProcessManager:
    pm = ProcessManager(num_workers=len(procs), worker_entry=_entry_stub, shutdown_grace_s=30.0)
    pm.workers = {p.pid: WorkerInfo(process=p, worker_id=i) for i, p in enumerate(procs)}
    return pm


def test_escalation_hits_the_phase_boundaries_exactly(clock: _VirtualTime) -> None:
    events: list[tuple[str, int, float]] = []
    pm = _pm(clock, _worker(clock, 1, events))
    pm._shutdown_all_workers()
    assert events == [("sigterm", 1, 23.0), ("sigkill", 1, 28.0)]


def test_unkillable_workers_do_not_stretch_the_budget(clock: _VirtualTime) -> None:
    events: list[tuple[str, int, float]] = []
    pm = _pm(clock, *(_worker(clock, pid, events, survives_kill=True) for pid in (1, 2, 3)))
    pm._shutdown_all_workers()
    pm._collect_zombies()
    assert clock.now <= 30.0


def test_workers_that_finish_their_drain_get_no_signal(clock: _VirtualTime) -> None:
    events: list[tuple[str, int, float]] = []
    pm = _pm(clock, _worker(clock, 1, events, exits_at=5.0), _worker(clock, 2, events, exits_at=20.0))
    pm._shutdown_all_workers()
    assert events == []
    assert clock.now <= 20.3
