"""BZ-27 (Plan 0.12.0, MR 17): Abkuehlzeit statt Dauersperre fuer Worker-Slots.

Test-Liste:
- [x] ``SlotTable.lock`` ohne Frist bleibt gesperrt, auch fuer eine ferne Uhr
- [x] ``unlock_expired`` hebt nur abgelaufene Sperren auf und liefert die Slots
- [x] Crash-Budget sperrt mit ``SLOT_COOLDOWN_S``; vor Ablauf kein Ersatz
- [x] Hang-Budget sperrt ebenfalls mit Abkuehlzeit; danach wird der Slot belegt
- [x] Entsperrter Slot mit lebendem Vorgaenger ist aktiv, nicht frei
- [x] Event ``worker_slot_unlocked`` mit Slot und Abkuehlzeit; Sperr-Events nennen die Abkuehlzeit
- [x] Kanal ``locked_slots``: der Supervisor schreibt die Zahl gesperrter Slots
"""

from __future__ import annotations

import math
import multiprocessing
import types
from unittest.mock import MagicMock

import pytest

from swarmq.process_manager import ProcessManager, WorkerInfo
from swarmq.process_manager import supervisor as supervisor_module
from swarmq.process_manager.slots import SlotState, SlotTable
from tests.unit.process_manager._helpers import FakeClock, _entry_stub, _fake_process, _register


def test_lock_without_deadline_stays_locked() -> None:
    table = SlotTable(2)
    table.lock(1)
    assert table.unlock_expired(math.inf) == []
    assert table.state(1) is SlotState.LOCKED


def test_unlock_expired_releases_only_expired_locks() -> None:
    table = SlotTable(3)
    table.lock(0, until=10.0)
    table.lock(2, until=20.0)
    assert table.unlock_expired(9.9) == []
    assert table.unlock_expired(10.0) == [0]
    assert table.state(0) is SlotState.FREE and table.state(2) is SlotState.LOCKED
    assert table.locked() == 1


def _pm(clock: FakeClock, monkeypatch: pytest.MonkeyPatch, **kw) -> ProcessManager:
    monkeypatch.setattr(supervisor_module, "time", types.SimpleNamespace(monotonic=clock, sleep=lambda _s: None))
    pm = ProcessManager(worker_entry=_entry_stub, **kw)
    pm._ctx = MagicMock()
    pm._ctx.Process = MagicMock(side_effect=lambda **_kw: _fake_process(pid=900))
    pm._ctx.Value = multiprocessing.get_context("spawn").Value
    return pm


def test_crash_budget_locks_for_the_cooldown(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    pm = _pm(clock, monkeypatch, num_workers=1)
    crashed = WorkerInfo(
        process=_fake_process(alive=False, exitcode=1, pid=2),
        worker_id=0,
        restart_timestamps=[clock.now - 10.0] * pm.MAX_RESTARTS,
    )
    _register(pm, crashed)
    pm._check_dead_workers()
    assert pm.slot_state(0) is SlotState.LOCKED

    clock.now += pm.SLOT_COOLDOWN_S - 1.0
    pm._unlock_expired_slots()
    assert pm.slot_state(0) is SlotState.LOCKED

    clock.now += 1.0
    pm._unlock_expired_slots()
    assert pm.slot_state(0) is SlotState.FREE
    assert pm._current_target() == 1


def test_hang_budget_locks_for_the_cooldown_and_the_predecessor_keeps_the_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    pm = _pm(clock, monkeypatch, num_workers=1, heartbeat_timeout_s=10.0, delivery_limit=1)
    hung = WorkerInfo(process=_fake_process(pid=5), worker_id=0)
    _register(pm, hung)
    pm._hang_timestamps = [clock.now] * pm.hang_replacement_limit
    pm._replace_hung_worker(hung, stale_s=61.0)
    assert pm.slot_state(0) is SlotState.LOCKED

    clock.now += pm.SLOT_COOLDOWN_S
    pm._unlock_expired_slots()
    # Der Vorgaenger lebt noch (SIGKILL nicht angekommen): der Slot ist belegt.
    assert pm.slot_state(0) is SlotState.ACTIVE


def test_unlock_and_lock_events_name_the_cooldown(monkeypatch: pytest.MonkeyPatch, swarmq_logs) -> None:
    clock = FakeClock()
    pm = _pm(clock, monkeypatch, num_workers=2)
    crashed = WorkerInfo(
        process=_fake_process(alive=False, exitcode=1, pid=2),
        worker_id=1,
        restart_timestamps=[clock.now - 10.0] * pm.MAX_RESTARTS,
    )
    _register(pm, WorkerInfo(process=_fake_process(pid=1), worker_id=0), crashed)
    pm._check_dead_workers()
    clock.now += pm.SLOT_COOLDOWN_S
    pm._unlock_expired_slots()

    (locked,) = [e for e in swarmq_logs if e["event"] == "worker_restart_limit_exceeded"]
    assert locked["cooldown_s"] == pm.SLOT_COOLDOWN_S
    (unlocked,) = [e for e in swarmq_logs if e["event"] == "worker_slot_unlocked"]
    assert unlocked["worker_id"] == 1 and unlocked["cooldown_s"] == pm.SLOT_COOLDOWN_S
    assert unlocked["locked_slots"] == 0


def test_channels_report_the_locked_slots(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    pm = _pm(clock, monkeypatch, num_workers=2)
    survivor = pm._start_worker(0)
    crashed = pm._start_worker(1)
    crashed.process = _fake_process(alive=False, exitcode=1, pid=2)
    crashed.restart_timestamps = [clock.now - 10.0] * pm.MAX_RESTARTS
    pm.workers = {1: survivor, 2: crashed}
    pm._check_dead_workers()
    pm._unlock_expired_slots()
    assert survivor.channel is not None and survivor.channel.locked_slots_value() == 1

    clock.now += pm.SLOT_COOLDOWN_S
    pm._unlock_expired_slots()
    assert survivor.channel.locked_slots_value() == 0
