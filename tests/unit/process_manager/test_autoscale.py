"""Autoskalierer über mehrere Takte (T-F-10, Plan 2026-10-20-1500, U7).

Die Bestandstests in ``test_supervisor.py::TestProcessManagerAutoscale`` setzen
alle Fristen auf null und fahren je einen Takt. Hier laufen die Vorgabefristen
(Abtastung, Abkühlzeit, Leerlauffrist, Mindestalter) unter virtueller Uhr, und
geprüft wird, was ein Takt für die folgenden hinterlässt.

Test-Liste:
- Die Leerlauffrist muss ganz verstrichen sein, bevor ein Worker geht.
- Nach einem Hochskalieren beginnt die Leerlauffrist von vorn.
- Nach einem Herunterskalieren beginnt die Leerlauffrist von vorn.
"""

from __future__ import annotations

import multiprocessing
import types
from unittest.mock import MagicMock

import pytest

from swarmq.process_manager import ProcessManager
from swarmq.process_manager import autoscale as autoscale_module
from swarmq.process_manager import supervisor as supervisor_module
from tests.unit.process_manager._helpers import FakeClock, _entry_stub, _fake_process, as_mock


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Eine virtuelle Uhr für Supervisor (Startzeit der Worker) und Autoskalierer."""
    fake = FakeClock()
    fake_time = types.SimpleNamespace(monotonic=fake, sleep=lambda _s: None)
    for module in (supervisor_module, autoscale_module):
        monkeypatch.setattr(module, "time", fake_time)
    return fake


def _pm(clock: FakeClock, n: int, depth: list[int], autoscale: tuple[int, int] = (2, 5)) -> ProcessManager:
    """Autoskalierte Gruppe mit ``n`` Workern, die zu ``clock.now`` schon 100 s alt sind."""
    pm = ProcessManager(num_workers=n, worker_entry=_entry_stub, autoscale=autoscale, queue_depth_fn=lambda: depth[0])
    pm._ctx = MagicMock()
    pids = iter(range(100, 200))
    pm._ctx.Process = MagicMock(side_effect=lambda **_kw: _fake_process(pid=next(pids)))
    pm._ctx.Value = multiprocessing.get_context("spawn").Value
    clock.now -= 100.0
    pm._start_all_workers()
    clock.now += 100.0
    return pm


def _drained(pm: ProcessManager) -> int:
    """Zum sanften Ende markierte Worker; jeder hat genau ein SIGTERM erhalten."""
    drained = [w for w in pm.workers.values() if w.expected_shutdown]
    for info in drained:
        as_mock(info.process).terminate.assert_called_once()
    return len(drained)


def _reap_drained(pm: ProcessManager) -> None:
    """Sanft beendete Worker sind fertig; der Supervisor räumt sie ab (ohne Ersatz)."""
    for info in pm.workers.values():
        if info.expected_shutdown:
            as_mock(info.process).is_alive.return_value = False
            as_mock(info.process).exitcode = 0
    pm._check_dead_workers()


def test_a_worker_goes_only_after_the_full_idle_window(clock: FakeClock) -> None:
    """Mutanten ``autoscale.py:85`` ``now - pm._idle_since`` -> ``now + pm._idle_since``
    und ``:87`` ``pm._idle_since = now`` -> ``= None``."""
    pm = _pm(clock, n=3, depth=[0])
    autoscale_module.tick(pm)  # Leerlauf beginnt
    clock.now += pm.SCALE_DOWN_IDLE_S / 2
    autoscale_module.tick(pm)
    assert _drained(pm) == 0
    clock.now += pm.SCALE_DOWN_IDLE_S / 2
    autoscale_module.tick(pm)
    assert _drained(pm) == 1


def test_after_a_scale_up_the_idle_window_starts_over(clock: FakeClock) -> None:
    """Mutant ``autoscale.py:77`` ``pm._idle_since = None`` -> ``= ""`` (``TypeError``
    im nächsten Leerlauf-Takt); auch das Streichen der Zeile wird rot."""
    depth = [0]
    pm = _pm(clock, n=3, depth=depth)
    autoscale_module.tick(pm)  # Leerlauf beginnt bei t0
    clock.now += pm.AUTOSCALE_SAMPLE_INTERVAL
    depth[0] = 10_000
    autoscale_module.tick(pm)
    assert len(pm.workers) == 4
    depth[0] = 0
    clock.now += pm.SCALE_COOLDOWN_S
    autoscale_module.tick(pm)  # Leerlauf beginnt neu
    clock.now += pm.SCALE_DOWN_IDLE_S / 2
    autoscale_module.tick(pm)
    assert _drained(pm) == 0  # über 60 s nach t0, aber erst 30 s nach dem Neubeginn
    clock.now += pm.SCALE_DOWN_IDLE_S / 2
    autoscale_module.tick(pm)
    assert _drained(pm) == 1


def test_after_a_scale_down_the_idle_window_starts_over(clock: FakeClock) -> None:
    """Mutant ``autoscale.py:88`` ``pm._idle_since = None`` -> ``= ""`` (``TypeError``
    im nächsten Leerlauf-Takt); auch das Streichen der Zeile wird rot."""
    pm = _pm(clock, n=4, depth=[0])
    autoscale_module.tick(pm)  # Leerlauf beginnt
    clock.now += pm.SCALE_DOWN_IDLE_S
    autoscale_module.tick(pm)
    assert _drained(pm) == 1
    _reap_drained(pm)
    clock.now += pm.SCALE_COOLDOWN_S
    autoscale_module.tick(pm)  # Leerlauf beginnt neu
    clock.now += pm.SCALE_DOWN_IDLE_S / 2
    autoscale_module.tick(pm)
    assert len(pm.workers) == 3
    assert _drained(pm) == 0
    clock.now += pm.SCALE_DOWN_IDLE_S / 2
    autoscale_module.tick(pm)
    assert _drained(pm) == 1
