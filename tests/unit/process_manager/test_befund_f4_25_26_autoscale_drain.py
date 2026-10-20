"""Nachweise F4-25 und F4-26: Abbau-Takte des Autoskalierers, während ein
früher abgebauter Worker noch draint.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-25, F4-26; Verdacht
V-E der Test-Runde 2026-10-20, Probe der Gegenprüfung).

- F4-25: ``scale_down_one`` wählt einen Worker, der schon draint
  (``expected_shutdown``), erneut und schickt ihm ein zweites SIGTERM; der
  Abbau-Takt ist verbraucht. Erwartet: Der drainende Worker bekommt kein
  zweites ``terminate``, der Takt trifft einen anderen Worker.
- F4-26: Der Autoskalierer zählt drainende Slots zur Untergrenze
  (``occupied()``); ein jüngerer Ersatzprozess kann abgebaut werden, und die
  nicht drainenden Worker fallen unter ``MIN``. Erwartet: Es bleiben
  mindestens ``MIN`` nicht drainende Worker.

Beide behoben in 0.14.2 (Regel E-084: drainende Worker zählen für den
Autoskalierer nicht und werden nicht erneut gewählt); die Tests laufen ohne
``xfail`` und bleiben als Rückverweis mit ``befund`` markiert.

Testliste:
# [x] --autoscale 1,5, drei Worker: zweiter Abbau-Takt während eines langen
#     Drains -> ein terminate am drainenden Worker, ein zweiter Worker draint
# [x] --autoscale 2,5, drei Worker: Abbau, Absturz und Ersatz, langer Drain
#     -> mindestens MIN nicht drainende Worker
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

# Ein Abbau-Takt nach dem Start: Leerlauffrist plus eine Abtastung Verzug.
_FIRST_DOWN_S = ProcessManager.SCALE_DOWN_IDLE_S + 1
# Der nächste Abbau-Takt folgt nach Abkühlzeit und Leerlauffrist (rund 90 s);
# so lange (und länger) draint der Worker hier, ohne abgeräumt zu werden.
_LONG_DRAIN_S = 2 * (ProcessManager.SCALE_COOLDOWN_S + ProcessManager.SCALE_DOWN_IDLE_S)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Eine virtuelle Uhr für Supervisor (Startzeit der Worker) und Autoskalierer."""
    fake = FakeClock()
    fake_time = types.SimpleNamespace(monotonic=fake, sleep=lambda _s: None)
    for module in (supervisor_module, autoscale_module):
        monkeypatch.setattr(module, "time", fake_time)
    return fake


def _pm(clock: FakeClock, n: int, autoscale: tuple[int, int]) -> ProcessManager:
    """Autoskalierte Gruppe mit ``n`` Workern ohne Rückstau, zu ``clock.now`` schon 100 s alt."""
    pm = ProcessManager(num_workers=n, worker_entry=_entry_stub, autoscale=autoscale, queue_depth_fn=lambda: 0)
    pm._ctx = MagicMock()
    pids = iter(range(100, 200))
    pm._ctx.Process = MagicMock(side_effect=lambda **_kw: _fake_process(pid=next(pids)))
    pm._ctx.Value = multiprocessing.get_context("spawn").Value
    clock.now -= 100.0
    pm._start_all_workers()
    clock.now += 100.0
    return pm


def _run(pm: ProcessManager, clock: FakeClock, seconds: float) -> None:
    """Autoskalierer im Takt des Monitor-Loops laufen lassen (ohne Reap)."""
    end = clock.now + seconds
    while clock.now < end:
        autoscale_module.tick(pm)
        clock.now += pm.MONITOR_INTERVAL


@pytest.mark.befund("F4-25")
@pytest.mark.entscheidung("E-084")
def test_a_down_tick_during_a_long_drain_leaves_the_draining_worker_alone(clock: FakeClock) -> None:
    pm = _pm(clock, n=3, autoscale=(1, 5))
    _run(pm, clock, _FIRST_DOWN_S)
    (draining,) = [w for w in pm.workers.values() if w.expected_shutdown]

    _run(pm, clock, _LONG_DRAIN_S)

    terminates = as_mock(draining.process).terminate.call_count
    assert terminates == 1, f"der drainende Worker bekam {terminates} SIGTERM statt eines (F4-25)"
    drained = [w.worker_id for w in pm.workers.values() if w.expected_shutdown]
    assert len(drained) == 2, f"der zweite Abbau-Takt traf keinen anderen Worker: drainend {drained} (F4-25)"


@pytest.mark.befund("F4-26")
@pytest.mark.entscheidung("E-084")
def test_a_draining_slot_does_not_count_towards_the_lower_bound(clock: FakeClock) -> None:
    min_workers = 2
    pm = _pm(clock, n=3, autoscale=(min_workers, 5))
    _run(pm, clock, _FIRST_DOWN_S)  # Slot 2 draint, zwei Worker bleiben (= MIN)
    crashed = next(w for w in pm.workers.values() if w.worker_id == 0)
    as_mock(crashed.process).is_alive.return_value = False
    as_mock(crashed.process).exitcode = 1
    pm._check_dead_workers()  # Ersatz für Slot 0: der jüngste Prozess

    _run(pm, clock, _LONG_DRAIN_S)  # der Drain von Slot 2 läuft weiter

    heading = [w.worker_id for w in pm.workers.values() if not w.expected_shutdown]
    assert len(heading) >= min_workers, f"nicht drainende Worker {heading} unter MIN={min_workers} (F4-26)"
