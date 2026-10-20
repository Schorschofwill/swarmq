"""Autoskalierer über mehrere Takte (T-F-10, Plan 2026-10-20-1500, U7).

Die Bestandstests in ``test_supervisor.py::TestProcessManagerAutoscale`` setzen
alle Fristen auf null und fahren je einen Takt. Hier laufen die Vorgabefristen
(Abtastung, Abkühlzeit, Leerlauffrist, Mindestalter) unter virtueller Uhr, und
geprüft wird, was ein Takt für die folgenden hinterlässt.

Test-Liste:
- Die Leerlauffrist muss ganz verstrichen sein, bevor ein Worker geht.
- Nach einem Hochskalieren beginnt die Leerlauffrist von vorn.
- Nach einem Herunterskalieren beginnt die Leerlauffrist von vorn.

Test-Liste 0.14.2 (F4-25, F4-26, E-084; Plan 2026-10-20-2100, U2):
- Ein Abbau-Takt, in dem nur drainende oder zu junge Worker übrig sind,
  schickt kein SIGTERM (F4-25).
- Ein solcher Takt verbraucht weder Abkühlzeit noch Leerlauf: Sobald ein
  Worker alt genug ist, baut der nächste Abtast-Takt ab (F4-25).
- E-084: Ein drainender Worker zählt auch für die Schwelle des Hochskalierens
  nicht (Option b): drei Worker, einer draint, Tiefe 120 > 2 x 50 -> hoch.
- Regressionstest (E-084) ohne Spielraum: Der drainende Worker hält seinen Slot, bei vollem
  Slot-Bestand startet nichts; nach seinem Exit startet einer im frei
  gewordenen Slot.
- E-084 mit Spielraum (--max-processes): genau ein Start in einen
  Spielraum-Slot, danach aktiv = MAX und kein weiterer Start.
- E-084: Recycling-Ersatz während eines Drains -- der Slot im Übergang zählt
  einmal, kein Abbau unter MIN, der Ersatz wird nie gewählt.
- E-084: Hang-Ersatz während eines Drains -- ebenso.
- Regressionstest (E-024): erschöpftes Hang-Budget -- der Slot ist gesperrt,
  der Vorgänger lebt drainend weiter und zählt nicht; in den gesperrten Slot
  startet nichts.
"""

from __future__ import annotations

import multiprocessing
import types
from typing import Any
from unittest.mock import MagicMock

import pytest

from swarmq.process_manager import ProcessManager
from swarmq.process_manager import autoscale as autoscale_module
from swarmq.process_manager import supervisor as supervisor_module
from swarmq.process_manager.slots import SlotState
from tests.unit.process_manager._helpers import FakeClock, _entry_stub, _fake_process, as_mock


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Eine virtuelle Uhr für Supervisor (Startzeit der Worker) und Autoskalierer."""
    fake = FakeClock()
    fake_time = types.SimpleNamespace(monotonic=fake, sleep=lambda _s: None)
    for module in (supervisor_module, autoscale_module):
        monkeypatch.setattr(module, "time", fake_time)
    return fake


def _pm(
    clock: FakeClock, n: int, depth: list[int], autoscale: tuple[int, int] = (2, 5), max_processes: int | None = None
) -> ProcessManager:
    """Autoskalierte Gruppe mit ``n`` Workern, die zu ``clock.now`` schon 100 s alt sind."""
    pm = ProcessManager(
        num_workers=n,
        worker_entry=_entry_stub,
        autoscale=autoscale,
        queue_depth_fn=lambda: depth[0],
        max_processes=max_processes,
    )
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


def _crash_and_restart(pm: ProcessManager, *slots: int) -> None:
    """Die Worker der Slots stürzen ab; der Supervisor startet je einen jungen Ersatz."""
    for info in pm.workers.values():
        if info.worker_id in slots and not info.expected_shutdown:
            as_mock(info.process).is_alive.return_value = False
            as_mock(info.process).exitcode = 1
    pm._check_dead_workers()


def _drain_one_by_idle(pm: ProcessManager, clock: FakeClock) -> None:
    """Ein Abbau-Takt nach voller Leerlauffrist (die Abkühlzeit ist vorbei)."""
    autoscale_module.tick(pm)  # Leerlauf beginnt
    clock.now += pm.SCALE_DOWN_IDLE_S
    autoscale_module.tick(pm)


@pytest.mark.befund("F4-25")
def test_a_down_tick_with_only_draining_or_young_candidates_sends_no_sigterm(clock: FakeClock) -> None:
    pm = _pm(clock, n=3, depth=[0], autoscale=(1, 5))
    _drain_one_by_idle(pm, clock)
    (draining,) = [w for w in pm.workers.values() if w.expected_shutdown]
    clock.now += pm.SCALE_COOLDOWN_S
    autoscale_module.tick(pm)  # Leerlauf beginnt: zwei nicht drainende Worker > MIN
    clock.now += pm.SCALE_DOWN_IDLE_S - 10.0
    _crash_and_restart(pm, 0, 1)  # beide Ersatzprozesse sind beim Abbau-Takt 10 s alt
    clock.now += 10.0

    autoscale_module.tick(pm)

    as_mock(draining.process).terminate.assert_called_once()
    assert [w for w in pm.workers.values() if w.expected_shutdown] == [draining]


@pytest.mark.befund("F4-25")
def test_a_down_tick_without_candidates_is_not_used_up(clock: FakeClock) -> None:
    pm = _pm(clock, n=3, depth=[0], autoscale=(1, 5))
    _drain_one_by_idle(pm, clock)
    clock.now += pm.SCALE_COOLDOWN_S
    autoscale_module.tick(pm)  # Leerlauf beginnt
    clock.now += pm.SCALE_DOWN_IDLE_S - 10.0
    _crash_and_restart(pm, 0, 1)
    clock.now += 10.0
    autoscale_module.tick(pm)  # kein Kandidat: die Ersatzprozesse sind 10 s alt
    young_left = pm.SCALE_MIN_AGE_S - 10.0

    clock.now += young_left  # jetzt sind sie alt genug, Abkühlzeit und Leerlauf liegen zurück
    autoscale_module.tick(pm)

    assert _drained(pm) == 2


@pytest.mark.entscheidung("E-084")
def test_a_draining_worker_does_not_count_towards_the_scale_up_threshold(clock: FakeClock) -> None:
    depth = [0]
    pm = _pm(clock, n=3, depth=depth, autoscale=(1, 5))
    _drain_one_by_idle(pm, clock)
    clock.now += pm.SCALE_COOLDOWN_S
    depth[0] = 2 * pm.SCALE_UP_THRESHOLD + 20  # über zwei, unter drei aktiven Workern

    autoscale_module.tick(pm)

    assert sorted(w.worker_id for w in pm.workers.values() if not w.expected_shutdown) == [0, 1, 3]


def _heading(pm: ProcessManager) -> list[int]:
    """Slots mit einem nicht drainenden Prozess, aufsteigend."""
    return sorted(w.worker_id for w in pm.workers.values() if not w.expected_shutdown)


def _run(pm: ProcessManager, clock: FakeClock, seconds: float) -> None:
    """Autoskalierer im Abtasttakt laufen lassen (ohne Reap)."""
    end = clock.now + seconds
    while clock.now < end:
        autoscale_module.tick(pm)
        clock.now += pm.AUTOSCALE_SAMPLE_INTERVAL


@pytest.mark.entscheidung("E-084")
def test_without_headroom_a_draining_worker_holds_its_slot_until_it_exits(clock: FakeClock) -> None:
    """Regressionstest: Ohne --max-processes hat die Tabelle MAX Slots."""
    depth = [0]
    pm = _pm(clock, n=3, depth=depth, autoscale=(1, 3))
    _drain_one_by_idle(pm, clock)
    clock.now += pm.SCALE_COOLDOWN_S
    depth[0] = 10_000

    _run(pm, clock, 3 * pm.SCALE_COOLDOWN_S)
    assert len(pm.workers) == 3
    assert _heading(pm) == [0, 1]

    _reap_drained(pm)
    _run(pm, clock, pm.AUTOSCALE_SAMPLE_INTERVAL)
    assert _heading(pm) == [0, 1, 2]


@pytest.mark.entscheidung("E-084")
def test_with_headroom_a_drain_lets_one_worker_start_in_a_headroom_slot(clock: FakeClock) -> None:
    depth = [0]
    pm = _pm(clock, n=3, depth=depth, autoscale=(1, 3), max_processes=5)
    _drain_one_by_idle(pm, clock)
    clock.now += pm.SCALE_COOLDOWN_S
    depth[0] = 10_000

    _run(pm, clock, 3 * pm.SCALE_COOLDOWN_S)

    assert _heading(pm) == [0, 1, 3]  # aktiv = MAX
    assert len(pm.workers) == 4  # plus der drainende, <= --max-processes


def _recycle(pm: ProcessManager, slot: int) -> None:
    """Recycling wie ``_check_heartbeats``: Vorgänger draint, Ersatz im selben Slot."""
    (old,) = [w for w in pm.workers.values() if w.worker_id == slot and not w.expected_shutdown]
    old.expected_shutdown = True
    pm._start_worker(slot)


def _hang(pm: ProcessManager, slot: int) -> None:
    """Der Worker im Slot hängt; der Supervisor ersetzt ihn (oder sperrt den Slot)."""
    (old,) = [w for w in pm.workers.values() if w.worker_id == slot and not w.expected_shutdown]
    pm._replace_hung_worker(old, stale_s=pm.heartbeat_timeout_s + 1.0)


@pytest.mark.entscheidung("E-084")
@pytest.mark.parametrize("replace", [_recycle, _hang], ids=["recycling", "hang"])
def test_a_slot_in_transition_during_a_drain_counts_once_and_its_replacement_stays(
    clock: FakeClock, replace: Any
) -> None:
    pm = _pm(clock, n=3, depth=[0], autoscale=(2, 5))
    _drain_one_by_idle(pm, clock)  # Slot 2 draint, zwei nicht drainende = MIN
    replace(pm, 0)
    (replacement,) = [w for w in pm.workers.values() if w.worker_id == 0 and not w.expected_shutdown]
    terminates_before = {pid: as_mock(w.process).terminate.call_count for pid, w in pm.workers.items()}

    _run(pm, clock, 3 * (pm.SCALE_COOLDOWN_S + pm.SCALE_DOWN_IDLE_S))

    assert _heading(pm) == [0, 1]
    as_mock(replacement.process).terminate.assert_not_called()
    terminates_after = {pid: as_mock(w.process).terminate.call_count for pid, w in pm.workers.items()}
    assert terminates_after == terminates_before


def test_an_exhausted_hang_budget_locks_the_slot_and_its_draining_worker_does_not_count(clock: FakeClock) -> None:
    """Regressionstest (E-024): Der gesperrte Slot gilt nicht als frei, der
    hängende Vorgänger lebt bis zum SIGKILL mit ``expected_shutdown`` weiter."""
    depth = [0]
    pm = _pm(clock, n=3, depth=depth, autoscale=(1, 5))
    pm._hang_timestamps = [clock.now] * pm.hang_replacement_limit
    _hang(pm, 0)
    assert pm.slot_state(0) is SlotState.LOCKED
    depth[0] = 2 * pm.SCALE_UP_THRESHOLD + 20  # über zwei, unter drei aktiven Workern

    autoscale_module.tick(pm)

    assert _heading(pm) == [1, 2, 3]
    assert pm.slot_state(0) is SlotState.LOCKED
