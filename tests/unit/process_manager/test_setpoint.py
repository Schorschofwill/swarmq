"""Sollwert-Durchlauf des Supervisors (Plan Kontrollkanal, U5-U7).

Test-Liste U5 (Pruefregeln R10, Lesen beim Start):
- Sollwert innerhalb des Spielraums ist zulaessig.
- Obergrenze ueber dem Spielraum wird mit dem Spielraum als Grund abgelehnt (AE5/AE12).
- Untergrenze unter eins und verdrehte Grenzen werden abgelehnt.
- Bereichs-Sollwert ohne Autoskalierung (Rechen-Pool, feste Prozesszahl) wird abgelehnt (AE11/AE13).
- Ohne --max-processes wird keine Fabrik gerufen (AE1).
- Sollwert im Spielraum ersetzt die Startwerte vor dem ersten Kind (AE2).
- Fester Sollwert 4..4 bei --processes 2 startet vier Worker (AE7/AE8).
- Abgelehnter Sollwert: Gruppe faehrt mit Startargumenten hoch (AE12).
- Nicht erreichbarer Speicher: Startargumente, Lage „ungelesen" (AE7).
- Unlesbarer Sollwert wird wie ein abgelehnter behandelt.
"""

from __future__ import annotations

import multiprocessing
from unittest.mock import MagicMock

import pytest

from swarmq.contracts.control import Setpoint, SetpointUnreadable
from swarmq.process_manager import ProcessManager, setpoint
from swarmq.process_manager.slots import SlotState
from tests.unit.process_manager._helpers import _entry_stub, _fake_process


class TestValidate:
    def test_setpoint_within_headroom_is_accepted(self):
        assert setpoint.validate(Setpoint(2, 8), headroom=16, autoscaling=True) is None
        assert setpoint.validate(Setpoint(4, 4), headroom=16, autoscaling=False) is None

    def test_upper_bound_above_headroom_names_the_headroom(self):
        reason = setpoint.validate(Setpoint(2, 12), headroom=8, autoscaling=True)
        assert reason is not None and "headroom 8" in reason

    @pytest.mark.parametrize("sp", [Setpoint(0, 4), Setpoint(-1, 4), Setpoint(6, 2)])
    def test_lower_bound_below_one_and_inverted_bounds_are_rejected(self, sp):
        assert setpoint.validate(sp, headroom=16, autoscaling=True) is not None

    def test_range_without_autoscaling_demands_the_fixed_form(self):
        """Covers AE11 (Rechen-Pool) und AE13 (Autoskalierung aus): beide laufen ohne Autoskalierung."""
        reason = setpoint.validate(Setpoint(2, 6), headroom=8, autoscaling=False)
        assert reason is not None and "N..N" in reason


class FakeStore:
    """Sollwert-Speicher fuer die Fabrik-Seam: liefert ein festes Ergebnis oder scheitert."""

    def __init__(self, result=None, *, fail=False):
        self.result = result
        self.fail = fail
        self.status_writes: list[dict] = []

    async def connect(self):
        if self.fail:
            raise ConnectionRefusedError("backend down")

    async def close(self):
        pass

    async def read_setpoint(self, group):
        return self.result

    async def write_status(self, group, supervisor_id, status, *, ttl_s):
        self.status_writes.append(dict(status))


def _pm(n: int = 2, *, store=None, factory=None, **kw) -> ProcessManager:
    """Supervisor mit gefaelschten Prozessen; PID = 100 * Generation + Slot."""
    if factory is None and store is not None:
        factory = lambda: store  # noqa: E731
    pm = ProcessManager(
        num_workers=n, worker_entry=_entry_stub, setpoint_store_factory=factory,
        supervisor_id="host:p1", group="orders", **kw,
    )
    pm._ctx = MagicMock()
    generations: dict[int, int] = {}

    def spawn(**kwargs):
        slot = kwargs["args"][1]
        generations[slot] = generations.get(slot, 0) + 1
        return _fake_process(pid=100 * generations[slot] + slot)

    pm._ctx.Process = MagicMock(side_effect=spawn)
    pm._ctx.Value = multiprocessing.get_context("spawn").Value
    return pm


class TestReadAtStart:
    def test_without_headroom_the_factory_is_never_called(self):
        """Covers AE1: ohne --max-processes kein Lesevorgang, auch wenn ein Sollwert hinterlegt ist."""
        calls = []

        def factory():
            calls.append(1)
            return FakeStore(Setpoint(4, 4))

        pm = _pm(n=2, factory=factory)
        setpoint.read_at_start(pm)
        pm._start_all_workers()
        assert calls == []
        assert pm.effective_bounds == (2, 2)
        assert len(pm.workers) == 2

    def test_setpoint_within_headroom_replaces_the_start_values_before_the_first_child(self):
        """Covers AE2: Autoscale 2..4, Spielraum 16, Sollwert 2..8 -> 2..8 wirkt, zwei Worker starten."""
        pm = _pm(n=2, store=FakeStore(Setpoint(2, 8)), autoscale=(2, 4), queue_depth_fn=lambda: 0, max_processes=16)
        setpoint.read_at_start(pm)
        assert pm.effective_bounds == (2, 8)
        pm._start_all_workers()
        assert len(pm.workers) == 2

    def test_fixed_setpoint_starts_that_many_workers_on_every_supervisor(self):
        """Covers AE7/AE8: --processes 2, Sollwert 4..4 -> vier Worker je Supervisor."""
        pms = [_pm(n=2, store=FakeStore(Setpoint(4, 4)), max_processes=8) for _ in range(2)]
        for pm in pms:
            setpoint.read_at_start(pm)
            pm._start_all_workers()
        assert [len(pm.workers) for pm in pms] == [4, 4]
        assert pms[0]._current_target() == 4

    def test_setpoint_above_headroom_is_rejected_and_the_group_starts_with_its_arguments(self):
        """Covers AE12: Sollwert 2..8 hinterlegt, Deploy senkt den Spielraum auf 4."""
        pm = _pm(n=2, store=FakeStore(Setpoint(2, 8)), autoscale=(2, 4), queue_depth_fn=lambda: 0, max_processes=4)
        setpoint.read_at_start(pm)
        pm._start_all_workers()
        assert pm.effective_bounds == (2, 4)
        assert len(pm.workers) == 2
        assert pm._setpoint.rejection is not None and "headroom 4" in pm._setpoint.rejection

    def test_unreachable_store_keeps_the_start_arguments_and_marks_unread(self):
        """Covers AE7 (erster Teil)."""
        pm = _pm(n=2, store=FakeStore(fail=True), max_processes=8)
        setpoint.read_at_start(pm)
        pm._start_all_workers()
        assert pm.effective_bounds == (2, 2) and len(pm.workers) == 2
        assert pm._setpoint.store == "unread"

    def test_unreadable_setpoint_is_treated_like_a_rejection(self):
        pm = _pm(n=2, store=FakeStore(SetpointUnreadable("not json")), max_processes=8)
        setpoint.read_at_start(pm)
        pm._start_all_workers()
        assert pm.effective_bounds == (2, 2) and len(pm.workers) == 2
        assert pm._setpoint.rejection is not None and "unreadable" in pm._setpoint.rejection

    def test_range_setpoint_on_a_group_without_autoscaling_is_rejected(self):
        """Covers AE11/AE13: feste Prozesszahl (auch Rechen-Pool) nimmt nur N..N."""
        pm = _pm(n=4, store=FakeStore(Setpoint(2, 6)), max_processes=8)
        setpoint.read_at_start(pm)
        assert pm.effective_bounds == (4, 4)
        assert pm._setpoint.rejection is not None and "N..N" in pm._setpoint.rejection


def _live_slots(pm: ProcessManager) -> list[int]:
    """Slots, deren Prozess nicht zum sanften Ende markiert ist."""
    return sorted(w.worker_id for w in pm.workers.values() if not w.expected_shutdown)


def _drained(pm: ProcessManager) -> list[int]:
    return sorted(w.worker_id for w in pm.workers.values() if w.expected_shutdown)


class TestTick:
    """U6: Laufzeit-Durchlauf -- uebernehmen, ablehnen, aufheben.

    Test-Liste:
    - Erhoehter Sollwert startet die fehlenden Worker im selben Durchlauf, ohne Last (AE10).
    - Gesenkter Sollwert beendet ueberzaehlige Worker sanft im selben Durchlauf (AE6).
    - Frisch gestartete Worker sind vom Abbau nicht ausgenommen.
    - Slots im Uebergang: Durchlauf erreicht den Sollwert nur teilweise, der naechste zieht nach.
    - Entfernter Sollwert stellt die Startargumente wieder her (AE9).
    - Nach einer Aenderung trifft der Autoskalierer in derselben Runde keine Entscheidung (KTD3).
    - Fester Sollwert unter der Startprozesszahl wird von der Nachfuellschleife nicht rueckgaengig gemacht.
    - Fest -> Bereich -> Startargumente bringt den Supervisor keiner Ausstiegsbedingung naeher.
    - Im Herunterfahren startet ein erhoehter Sollwert keine Worker mehr (KTD2).
    - Abtastintervall gilt, auch nach einem Fehlschlag.
    - Der Autoskalierer nutzt die wirksamen Grenzen, nicht das Startargument.
    """

    def _autoscaled(self, n: int, store: FakeStore, depth: int = 0, **kw) -> ProcessManager:
        pm = _pm(n=n, store=store, autoscale=kw.pop("autoscale", (2, 4)), queue_depth_fn=lambda: depth,
                 max_processes=kw.pop("max_processes", 16), **kw)
        pm.SETPOINT_SAMPLE_INTERVAL = 0.0
        pm._start_all_workers()
        return pm

    def test_raised_setpoint_starts_the_missing_workers_without_load(self):
        """Covers AE10: zwei Worker, leere Queues, Sollwert 6..8 -> sechs Worker."""
        store = FakeStore(None)
        pm = self._autoscaled(2, store)
        store.result = Setpoint(6, 8)
        setpoint.tick(pm)
        assert pm.effective_bounds == (6, 8)
        assert _live_slots(pm) == [0, 1, 2, 3, 4, 5]

    def test_lowered_setpoint_drains_the_surplus_gently_in_the_same_pass(self):
        """Covers AE6: sechs Worker, Sollwert 4..4 -> zwei enden per SIGTERM (Drain), Autoskalierung aus."""
        store = FakeStore(None)
        pm = self._autoscaled(6, store, autoscale=(2, 8))
        store.result = Setpoint(4, 4)
        setpoint.tick(pm)
        assert pm.effective_bounds == (4, 4)
        assert len(_live_slots(pm)) == 4 and len(_drained(pm)) == 2
        for info in pm.workers.values():
            if info.expected_shutdown:
                info.process.terminate.assert_called_once()
                info.process.kill.assert_not_called()

    def test_fresh_workers_are_not_exempt_from_the_shrink(self):
        store = FakeStore(None)
        pm = self._autoscaled(4, store, autoscale=(2, 8))
        pm.SCALE_MIN_AGE_S = 3600.0  # der Autoskalierer wuerde alle verschonen
        store.result = Setpoint(2, 2)
        setpoint.tick(pm)
        assert len(_drained(pm)) == 2

    def test_slots_in_transition_are_skipped_and_the_next_pass_catches_up(self):
        store = FakeStore(None)
        pm = self._autoscaled(3, store, autoscale=(1, 8))
        # Slots 1 und 2 im Uebergang: Ersatzprozesse halten sie neben dem Vorgaenger.
        old = {w.worker_id: pid for pid, w in pm.workers.items()}
        for slot in (1, 2):
            pm.workers[old[slot]].expected_shutdown = True
            pm._start_worker(slot)
        assert pm.slot_state(1) is SlotState.TRANSITION and pm.slot_state(2) is SlotState.TRANSITION
        store.result = Setpoint(1, 1)
        setpoint.tick(pm)
        # Nur Slot 0 war waehlbar: Ziel 1 noch nicht erreicht.
        assert _live_slots(pm) == [1, 2]
        for slot in (1, 2):  # Vorgaenger enden, Uebergang loest sich
            pm.workers[old[slot]].process.is_alive.return_value = False
            pm.workers[old[slot]].process.exitcode = 0
        pm._check_dead_workers()
        setpoint.tick(pm)
        assert len(_live_slots(pm)) == 1

    def test_removed_setpoint_restores_the_start_arguments(self):
        """Covers AE9: Sollwert 4..4 wirksam, Deployment 2..4, Sollwert entfernt -> 2..4 aus den Startargumenten."""
        store = FakeStore(Setpoint(4, 4))
        pm = self._autoscaled(2, store)
        setpoint.tick(pm)
        assert pm.effective_bounds == (4, 4) and len(_live_slots(pm)) == 4
        store.result = None
        setpoint.tick(pm)
        assert pm.effective_bounds == (2, 4)
        assert pm._setpoint.source == "startup"
        assert len(_live_slots(pm)) == 4  # innerhalb 2..4: der Autoskalierer baut spaeter bei Leerlauf ab

    def test_autoscaler_makes_no_decision_in_the_round_of_a_change(self):
        """KTD3: Rueckstau liegt an, aber die Abkuehlbuchhaltung wurde gerade zurueckgesetzt."""
        store = FakeStore(None)
        pm = self._autoscaled(2, store, depth=10_000)
        store.result = Setpoint(2, 8)
        setpoint.tick(pm)
        before = len(pm.workers)
        pm._autoscale_tick()
        assert len(pm.workers) == before

    def test_fixed_setpoint_below_the_start_count_is_not_undone_by_the_backfill(self):
        store = FakeStore(None)
        pm = _pm(n=6, store=store, max_processes=8)
        pm.SETPOINT_SAMPLE_INTERVAL = 0.0
        pm._start_all_workers()
        store.result = Setpoint(4, 4)
        setpoint.tick(pm)
        spawned = pm._ctx.Process.call_count
        assert pm._current_target() == 4 and pm._slots.occupied() == 6
        for info in list(pm.workers.values()):
            if info.expected_shutdown:
                info.process.is_alive.return_value = False
                info.process.exitcode = 0
        pm._check_dead_workers()
        assert pm._current_target() == 4 and pm._slots.occupied() == 4
        assert pm._ctx.Process.call_count == spawned  # nichts nachgefuellt

    def test_fixed_then_range_then_startup_moves_no_exit_condition(self):
        store = FakeStore(Setpoint(4, 4))
        pm = self._autoscaled(2, store)
        setpoint.tick(pm)
        store.result = Setpoint(2, 8)
        setpoint.tick(pm)
        store.result = None
        setpoint.tick(pm)
        for info in list(pm.workers.values()):
            if info.expected_shutdown:
                info.process.is_alive.return_value = False
                info.process.exitcode = 0
        pm._check_dead_workers()
        assert pm._clean_exits == 0 and pm._exhausted_slots == 0
        assert pm.initial_workers == 2 and pm._autoscale == (2, 4)

    def test_no_worker_starts_once_shutdown_began(self):
        """KTD2: der Durchlauf ruht im Herunterfahren -- es wird nicht einmal gelesen."""
        calls = []

        def factory():
            calls.append(1)
            return FakeStore(Setpoint(6, 8))

        pm = _pm(n=2, factory=factory, autoscale=(2, 4), queue_depth_fn=lambda: 0, max_processes=16)
        pm.SETPOINT_SAMPLE_INTERVAL = 0.0
        pm._start_all_workers()
        pm._shutting_down = True
        setpoint.tick(pm)
        assert calls == [] and len(pm.workers) == 2

    def test_sample_interval_gates_reads_also_after_a_failure(self):
        calls = []

        def factory():
            calls.append(1)
            return FakeStore(fail=True)

        pm = _pm(n=2, factory=factory, max_processes=8)
        pm.SETPOINT_SAMPLE_INTERVAL = 3600.0
        pm._start_all_workers()
        setpoint.tick(pm)
        setpoint.tick(pm)
        assert calls == [1]
        assert pm._setpoint.store == "unread"

    def test_autoscaler_scales_into_the_effective_bounds_not_the_start_argument(self):
        store = FakeStore(Setpoint(2, 8))
        pm = self._autoscaled(2, store, depth=10_000)
        setpoint.tick(pm)
        pm.AUTOSCALE_SAMPLE_INTERVAL = 0.0
        pm.SCALE_COOLDOWN_S = 0.0
        pm._last_scale_decision = float("-inf")
        pm._last_autoscale_sample = float("-inf")
        for _ in range(10):
            pm._autoscale_tick()
        assert len(pm.workers) == 8  # ueber die Startobergrenze 4 hinaus, bis zur wirksamen 8
