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
- Plan 2026-10-06-0831, U9: die Ablehnung kommt ueber den Bibliotheks-Logger
  als ``setpoint_rejected`` (WARNING) mit Gruppe, Sollwert und Grund an.
"""

from __future__ import annotations

import multiprocessing
import types
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from swarmq.contracts.control import Setpoint, SetpointUnreadable
from swarmq.process_manager import ProcessManager, setpoint
from swarmq.process_manager import autoscale as autoscale_module
from swarmq.process_manager import supervisor as supervisor_module
from swarmq.process_manager.slots import SlotState
from tests.unit.process_manager._helpers import FakeClock, _entry_stub, _fake_process, as_mock


class TestValidate:
    def test_setpoint_within_headroom_is_accepted(self):
        assert setpoint.validate(Setpoint(2, 8), headroom=16, autoscaling=True) is None
        assert setpoint.validate(Setpoint(4, 4), headroom=16, autoscaling=False) is None

    def test_upper_bound_above_headroom_names_the_headroom(self):
        reason = setpoint.validate(Setpoint(2, 12), headroom=8, autoscaling=True)
        assert reason is not None
        assert "headroom 8" in reason

    @pytest.mark.parametrize("sp", [Setpoint(0, 4), Setpoint(-1, 4), Setpoint(6, 2)])
    def test_lower_bound_below_one_and_inverted_bounds_are_rejected(self, sp):
        assert setpoint.validate(sp, headroom=16, autoscaling=True) is not None

    def test_range_without_autoscaling_demands_the_fixed_form(self):
        """Covers AE11 (Rechen-Pool) und AE13 (Autoskalierung aus): beide laufen ohne Autoskalierung."""
        reason = setpoint.validate(Setpoint(2, 6), headroom=8, autoscaling=False)
        assert reason is not None
        assert "N..N" in reason

    @pytest.mark.parametrize("sp", [Setpoint(1, 8), Setpoint(8, 8)])
    def test_bounds_exactly_at_one_and_at_the_headroom_are_accepted(self, sp):
        """T-F-19 (Teil ``setpoint.validate``), Mutant ``:36`` ``>`` -> ``>=``:
        Die Obergrenze darf den Spielraum erreichen, die Untergrenze eins."""
        assert setpoint.validate(sp, headroom=8, autoscaling=True) is None


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Virtuelle Uhr fuer Supervisor, Autoskalierer und Sollwert-Durchlauf zugleich."""
    fake = FakeClock()
    fake_time = types.SimpleNamespace(monotonic=fake, sleep=lambda _s: None)
    for module in (supervisor_module, autoscale_module, setpoint):
        monkeypatch.setattr(module, "time", fake_time)
    return fake


class FakeStore:
    """Sollwert-Speicher fuer die Fabrik-Seam: liefert ein festes Ergebnis oder scheitert."""

    def __init__(self, result=None, *, fail=False, read_fail=False):
        self.result = result
        self.fail = fail
        self.read_fail = read_fail
        self.status_writes: list[dict[str, Any]] = []
        self.status_ttls: list[float] = []
        self.status_keys: list[tuple[str, str]] = []
        self.reads = 0
        self.read_groups: list[str] = []

    async def connect(self):
        if self.fail:
            raise ConnectionRefusedError("backend down")

    async def close(self):
        pass

    async def read_setpoint(self, group):
        self.reads += 1
        self.read_groups.append(group)
        if self.read_fail:
            raise ConnectionRefusedError("backend down")
        return self.result

    async def write_status(self, group, supervisor_id, status, *, ttl_s):
        self.status_keys.append((group, supervisor_id))
        self.status_writes.append(dict(status))
        self.status_ttls.append(ttl_s)


def _pm(n: int = 2, *, store=None, factory=None, **kw) -> ProcessManager:
    """Supervisor mit gefaelschten Prozessen; PID = 100 * Generation + Slot."""
    if factory is None and store is not None:
        factory = lambda: store  # noqa: E731
    pm = ProcessManager(
        num_workers=n,
        worker_entry=_entry_stub,
        setpoint_store_factory=factory,
        supervisor_id="host:p1",
        group="orders",
        **kw,
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
        assert pm._setpoint.rejection is not None
        assert "headroom 4" in pm._setpoint.rejection

    def test_rejection_is_logged_as_warning_with_reason(self, swarmq_logs):
        pm = _pm(n=2, store=FakeStore(Setpoint(2, 8)), autoscale=(2, 4), queue_depth_fn=lambda: 0, max_processes=4)
        setpoint.read_at_start(pm)
        [entry] = [e for e in swarmq_logs if e["event"] == "setpoint_rejected"]
        assert entry["log_level"] == "warning"
        assert entry["logger"] == "swarmq.process_manager.setpoint"
        assert entry["group"] == pm.group
        assert entry["setpoint"] == [2, 8]
        assert "headroom 4" in entry["reason"]

    def test_unreachable_store_keeps_the_start_arguments_and_marks_unread(self):
        """Covers AE7 (erster Teil)."""
        pm = _pm(n=2, store=FakeStore(fail=True), max_processes=8)
        setpoint.read_at_start(pm)
        pm._start_all_workers()
        assert pm.effective_bounds == (2, 2)
        assert len(pm.workers) == 2
        assert pm._setpoint.store == "unread"

    def test_unreadable_setpoint_is_treated_like_a_rejection(self):
        pm = _pm(n=2, store=FakeStore(SetpointUnreadable("not json")), max_processes=8)
        setpoint.read_at_start(pm)
        pm._start_all_workers()
        assert pm.effective_bounds == (2, 2)
        assert len(pm.workers) == 2
        assert pm._setpoint.rejection is not None
        assert "unreadable" in pm._setpoint.rejection

    def test_range_setpoint_on_a_group_without_autoscaling_is_rejected(self):
        """Covers AE11/AE13: feste Prozesszahl (auch Rechen-Pool) nimmt nur N..N."""
        pm = _pm(n=4, store=FakeStore(Setpoint(2, 6)), max_processes=8)
        setpoint.read_at_start(pm)
        assert pm.effective_bounds == (4, 4)
        assert pm._setpoint.rejection is not None
        assert "N..N" in pm._setpoint.rejection


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
        pm = _pm(
            n=n,
            store=store,
            autoscale=kw.pop("autoscale", (2, 4)),
            queue_depth_fn=lambda: depth,
            max_processes=kw.pop("max_processes", 16),
            **kw,
        )
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
        assert len(_live_slots(pm)) == 4
        assert len(_drained(pm)) == 2
        for info in pm.workers.values():
            if info.expected_shutdown:
                as_mock(info.process).terminate.assert_called_once()
                as_mock(info.process).kill.assert_not_called()

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
        assert pm.slot_state(1) is SlotState.TRANSITION
        assert pm.slot_state(2) is SlotState.TRANSITION
        store.result = Setpoint(1, 1)
        setpoint.tick(pm)
        # Nur Slot 0 war waehlbar: Ziel 1 noch nicht erreicht.
        assert _live_slots(pm) == [1, 2]
        for slot in (1, 2):  # Vorgaenger enden, Uebergang loest sich
            as_mock(pm.workers[old[slot]].process).is_alive.return_value = False
            as_mock(pm.workers[old[slot]].process).exitcode = 0
        pm._check_dead_workers()
        setpoint.tick(pm)
        assert len(_live_slots(pm)) == 1

    def test_removed_setpoint_restores_the_start_arguments(self):
        """Covers AE9: Sollwert 4..4 wirksam, Deployment 2..4, Sollwert entfernt -> 2..4 aus den Startargumenten."""
        store = FakeStore(Setpoint(4, 4))
        pm = self._autoscaled(2, store)
        setpoint.tick(pm)
        assert pm.effective_bounds == (4, 4)
        assert len(_live_slots(pm)) == 4
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
        spawned = as_mock(pm._ctx).Process.call_count
        assert pm._current_target() == 4
        assert pm._slots.occupied() == 6
        for info in list(pm.workers.values()):
            if info.expected_shutdown:
                as_mock(info.process).is_alive.return_value = False
                as_mock(info.process).exitcode = 0
        pm._check_dead_workers()
        assert pm._current_target() == 4
        assert pm._slots.occupied() == 4
        assert as_mock(pm._ctx).Process.call_count == spawned  # nichts nachgefuellt

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
                as_mock(info.process).is_alive.return_value = False
                as_mock(info.process).exitcode = 0
        pm._check_dead_workers()
        assert pm._clean_exits == 0
        assert pm._exhausted_slots == 0
        assert pm.initial_workers == 2
        assert pm._autoscale == (2, 4)

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
        assert calls == []
        assert len(pm.workers) == 2

    def test_sample_interval_gates_reads_also_after_a_failure(self):
        store = FakeStore(read_fail=True)
        pm = _pm(n=2, store=store, max_processes=8)
        pm.SETPOINT_SAMPLE_INTERVAL = 3600.0
        pm._start_all_workers()
        setpoint.tick(pm)
        setpoint.tick(pm)
        assert store.reads == 1  # der Rest des Intervalls wird uebersprungen (KTD5)
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


class TestStatus:
    """U7: Rueckmeldung des Supervisors (R14, R15).

    Test-Liste:
    - Die Rueckmeldung nennt den wirksamen Gruppenschluessel und den Supervisor.
    - Ohne hinterlegten Sollwert weist sie die Startargumente als Herkunft aus.
    - Nach einer Ablehnung traegt sie den Grund und die weiterhin wirksamen alten Grenzen (AE5).
    - Bei nicht erreichbarem Speicher weist sie den Sollwert als ungelesen aus (AE7).
    - Gesperrte Slots erscheinen mit ihrer Zahl (KTD9).
    - Das Verfallsdatum deckt Gnadenfrist, Terminierungsfrist und ein Intervall (KTD7).
    - Schon der Start schreibt eine Rueckmeldung (F1).
    """

    def _pm(self, store: FakeStore, n: int = 2, **kw) -> ProcessManager:
        pm = _pm(n=n, store=store, max_processes=kw.pop("max_processes", 8), **kw)
        pm.SETPOINT_SAMPLE_INTERVAL = 0.0
        pm._start_all_workers()
        return pm

    def test_status_names_group_and_supervisor(self):
        store = FakeStore(None)
        pm = self._pm(store)
        setpoint.tick(pm)
        assert store.status_keys == [("orders", "host:p1")]
        assert store.status_writes[-1]["group"] == "orders"
        assert store.status_writes[-1]["supervisor"] == "host:p1"

    def test_without_setpoint_the_start_arguments_are_the_source(self):
        store = FakeStore(None)
        pm = self._pm(store)
        setpoint.tick(pm)
        status = store.status_writes[-1]
        assert status["store"] == "absent"
        assert status["setpoint"] is None
        assert status["effective"] == {"min": 2, "max": 2}
        assert status["source"] == "startup"
        assert status["rejection"] is None
        assert status["headroom"] == 8

    def test_rejection_carries_the_reason_and_the_still_effective_bounds(self):
        """Covers AE5: Spielraum 8, wirksam 2..6, Sollwert 2..12 -> bleibt 2..6, Grund nennt den Spielraum."""
        store = FakeStore(Setpoint(2, 6))
        pm = self._pm(store, autoscale=(2, 4), queue_depth_fn=lambda: 0)
        setpoint.tick(pm)
        store.result = Setpoint(2, 12)
        setpoint.tick(pm)
        status = store.status_writes[-1]
        assert status["effective"] == {"min": 2, "max": 6}
        assert status["source"] == "setpoint"
        assert status["setpoint"] == {"min": 2, "max": 12}
        assert "headroom 8" in status["rejection"]

    def test_unreachable_store_is_reported_as_unread_once_it_answers_again(self):
        """Covers AE7: waehrend der Speicher schweigt, gelten die Startargumente; die Rueckmeldung
        (sobald sie ankommt) weist den Sollwert als ungelesen aus."""
        store = FakeStore(Setpoint(4, 4), read_fail=True)
        pm = self._pm(store)
        setpoint.tick(pm)
        status = store.status_writes[-1]
        assert status["store"] == "unread"
        assert status["source"] == "startup"
        assert status["effective"] == {"min": 2, "max": 2}
        store.read_fail = False
        setpoint.tick(pm)
        status = store.status_writes[-1]
        assert status["store"] == "present"
        assert status["effective"] == {"min": 4, "max": 4}

    def test_locked_slots_are_counted(self):
        store = FakeStore(None)
        pm = self._pm(store)
        pm._slots.lock(5)  # ein freier Slot mit erschoepftem Budget (KTD9)
        setpoint.tick(pm)
        assert store.status_writes[-1]["slots"] == {"size": 8, "occupied": 2, "locked": 1}

    def test_ttl_covers_grace_term_and_one_interval(self):
        store = FakeStore(None)
        pm = self._pm(store, shutdown_grace_s=45.0)
        setpoint.tick(pm)
        assert store.status_ttls[-1] == 45.0 + pm.SHUTDOWN_TERM_S + pm.SETPOINT_SAMPLE_INTERVAL
        assert store.status_writes[-1]["ttl_s"] == store.status_ttls[-1]

    def test_start_already_reports(self):
        store = FakeStore(Setpoint(3, 3))
        pm = _pm(n=2, store=store, max_processes=8)
        setpoint.read_at_start(pm)
        assert store.status_writes[-1]["effective"] == {"min": 3, "max": 3}
        assert store.status_writes[-1]["workers"] == 0  # vor dem ersten Kind

    def test_ttl_outlasts_grace_and_term_with_the_default_interval(self):
        """T-F-06, Mutant ``:210`` ``+ SETPOINT_SAMPLE_INTERVAL`` -> ``-``: Die Meldung
        eines Supervisors im geordneten Abbau verfaellt nicht vor dessen Ende.

        Bewusst nur relational und ohne Marker: die genaue Formel ist ein
        offener Punkt in E-018."""
        store = FakeStore(None)
        pm = _pm(n=2, store=store, max_processes=8)
        setpoint.read_at_start(pm)
        assert pm.SETPOINT_SAMPLE_INTERVAL > 0
        assert store.status_ttls[-1] > pm.shutdown_grace_s + pm.SHUTDOWN_TERM_S

    @pytest.mark.parametrize(
        ("autoscale", "stored", "expected"),
        [
            ((2, 4), None, True),
            ((2, 4), Setpoint(3, 3), False),
            (None, None, False),
        ],
        ids=["autoscaled-range", "autoscaled-but-fixed-setpoint", "fixed-group"],
    )
    def test_autoscaling_is_reported_only_for_an_autoscaled_range(self, autoscale, stored, expected):
        """T-F-07, Mutant ``:229`` ``and low != high`` -> ``or``: Ein fester Sollwert
        legt die Autoskalierung still, auch wenn die Gruppe sie eingeschaltet hat."""
        store = FakeStore(stored)
        kw: dict[str, Any] = {} if autoscale is None else {"autoscale": autoscale, "queue_depth_fn": lambda: 0}
        pm = self._pm(store, **kw)
        setpoint.tick(pm)
        assert store.status_writes[-1]["autoscaling"] is expected

    def test_reported_at_is_the_current_time_in_utc(self):
        """T-F-07, Mutant ``:233`` ``datetime.now(UTC)`` -> ``datetime.now()``: Die
        Zeitangabe traegt die Zone UTC, sonst ist sie zwischen Hosts nicht vergleichbar."""
        store = FakeStore(None)
        pm = self._pm(store)
        setpoint.tick(pm)
        reported = datetime.fromisoformat(store.status_writes[-1]["reported_at"])
        assert reported.utcoffset() == timedelta(0)
        assert abs(reported - datetime.now(UTC)) < timedelta(minutes=1)


def _autoscaled_at(clock: FakeClock, n: int, store: FakeStore, *, depth: list[int], **kw: Any) -> ProcessManager:
    """Autoskalierte Gruppe 2..4 (Spielraum 16) unter virtueller Uhr; Abtast-,
    Abkuehl- und Leerlauffristen bleiben auf ihren Vorgaben. Die Worker sind
    zum Zeitpunkt ``clock.now`` schon 100 s alt."""
    clock.now -= 100.0
    pm = _pm(
        n=n,
        store=store,
        autoscale=kw.pop("autoscale", (2, 4)),
        queue_depth_fn=lambda: depth[0],
        max_processes=16,
        **kw,
    )
    pm._start_all_workers()
    clock.now += 100.0
    return pm


def _terminations(pm: ProcessManager) -> int:
    """SIGTERMs an alle noch eingetragenen Prozesse (sanftes Ende)."""
    return sum(as_mock(w.process).terminate.call_count for w in pm.workers.values())


class TestApplyResult:
    """T-F-05: Nur eine Aenderung der wirksamen Grenzen setzt die Buchhaltung
    des Autoskalierers zurueck (KTD3). Beobachtet wird der Autoskalier-Takt in
    derselben Runde bei Rueckstau 10 000.

    Test-Liste:
    - Ungelesen, fehlend (ohne vorherigen Sollwert), unlesbar, abgelehnt: der
      Autoskalierer skaliert in derselben Runde hoch.
    - Derselbe Sollwert ein zweites Mal gelesen: ebenso.
    - Entfernter Sollwert mit Grenzen, bei denen ``reconcile`` nichts tut: der
      Autoskalierer entscheidet in dieser Runde nicht.
    Kein Test auf „Sollwert gleich den Startgrenzen“: der liefert bewusst
    ``True`` (Quellenwechsel).
    """

    @pytest.mark.parametrize(
        "store_kw",
        [
            {"fail": True},
            {"result": None},
            {"result": SetpointUnreadable("not json")},
            {"result": Setpoint(2, 32)},
        ],
        ids=["unread", "absent", "unreadable", "rejected"],
    )
    def test_a_read_without_change_leaves_the_autoscaler_free_in_the_same_round(self, clock, store_kw):
        """Mutanten ``return False`` -> ``return True`` in ``apply`` ``:96`` (unread),
        ``:106`` (absent), ``:112`` (unreadable), ``:118`` (rejected)."""
        depth = [10_000]
        pm = _autoscaled_at(clock, 2, FakeStore(**store_kw), depth=depth)
        setpoint.tick(pm)
        pm._autoscale_tick()
        assert len(pm.workers) == 3

    def test_the_same_setpoint_read_a_second_time_leaves_the_autoscaler_free(self, clock):
        """Mutant ``:122`` ``return False`` -> ``return True``: derselbe Sollwert ein
        zweites Mal gelesen ist keine Aenderung."""
        depth = [10_000]
        store = FakeStore(Setpoint(2, 8))
        pm = _autoscaled_at(clock, 2, store, depth=depth)
        setpoint.tick(pm)
        clock.now += 60.0
        setpoint.tick(pm)
        assert store.reads == 2
        pm._autoscale_tick()
        assert len(pm.workers) == 3

    def test_a_removed_setpoint_is_a_change_even_when_reconcile_does_nothing(self, clock):
        """Mutant ``:105`` ``return True`` -> ``return False``: 2..8 -> entfernt -> 2..4
        bei zwei Workern; ``reconcile`` tut nichts, die Grenzen haben sich aber geaendert."""
        depth = [10_000]
        store = FakeStore(Setpoint(2, 8))
        pm = _autoscaled_at(clock, 2, store, depth=depth)
        setpoint.tick(pm)
        clock.now += 60.0
        store.result = None
        setpoint.tick(pm)
        assert pm.effective_bounds == (2, 4)
        pm._autoscale_tick()
        assert len(pm.workers) == 2


class TestFollowUpRounds:
    """T-F-10 (Sollwert-Teil): die Takte nach dem Lesen beim Start und nach
    einer Aenderung, mit den Vorgabefristen unter virtueller Uhr.

    Test-Liste:
    - Nach dem Lesen beim Start liest der Durchlauf erst nach einem Intervall wieder.
    - Nach einer Aenderung wartet der Autoskalierer die volle Abkuehlzeit ab.
    - Nach einer Aenderung beginnt die Leerlauffrist von vorn.
    """

    def test_the_first_pass_after_the_start_read_waits_one_interval(self, clock):
        """Mutant ``:84`` ``last_sample = time.monotonic()`` -> ``= None``."""
        store = FakeStore(None)
        pm = _pm(n=2, store=store, max_processes=8)
        setpoint.read_at_start(pm)
        clock.now += pm.SETPOINT_SAMPLE_INTERVAL / 2
        setpoint.tick(pm)
        assert store.reads == 1
        clock.now += pm.SETPOINT_SAMPLE_INTERVAL / 2
        setpoint.tick(pm)
        assert store.reads == 2

    def test_after_a_change_the_autoscaler_waits_the_full_cooldown(self, clock):
        """Mutant ``:147`` ``_last_scale_decision = now`` -> ``= None`` (endet im
        zweiten Takt in ``TypeError``)."""
        depth = [10_000]
        store = FakeStore(Setpoint(2, 8))
        pm = _autoscaled_at(clock, 2, store, depth=depth)
        setpoint.tick(pm)
        clock.now += pm.AUTOSCALE_SAMPLE_INTERVAL
        assert pm.AUTOSCALE_SAMPLE_INTERVAL < pm.SCALE_COOLDOWN_S
        pm._autoscale_tick()
        assert len(pm.workers) == 2  # Abtastung faellig, Abkuehlzeit noch nicht um
        clock.now += pm.SCALE_COOLDOWN_S - pm.AUTOSCALE_SAMPLE_INTERVAL
        pm._autoscale_tick()
        assert len(pm.workers) == 3

    def test_after_a_change_the_idle_window_starts_over(self, clock):
        """Mutant ``:149`` ``_idle_since = None`` -> ``= ""`` (``TypeError`` im
        naechsten Leerlauf-Takt); auch das Streichen der Zeile wird rot."""
        depth = [0]
        store = FakeStore(None)
        pm = _autoscaled_at(clock, 3, store, depth=depth)
        pm._autoscale_tick()  # Leerlauf beginnt jetzt
        clock.now += 10.0
        store.result = Setpoint(2, 8)
        setpoint.tick(pm)  # Aenderung: Buchhaltung zurueck
        clock.now += pm.SCALE_COOLDOWN_S
        pm._autoscale_tick()  # Leerlauf beginnt neu
        clock.now += pm.SCALE_DOWN_IDLE_S / 2
        pm._autoscale_tick()
        assert _terminations(pm) == 0  # 70 s nach dem ersten Leerlauf, aber erst 30 s nach dem Neubeginn
        clock.now += pm.SCALE_DOWN_IDLE_S / 2
        pm._autoscale_tick()
        assert _terminations(pm) == 1


class TestReconcile:
    """T-F-21: Auswahl beim Abbau, Untergrenze ohne Autoskalierer, ``acted``.

    Test-Liste:
    - Ein gesenkter Sollwert beendet den juengsten Worker nach Startzeit, nicht den hoechsten Slot.
    - Ohne Autoskalierer startet der Durchlauf einen sauber beendeten Slot nicht neu.
    - Ein Abbau ohne Grenzaenderung setzt die Autoskalier-Buchhaltung zurueck.
    - Ein Nachstarten ohne Grenzaenderung setzt sie ebenfalls zurueck.
    """

    def test_a_lowered_setpoint_drains_the_youngest_worker_by_start_time(self):
        """Mutant ``:167`` ``max(`` -> ``min(``: Slot 0 traegt nach einem Ersatz den
        juengsten Prozess, er geht zuerst."""
        store = FakeStore(None)
        pm = _pm(n=4, store=store, max_processes=8)
        pm.SETPOINT_SAMPLE_INTERVAL = 0.0
        pm._start_all_workers()
        ages = {0: 400.0, 1: 100.0, 2: 300.0, 3: 200.0}
        for info in pm.workers.values():
            info.spawned_at = ages[info.worker_id]
        store.result = Setpoint(3, 3)
        setpoint.tick(pm)
        assert _drained(pm) == [0]

    def test_without_autoscaler_a_cleanly_exited_slot_is_not_restarted(self):
        """Mutant ``:185`` ``floor = low if ... else pm._current_target()`` -> ``floor = low``."""
        store = FakeStore(None)
        pm = _pm(n=4, store=store, max_processes=8)
        pm.SETPOINT_SAMPLE_INTERVAL = 0.0
        pm._start_all_workers()
        [finished] = [w for w in pm.workers.values() if w.worker_id == 3]
        as_mock(finished.process).is_alive.return_value = False
        as_mock(finished.process).exitcode = 0
        pm._check_dead_workers()
        spawned = as_mock(pm._ctx).Process.call_count
        setpoint.tick(pm)
        assert _live_slots(pm) == [0, 1, 2]
        assert as_mock(pm._ctx).Process.call_count == spawned

    def test_a_drain_without_a_bound_change_resets_the_autoscaler(self, clock):
        """Mutant ``:189`` ``acted = True`` -> ``False``: der zweite Durchlauf mit
        demselben Sollwert 1..1 zieht den Abbau nach; der Autoskalierer, dessen
        Leerlauffrist schon um ist, beendet in derselben Runde keinen weiteren."""
        depth = [0]
        store = FakeStore(None)
        pm = _autoscaled_at(clock, 3, store, depth=depth, autoscale=(1, 8))
        old = {w.worker_id: pid for pid, w in pm.workers.items()}
        for slot in (1, 2):  # Slots 1 und 2 im Uebergang
            pm.workers[old[slot]].expected_shutdown = True
            pm._start_worker(slot)
        clock.now += 100.0  # Ersatzprozesse sind alt genug fuer den Autoskalierer
        store.result = Setpoint(1, 1)
        setpoint.tick(pm)  # Aenderung; nur Slot 0 war waehlbar
        assert _drained(pm) == [0, 1, 2]
        for pid in (old[0], old[1], old[2]):
            as_mock(pm.workers[pid].process).is_alive.return_value = False
            as_mock(pm.workers[pid].process).exitcode = 0
        pm._check_dead_workers()
        assert _live_slots(pm) == [1, 2]
        clock.now += pm.SCALE_COOLDOWN_S + pm.AUTOSCALE_SAMPLE_INTERVAL
        pm._autoscale_tick()  # Leerlauf beginnt
        clock.now += pm.SCALE_DOWN_IDLE_S + pm.AUTOSCALE_SAMPLE_INTERVAL
        setpoint.tick(pm)  # derselbe Sollwert: nur der Abbau
        assert pm._setpoint.source == "setpoint"
        assert _terminations(pm) == 1
        pm._autoscale_tick()
        assert _terminations(pm) == 1

    def test_a_restart_without_a_bound_change_resets_the_autoscaler(self, clock):
        """Mutant ``:197`` ``acted = True`` -> ``False``: ein sauber beendeter Worker
        einer autoskalierten Gruppe wird bis zur Untergrenze nachgestartet; der
        Autoskalierer entscheidet trotz Rueckstau in derselben Runde nicht."""
        depth = [10_000]
        store = FakeStore(Setpoint(3, 8))
        pm = _autoscaled_at(clock, 3, store, depth=depth)
        setpoint.tick(pm)
        assert len(pm.workers) == 3
        [finished] = [w for w in pm.workers.values() if w.worker_id == 2]
        as_mock(finished.process).is_alive.return_value = False
        as_mock(finished.process).exitcode = 0
        pm._check_dead_workers()
        assert _live_slots(pm) == [0, 1]
        clock.now += 60.0
        setpoint.tick(pm)  # derselbe Sollwert: nur das Nachstarten
        assert _live_slots(pm) == [0, 1, 2]
        pm._autoscale_tick()
        assert len(pm.workers) == 3


class TestAutoscalerDuringDrain:
    """E-084 (Plan 2026-10-20-2100, U2): Sollwert-Durchlauf und Autoskalierer
    zählen im selben Monitor-Durchlauf gleich, auch während ein Worker draint.

    Test-Liste:
    - Der Sollwert hebt die Untergrenze, während der Autoskalierer einen Worker
      drainen lässt: Der Durchlauf füllt bis ``low`` auf, der Autoskalierer
      baut danach nicht gegen ihn ab (kein Hin und Her über viele Durchläufe).
      Nach dem Fix geschrieben; hält das Verhalten fest und ist gegen be0e257 rot.
    """

    @pytest.mark.entscheidung("E-084")
    def test_raised_lower_bound_during_a_drain_is_not_undone_by_the_autoscaler(self, clock, monkeypatch):
        depth = [0]
        store = FakeStore(None)
        pm = _autoscaled_at(clock, 3, store, depth=depth, autoscale=(1, 5))
        pm._autoscale_tick()  # Leerlauf beginnt
        clock.now += pm.SCALE_DOWN_IDLE_S
        pm._autoscale_tick()
        assert _drained(pm) == [2]  # Slot 2 draint und hält seinen Slot
        store.result = Setpoint(3, 5)
        passes = []

        def beat_all() -> None:
            for info in pm.workers.values():  # alle leben und melden sich, keiner hängt
                assert info.channel is not None
                info.channel.heartbeat.value = clock.now

        def one_pass(_seconds: float) -> None:
            passes.append(1)
            clock.now += pm.SETPOINT_SAMPLE_INTERVAL
            beat_all()
            if len(passes) * pm.SETPOINT_SAMPLE_INTERVAL >= 3 * (pm.SCALE_COOLDOWN_S + pm.SCALE_DOWN_IDLE_S):
                pm._shutting_down = True

        monkeypatch.setattr(supervisor_module, "time", types.SimpleNamespace(monotonic=clock, sleep=one_pass))
        beat_all()

        pm._monitor_loop()

        assert pm.effective_bounds == (3, 5)
        assert _live_slots(pm) == [0, 1, 3]
        assert _drained(pm) == [2]
        for info in pm.workers.values():
            assert as_mock(info.process).terminate.call_count == (1 if info.worker_id == 2 else 0)


class TestGroupKey:
    """T-F-22: gelesen wird der Sollwert der eigenen Gruppe, beim Start und im Takt."""

    def test_start_read_and_pass_read_the_own_group(self):
        """Mutanten ``:85`` und ``:144`` ``pm.group`` -> ``None``."""
        store = FakeStore(None)
        pm = _pm(n=2, store=store, max_processes=8)
        pm.SETPOINT_SAMPLE_INTERVAL = 0.0
        setpoint.read_at_start(pm)
        setpoint.tick(pm)
        assert store.read_groups == ["orders", "orders"]
