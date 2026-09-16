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
