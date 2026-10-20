"""Autoskalierer-Maschine (swarmq 0.14.2, Plan 2026-10-20-2100, U2; E-084).

Subjekt: ein echter ``ProcessManager`` mit ``--autoscale MIN,MAX`` (und
optional ``--max-processes``) über gefälschten Prozessen (``MagicMock``,
keine Kinder). Die Regeln takten den Autoskalierer (``autoscale.tick``) mit
wechselnder Queue-Tiefe und treiben die Ereignisse, die Slots und Drains
erzeugen: Absturz mit Neustart (``_check_dead_workers`` -> ``_maybe_restart``,
Budget und Sperre nach E-024), Ende eines Drains (Exit 0 eines Prozesses mit
``expected_shutdown``), Recycling (Vorgänger draint, Ersatz im selben Slot,
wie ``_check_heartbeats``), Hang-Ersatz (``_replace_hung_worker``, mit
erschöpfbarem Budget) und das Aufheben abgelaufener Sperren. Mit Spielraum
(``--max-processes``) schreibt eine Regel Sollwerte des Kontrollkanals
(oder entfernt sie); sie laufen durch den echten ``setpoint.tick`` samt
``reconcile``, nur Lesen und Rückmelden am Speicher sind ersetzt.

Zeit: ``time`` in ``process_manager.supervisor``, ``process_manager.autoscale``
und ``process_manager.setpoint`` läuft auf einer ``FakeClock``. Logsätze des
Autoskalierers werden über einen Ersatz-Logger mitgeschrieben.

Recycling und Hang-Ersatz treffen hier nur Slots, die nicht schon im
Übergang sind; ein dritter Prozess im selben Slot (Ersatz des Ersatzes) ist
kein Thema des Autoskalierers.

Invarianten (Quelle E-084):

- A1 Prozesse <= Slot-Tabelle + Slots im Übergang: Der Autoskalierer startet
  nur in freie Slots.
- A2 Höchstens ein ``autoscale_down`` je PID (über das Log, nicht über
  ``terminate``, das auch Hang-Ersatz schickt): Ein drainender Worker wird
  nie erneut gewählt (F4-25).
- A3 Nach einem Abbau bleiben mindestens ``MIN`` (wirksame Untergrenze) Slots mit einem nicht
  drainenden Prozess (F4-26).
- A4 Nach einem Hochskalieren sind höchstens ``MAX`` (wirksame Obergrenze) Slots mit einem nicht
  drainenden Prozess besetzt.
"""

from __future__ import annotations

import multiprocessing
import types
from typing import Any, override
from unittest.mock import MagicMock, patch

from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from swarmq.contracts.control import Setpoint
from swarmq.process_manager import ProcessManager, setpoint
from swarmq.process_manager import autoscale as autoscale_module
from swarmq.process_manager import supervisor as supervisor_module
from swarmq.process_manager.slots import SlotState, WorkerInfo, heading_slots
from tests.unit.process_manager._helpers import FakeClock, _entry_stub, _fake_process, as_mock

_DEPTHS = st.sampled_from([0, 0, 0, 30, 120, 10_000])


class AutoscaleMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.clock = FakeClock()
        fake_time = types.SimpleNamespace(monotonic=self.clock, sleep=lambda _s: None)
        self.logger = MagicMock()
        self._patches: list[Any] = [
            patch.object(supervisor_module, "time", fake_time),
            patch.object(autoscale_module, "time", fake_time),
            patch.object(autoscale_module, "logger", self.logger),
            patch.object(setpoint, "time", fake_time),
            patch.object(setpoint, "read_setpoint_sync", lambda _factory, _group: self.stored),
            patch.object(setpoint, "write_status_sync", lambda *_a, **_kw: None),
        ]
        for p in self._patches:
            p.start()
        self.depth = 0
        self.stored: Setpoint | None = None
        self.pm: ProcessManager | None = None
        self.bounds = (1, 1)

    @override
    def teardown(self) -> None:
        for p in reversed(self._patches):
            p.stop()

    @initialize(
        bounds=st.tuples(st.integers(1, 3), st.integers(0, 3)).map(lambda b: (b[0], b[0] + b[1])),
        headroom=st.integers(0, 2),
        data=st.data(),
    )
    def start(self, bounds: tuple[int, int], headroom: int, data: st.DataObject) -> None:
        min_w, max_w = bounds
        n = data.draw(st.integers(min_w, max_w), label="workers")
        self.bounds = bounds
        pm = ProcessManager(
            num_workers=n,
            worker_entry=_entry_stub,
            autoscale=bounds,
            queue_depth_fn=lambda: self.depth,
            max_processes=max_w + headroom if headroom else None,
            setpoint_store_factory=MagicMock(),  # ersetzt: read_setpoint_sync/write_status_sync
            group="g",
        )
        pm.SETPOINT_SAMPLE_INTERVAL = 0.0  # die Regel bestimmt, wann gelesen wird
        pm._ctx = MagicMock()
        pids = iter(range(100, 100_000))
        pm._ctx.Process = MagicMock(side_effect=lambda **_kw: _fake_process(pid=next(pids)))
        pm._ctx.Value = multiprocessing.get_context("spawn").Value
        self.clock.now -= 100.0
        pm._start_all_workers()
        self.clock.now += 100.0
        self.pm = pm

    # ---- Hilfen

    def _pm(self) -> ProcessManager:
        assert self.pm is not None
        return self.pm

    def _heading(self) -> int:
        return len(heading_slots(self._pm().workers.values()))

    def _pick(self, index: int, *, draining: bool) -> WorkerInfo | None:
        pm = self._pm()
        candidates = sorted(
            (
                w
                for w in pm.workers.values()
                if w.expected_shutdown is draining and (draining or pm.slot_state(w.worker_id) is SlotState.ACTIVE)
            ),
            key=lambda w: (w.worker_id, w.spawned_at),
        )
        return candidates[index % len(candidates)] if candidates else None

    def _downs(self) -> list[Any]:
        return [c.kwargs["pid"] for c in self.logger.info.call_args_list if c.args == ("autoscale_down",)]

    # ---- Regeln

    @rule(depth=_DEPTHS, wait=st.sampled_from([0.5, 10.0, 30.0, 60.0]))
    def tick(self, depth: int, wait: float) -> None:
        """Ein Takt nach ``wait`` Sekunden (der Monitor taktet alle 0,5 s)."""
        self.clock.now += wait
        self.depth = depth
        downs_before = len(self._downs())
        ups_before = sum(1 for c in self.logger.info.call_args_list if c.args == ("autoscale_up",))
        autoscale_module.tick(self._pm())
        min_w, max_w = self._pm().effective_bounds
        if len(self._downs()) > downs_before:
            assert self._heading() >= min_w, f"A3: nach dem Abbau {self._heading()} nicht drainende < MIN={min_w}"
        if sum(1 for c in self.logger.info.call_args_list if c.args == ("autoscale_up",)) > ups_before:
            assert self._heading() <= max_w, f"A4: nach dem Hochskalieren {self._heading()} aktiv > MAX={max_w}"

    @rule()
    def long_pause(self) -> None:
        """Zehn Minuten ohne Takt (Sperren laufen ab, Prozesse altern)."""
        self.clock.now += 600.0

    @rule(index=st.integers(0, 7))
    def crash(self, index: int) -> None:
        info = self._pick(index, draining=False)
        if info is None:
            return
        as_mock(info.process).is_alive.return_value = False
        as_mock(info.process).exitcode = 1
        self._pm()._check_dead_workers()

    @rule(index=st.integers(0, 7))
    def finish_drain(self, index: int) -> None:
        info = self._pick(index, draining=True)
        if info is None:
            return
        as_mock(info.process).is_alive.return_value = False
        as_mock(info.process).exitcode = 0
        self._pm()._check_dead_workers()

    @rule(index=st.integers(0, 7))
    def recycle(self, index: int) -> None:
        info = self._pick(index, draining=False)
        if info is None:
            return
        info.expected_shutdown = True
        self._pm()._start_worker(info.worker_id)

    @rule(index=st.integers(0, 7))
    def hang(self, index: int) -> None:
        info = self._pick(index, draining=False)
        if info is None:
            return
        pm = self._pm()
        pm._replace_hung_worker(info, stale_s=pm.heartbeat_timeout_s + 1.0)

    @rule(low=st.integers(1, 5), span=st.integers(0, 4), clear=st.booleans())
    def write_setpoint(self, low: int, span: int, clear: bool) -> None:
        """Kontrollkanal: Sollwert schreiben oder entfernen, dann ein Durchlauf."""
        pm = self._pm()
        if pm.max_processes is None:
            return
        headroom = int(pm.max_processes)
        low = min(low, headroom)
        self.stored = None if clear else Setpoint(low, min(headroom, low + span))
        setpoint.tick(pm)

    @rule()
    def unlock_expired(self) -> None:
        self._pm()._unlock_expired_slots()

    # ---- Invarianten

    @invariant()
    def processes_fit_the_slot_table(self) -> None:
        if self.pm is None:
            return
        pm = self.pm
        transition = sum(1 for slot in range(len(pm._slots)) if pm.slot_state(slot) is SlotState.TRANSITION)
        assert len(pm.workers) <= len(pm._slots) + transition, (
            f"A1: {len(pm.workers)} Prozesse > {len(pm._slots)} Slots + {transition} im Übergang"
        )

    @invariant()
    def at_most_one_scale_down_per_pid(self) -> None:
        downs = self._downs() if self.pm is not None else []
        assert len(downs) == len(set(downs)), f"A2: autoscale_down mehrfach für eine PID: {downs}"
