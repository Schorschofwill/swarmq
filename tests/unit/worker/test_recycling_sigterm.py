"""SIGTERM waehrend eines Recycling-Drains (FS-30, Plan 2026-10-14-0748, U9, KTD4).

Ein Recycling-Drain wartet ohne erzwungenen Abbruch seine eigene Frist ab
(E-038), die mit FS-30 bis ``consumer_timeout_s`` reichen kann. Ein SIGTERM
(Deploy, Supervisor-Abbau) setzt ab diesem Signal das Shutdown-Budget in Kraft
(E-018): Drain-Ende und Publish-Endfrist werden auf
``min(bisher, SIGTERM + drain_until)`` gezogen, nur nach vorn. Das zweite
SIGTERM des Supervisors ankert nicht neu, und ``request_recycle`` selbst
unterbricht den Drain nicht.

Zeit ist skaliert (wie ``test_recycling_drain_end.py``); die SIGTERM-Zeitpunkte
kommen per ``call_later`` statt per Sleep.

Testliste:
- [x] SIGTERM im Recycling-Drain: Abbruch bei SIGTERM + drain_until, Fehlerpfad
      nach E-001, Sperre frei und Leader uebergeben vor SIGTERM + finish_by
- [x] zweites SIGTERM verschiebt den Anker nicht; die Endfrist wird nie spaeter
- [x] SIGTERM zwischen Recycling-Anfrage und Drain-Beginn: Drain-Ende ab SIGTERM
- [x] SIGTERM, waehrend request_recycle auf die Bestaetigung wartet: kein
      Recycling-Override, es gilt das Budget
- [x] Recycling-Drain ohne SIGTERM laesst keinen Hilfstask zurueck
- [x] Anker nur nach vorn: spaetes SIGTERM verlaengert einen kurzen
      Recycling-Drain nicht; SIGTERM vor einem kurzen Drain behaelt dessen Ende
- [x] SIGTERM im Ack-Warten ohne Bestaetigung: kein recycle_ack_missing (Exit 75)
- [x] SIGTERM im Schlaf des Recycling-Monitors: kein Recycling-Antrag
- [x] SIGTERM, waehrend ``cancel_consume`` im Recycling-Drain haengt: der Schritt
      endet bei SIGTERM + drain_until, die Endfrist folgt (Review U10)
(request_recycle selbst setzt keinen Anker: Nachweis FS-30 mit shutdown_timeout 0;
bis zum SIGTERM gilt die Endfrist aus U3: test_recycling_drain_end.py)
"""

from __future__ import annotations

import asyncio
import multiprocessing
import uuid
from collections.abc import Iterator
from typing import Any, override
from unittest.mock import MagicMock

import pytest

from swarmq import supervision as sv
from swarmq.contracts.coordination import LockToken
from swarmq.shutdown_budget import shutdown_budget
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.worker import WorkerState, recycling
from swarmq.worker import lifecycle as _lc
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_EPS = 0.005


class _Probe:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled_at: list[float] = []


_PROBES: list[_Probe] = []


class _RecyclingSigtermRunner(Task):
    """Ohne ``hard_timeout``; laeuft, bis er abgebrochen wird."""

    task_info = TaskInfo(name="recycling_sigterm_runner", queues=["default"])

    @override
    async def run(self) -> str:
        probe = _PROBES[-1]
        probe.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            probe.cancelled_at.append(asyncio.get_running_loop().time())
            raise
        return "nie"


@pytest.fixture
def no_supervisor() -> Iterator[None]:
    sv.set_current_channel(None)
    yield
    sv.set_current_channel(None)


def _sigterm_at(worker: Any, delay: float, times: list[float]) -> None:
    """SIGTERM nach *delay* s (der Signal-Handler ist ``_request_shutdown``)."""
    loop = asyncio.get_running_loop()

    def _fire() -> None:
        times.append(loop.time())
        worker._request_shutdown()

    loop.call_later(delay, _fire)


def _hang(worker: Any) -> asyncio.Task[bool]:
    task = asyncio.create_task(asyncio.Event().wait())
    worker._active_tasks.add(task)
    task.add_done_callback(worker._active_tasks.discard)
    return task


def _deadlines(broker: Any) -> list[float | None]:
    return [call.args[0] for call in broker.set_publish_deadline.call_args_list]


@pytest.mark.entscheidung("E-018")
async def test_sigterm_cuts_the_recycling_drain_with_the_budget_from_the_signal(
    worker: Any, broker: Any, no_supervisor: None
) -> None:
    loop = asyncio.get_running_loop()
    probe = _Probe()
    _PROBES[:] = [probe]
    worker._registry.register(_RecyclingSigtermRunner)
    worker._config.shutdown_timeout = 0.1  # drain_until 0.05 s, finish_by 0.1 s ab SIGTERM
    budget = shutdown_budget(0.1, supervised=False)
    worker._state = WorkerState.RUNNING
    worker._tasks_done = 1  # Recycling-Schwelle erreicht (sonst setzt der Monitor das)
    released_at: list[float] = []
    stopped_at: list[float] = []

    async def _release(_token: LockToken) -> bool:
        released_at.append(loop.time())
        return True

    async def _stop() -> None:
        stopped_at.append(loop.time())

    worker._lock_manager = MagicMock()
    worker._lock_manager.release = _release
    worker._scheduler = MagicMock()
    worker._scheduler.stop = _stop
    message = make_message(_default_payload(task_id="rec-sig-1", name="recycling_sigterm_runner", args=[], kwargs={}))
    await worker._on_message(message)
    await probe.started.wait()
    worker._active_handler_locks[uuid.uuid4()] = [LockToken(name="report", owner="tok", max_concurrency=1)]

    await recycling.request_recycle(worker, "tasks", 1.0)  # Frist consumer_timeout_s (1800 s)
    sigterms: list[float] = []
    _sigterm_at(worker, 0.02, sigterms)
    await asyncio.wait_for(worker._execute_shutdown(), 2.0)

    (sigterm,) = sigterms
    (cancelled_at,) = probe.cancelled_at
    assert sigterm + budget.drain_until - _EPS <= cancelled_at < sigterm + budget.finish_by
    assert broker.ack.await_count == 0
    # E-001: der Abbruch laeuft als Fehlerergebnis ueber Retry bzw. DLQ.
    assert broker.retry_later.await_count + broker.dead_letter.await_count == 1
    assert released_at
    assert released_at[0] < sigterm + budget.finish_by
    assert stopped_at
    assert stopped_at[0] < sigterm + budget.finish_by
    assert any(d is not None and abs(d - (sigterm + budget.drain_until)) < _EPS for d in _deadlines(broker))


async def test_a_second_sigterm_does_not_move_the_anchor(worker: Any, broker: Any) -> None:
    worker._config.shutdown_timeout = 0.08  # drain_until 0.04 s
    budget = shutdown_budget(0.08, supervised=False)
    drain_until, finish_by = budget.drain_until, budget.finish_by
    worker._drain_timeout_override = 5.0  # Recycling-Frist weit hinter dem Budget
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()  # wie request_recycle
    cancelled = asyncio.Event()

    async def _slow_to_cancel() -> None:
        # Braucht nach dem Abbruch noch 30 ms: das Aufraeumende muss dann am
        # ersten Anker gedeckelt werden, nicht am zweiten.
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            late = asyncio.Event()
            asyncio.get_running_loop().call_later(0.03, late.set)
            await late.wait()

    task = asyncio.create_task(_slow_to_cancel())
    worker._active_tasks.add(task)
    task.add_done_callback(worker._active_tasks.discard)
    sigterms: list[float] = []
    _sigterm_at(worker, 0.005, sigterms)
    _sigterm_at(worker, 0.03, sigterms)  # Eskalation des Supervisors

    await asyncio.wait_for(worker._execute_shutdown(), 2.0)

    first, _second = sigterms
    assert cancelled.is_set()
    # Das Aufraeumen endet im Budget ab dem ersten SIGTERM (sonst laege es
    # hinter dem SIGKILL, den der Supervisor ab seinem Abbau rechnet).
    assert worker._shutdown_end <= first + finish_by + _EPS
    deadlines = [d for d in _deadlines(broker) if d is not None]
    assert deadlines == sorted(deadlines, reverse=True), "die Endfrist darf nie spaeter werden"
    assert abs(deadlines[-1] - (first + drain_until)) < _EPS


async def test_sigterm_before_the_drain_starts_anchors_the_drain_end(worker: Any, broker: Any) -> None:
    worker._config.shutdown_timeout = 0.04
    drain_until = shutdown_budget(0.04, supervised=False).drain_until
    worker._drain_timeout_override = 5.0
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()  # wie request_recycle
    sigterm = asyncio.get_running_loop().time()
    worker._request_shutdown()  # SIGTERM, bevor _execute_shutdown laeuft
    task = _hang(worker)

    await asyncio.wait_for(worker._execute_shutdown(), 2.0)

    assert task.cancelled()
    first = _deadlines(broker)[0]
    assert first is not None
    assert sigterm + drain_until - _EPS <= first < sigterm + drain_until + _EPS


def _cancel_times(task: asyncio.Task[Any]) -> list[float]:
    loop = asyncio.get_running_loop()
    times: list[float] = []
    task.add_done_callback(lambda _t: times.append(loop.time()))
    return times


async def test_a_late_sigterm_never_extends_a_short_recycling_drain(worker: Any, broker: Any) -> None:
    worker._config.shutdown_timeout = 0.08
    drain_until = shutdown_budget(0.08, supervised=False).drain_until  # 0.04 s
    worker._drain_timeout_override = 0.05  # Recycling-Frist endet vor SIGTERM + drain_until
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()  # wie request_recycle
    task = _hang(worker)
    cancelled_at = _cancel_times(task)
    sigterms: list[float] = []
    _sigterm_at(worker, 0.03, sigterms)  # SIGTERM + drain_until = 0.07 s
    started = asyncio.get_running_loop().time()

    await asyncio.wait_for(worker._execute_shutdown(), 2.0)

    assert sigterms
    assert task.cancelled()
    # Obergrenze ist das Ende, das ein verlaengernder SIGTERM ergaebe; die
    # genaue Frist pruefen die berechneten Endfristen unten (CI-Last verzoegert
    # den Abbruch selbst um mehr als _EPS).
    assert started + 0.05 - _EPS <= cancelled_at[0] < sigterms[0] + drain_until
    deadlines = [d for d in _deadlines(broker) if d is not None]
    assert abs(deadlines[0] - (started + 0.05)) < _EPS
    assert max(deadlines) == deadlines[0], "die Endfrist darf nie spaeter werden"


async def test_sigterm_before_a_short_recycling_drain_keeps_the_earlier_end(worker: Any, broker: Any) -> None:
    worker._config.shutdown_timeout = 0.08
    drain_until = shutdown_budget(0.08, supervised=False).drain_until  # 0.04 s
    worker._drain_timeout_override = 0.02  # endet vor SIGTERM + drain_until
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()  # wie request_recycle
    started = asyncio.get_running_loop().time()
    worker._request_shutdown()  # SIGTERM, bevor _execute_shutdown laeuft
    task = _hang(worker)
    cancelled_at = _cancel_times(task)

    await asyncio.wait_for(worker._execute_shutdown(), 2.0)

    assert task.cancelled()
    # Obergrenze wie oben: das Ende ab SIGTERM; die genaue Frist pruefen die
    # berechneten Endfristen.
    assert started + 0.02 - _EPS <= cancelled_at[0] < started + drain_until
    first = _deadlines(broker)[0]
    assert first is not None
    assert abs(first - (started + 0.02)) < _EPS


@pytest.fixture
def channel() -> Iterator[sv.SupervisorChannel]:
    ch = sv.SupervisorChannel.create(multiprocessing.get_context("spawn"))
    sv.set_current_channel(ch)
    yield ch
    sv.set_current_channel(None)


async def test_sigterm_while_waiting_for_the_recycle_ack_keeps_the_budget(
    worker: Any, broker: Any, channel: sv.SupervisorChannel, swarmq_logs: list[dict[str, Any]]
) -> None:
    """Laufzeit rund 0,15 s: ``request_recycle`` fragt die Bestaetigung im
    100-ms-Takt ab (Bestand); das laesst sich ohne Eingriff in den Takt nicht
    durch ein Event ersetzen.

    Der Abbau beginnt wie im Worker beim SIGTERM; ein langsamer
    ``WORKER_SHUTDOWN``-Handler haelt ihn, bis ``request_recycle`` zurueck ist.
    Setzte ``request_recycle`` danach doch die Recycling-Frist, rechnete das
    Drain-Ende ab seiner Rueckkehr (bis zu einen Abfragetakt spaeter) statt ab
    dem SIGTERM."""
    worker._config.shutdown_timeout = 0.16
    budget = shutdown_budget(0.16, supervised=True)  # drain_until 0.04 s
    worker._state = WorkerState.RUNNING
    worker._tasks_done = 1
    task = _hang(worker)
    cancelled_at = _cancel_times(task)
    asked = asyncio.Event()
    request = asyncio.create_task(recycling.request_recycle(worker, "tasks", 1.0))
    loop = asyncio.get_running_loop()

    async def _slow_shutdown_handler(**_kw: Any) -> None:
        await asyncio.shield(request)

    worker._signal_dispatcher.on_signal(Signal.WORKER_SHUTDOWN)(_slow_shutdown_handler)

    def _poll() -> None:
        if channel.intent_value() == sv.INTENT_RECYCLE:
            asked.set()
        else:
            loop.call_later(0.005, _poll)

    _poll()
    await asked.wait()
    sigterm = loop.time()
    worker._request_shutdown()  # SIGTERM vor der Bestaetigung
    shutdown = asyncio.create_task(worker._execute_shutdown())  # der Worker baut ab SIGTERM ab
    loop.call_later(0.03, channel.acknowledge_replacement)
    await request
    returned = loop.time()  # danach erst laeuft der Drain an

    await asyncio.wait_for(shutdown, 2.0)

    assert task.cancelled(), "SIGTERM-Shutdown wartete die Recycling-Frist ab"
    # Das Drain-Ende ab SIGTERM ist bei Drain-Beginn verstrichen: sofortiger Abbruch.
    assert returned > sigterm + 2 * _EPS  # Vorbedingung: messbare Luecke
    # Obergrenze: ein ab der Rueckkehr von ``request_recycle`` geankertes
    # Drain-Ende brach erst bei ``returned + drain_until`` ab. Die Grenze
    # davor laesst Planungsverzug unter Last zu (CI-Pipeline 2950202597).
    assert cancelled_at[0] < returned + budget.drain_until
    first = _deadlines(broker)[0]
    assert first is not None
    assert abs(first - (sigterm + budget.drain_until)) < _EPS
    events = [entry["event"] for entry in swarmq_logs if entry["logger"] == "swarmq.worker.recycling"]
    assert "worker_recycling_superseded_by_shutdown" in events


async def test_sigterm_ends_the_ack_wait_without_marking_a_missing_ack(
    worker: Any, channel: sv.SupervisorChannel, swarmq_logs: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Supervisor im Abbau bestaetigt nie: SIGTERM beendet das Warten, ohne
    ``recycle_ack_missing`` (Exit 75). Laufzeit rund 0,1 s wie oben."""
    monkeypatch.setattr(recycling, "RECYCLE_ACK_TIMEOUT_S", 0.3)
    worker._state = WorkerState.RUNNING
    worker._tasks_done = 1
    asked = asyncio.Event()
    request = asyncio.create_task(recycling.request_recycle(worker, "tasks", 1.0))
    loop = asyncio.get_running_loop()

    def _poll() -> None:
        if channel.intent_value() == sv.INTENT_RECYCLE:
            asked.set()
        else:
            loop.call_later(0.005, _poll)

    _poll()
    await asked.wait()
    worker._request_shutdown()  # SIGTERM, keine Bestaetigung folgt

    await asyncio.wait_for(request, 0.25)

    assert getattr(worker, "_recycle_ack_missing", False) is False
    events = [entry["event"] for entry in swarmq_logs if entry["logger"] == "swarmq.worker.recycling"]
    assert "recycle_ack_missing" not in events
    assert "worker_recycling_superseded_by_shutdown" in events
    assert not getattr(worker, "_drain_timeout_override", None)


async def test_sigterm_during_the_monitor_sleep_does_not_request_a_recycle(worker: Any, no_supervisor: None) -> None:
    """Laufzeit rund 0,25 s: der Monitor prueft im festen 250-ms-Takt
    (Bestand); das SIGTERM muss genau in diesen Schlaf fallen."""
    worker._config.recycle_max_tasks = 1
    worker._state = WorkerState.RUNNING
    recyclings: list[dict[str, Any]] = []

    async def _on_recycling(**kw: Any) -> None:
        recyclings.append(kw)

    worker._signal_dispatcher.on_signal(Signal.WORKER_RECYCLING)(_on_recycling)
    worker._start_recycle_monitor()
    await asyncio.sleep(0)  # der Monitor prueft das Ereignis und schlaeft
    await worker._signal_dispatcher.dispatch(Signal.TASK_COMPLETED, task_id="t", task_name="n", queue="default")
    worker._request_shutdown()  # SIGTERM im Schlaf des Monitors

    await asyncio.wait_for(worker._recycle_monitor_task, 1.0)

    assert recyclings == []


async def test_recycling_drain_without_sigterm_leaves_no_helper_task(worker: Any) -> None:
    worker._config.shutdown_timeout = 0.02
    worker._drain_timeout_override = 5.0
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()
    done = asyncio.Event()
    asyncio.get_running_loop().call_later(0.03, done.set)
    task = asyncio.create_task(done.wait())
    worker._active_tasks.add(task)
    task.add_done_callback(worker._active_tasks.discard)
    before = asyncio.all_tasks()

    await asyncio.wait_for(worker._execute_shutdown(), 2.0)

    leftover = {t for t in asyncio.all_tasks() - before if not t.done()}
    assert not leftover


async def test_sigterm_cuts_a_hanging_cancel_consume_in_the_recycling_drain(
    worker: Any, broker: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review U10: ``cancel_consume`` haengt (Broker stumm) im Recycling-Drain
    mit langer Frist. Das SIGTERM muss auch diesen Schritt auf SIGTERM +
    drain_until verkuerzen, sonst kaeme der SIGKILL vor dem Aufraeumen."""
    monkeypatch.setattr(_lc, "_SHUTDOWN_STEP_MIN_S", 0.01)
    loop = asyncio.get_running_loop()
    worker._config.shutdown_timeout = 0.08  # drain_until 0.04 s, finish_by 0.08 s
    budget = shutdown_budget(0.08, supervised=False)
    worker._drain_timeout_override = 5.0  # Recycling-Frist weit hinter dem Budget
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()  # wie request_recycle
    broker.cancel_consume.side_effect = _forever
    step_ended: list[float] = []

    async def _close_executor() -> None:
        step_ended.append(loop.time())  # erster Schritt nach dem Drain

    worker._close_executor = _close_executor
    sigterms: list[float] = []
    _sigterm_at(worker, 0.01, sigterms)

    await asyncio.wait_for(worker._execute_shutdown(), 1.0)

    (sigterm,) = sigterms
    (ended,) = step_ended
    assert sigterm + budget.drain_until - _EPS <= ended < sigterm + budget.finish_by
    deadlines = [d for d in _deadlines(broker) if d is not None]
    assert abs(deadlines[-1] - (sigterm + budget.drain_until)) < _EPS


async def _forever() -> None:
    await asyncio.Event().wait()
