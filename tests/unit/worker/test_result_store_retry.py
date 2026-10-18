"""E-072: ``ResultTail.finish`` wiederholt ein gescheitertes Speichern des
Ergebnisses begrenzt selbst, bevor die Nachricht zurückgeht (Plan
2026-10-17-2103, U13, R11, KTD7, KD8).

Bis zu fünf Speicherversuche mit den Wartezeiten der Zurückstellung
(``backend_defer_delay``: nominell 1, 2, 4, 8 s, Jitter 0,8 ... 1,2),
gedeckelt durch ``consumer_timeout_s`` ab dem Zustellzeitpunkt und durch das
Drain-Ende (Abbruch im Warten endet ohne Propagation mit
``redeliver_in_place``, E-001). FIRE_AND_FORGET bleibt bei einem Versuch.
Die Fake-Uhr ``result_store_clock`` (``tests/unit/conftest.py``) ersetzt
Warten und Zeitquelle.

Testliste:
# [x] Speichern gelingt beim dritten Versuch: Ack, kein redeliver, Wartezeiten
#     1 und 2 s (nominell), INFO result_store_recovered
# [x] Fünf Versuche scheitern: redeliver_in_place nach 15 s nomineller
#     Wartesumme (vier Wartezeiten), kein Ack
# [x] Wanduhr: mit backend_op_timeout je Versuch bleibt die Zeit nach dem
#     ersten Fehlschlag unter 4 * op_timeout + 15 s * 1,2
# [x] Wanduhr bei sofort abgewiesener Verbindung: jeder Versuch endet nach dem
#     Wiederholfenster des Clients (3,1 s), nicht nach op_timeout -- nach dem
#     ersten Fehlschlag 4 * 3,1 s + Wartezeiten (Charakterisierung, Loadsuite
#     0.13.0rc1)
# [x] consumer_timeout_s deckelt frueher: redeliver vor der Frist (inkl.
#     op_timeout des nächsten Versuchs)
# [x] Frist schon beim ersten Fehlschlag knapp: keine Wartezeit, sofort redeliver
# [x] Drain-Ende während des Wartens: kein CancelledError, redeliver_in_place,
#     Redelivered
# [x] Drain-Ende während eines wiederholten Speicherversuchs: ebenso
# [x] Drain-Ende im Worker: kein Schedfire-Completed-Marker, keine
#     Unique-Freigabe, Running-Marker frei (die Neuzustellung läuft)
# [x] Fehlerergebnis (dead_letter=False), zweiter Versuch gelingt: Ack
# [x] Abbruchergebnis, zweiter Versuch gelingt: Ack, Cancel-Flag verkürzt
# [x] FIRE_AND_FORGET: ein Versuch, keine Wartezeit
# [x] WARNING result_store_retrying gedrosselt: je Worker höchstens eine je
#     Intervall, sonst DEBUG
# [x] Speichern gelingt, Notify scheitert: Ack trotzdem
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.headers import HEADER_UNIQUE_HASH
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.execution import ExecutionResult
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Redelivered, Terminal
from swarmq.worker.results import RETRY_WARNING_INTERVAL_S
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)


class StoreTask(Task):
    task_info = TaskInfo(name="store-retry", queues=["default"])

    @override
    async def run(self) -> str:
        return "ok"


def _tail(broker, backend, config, task: type[Task] = StoreTask):
    registry = TaskRegistry()
    registry.register(task)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    return worker._result_tail, worker


def _execution(**overrides: Any) -> ExecutionResult:
    base: dict[str, Any] = {
        "result_value": "ok",
        "is_error": False,
        "is_cancelled": False,
        "error_message": "",
        "task_exception": None,
    }
    base.update(overrides)
    return ExecutionResult(**base)


def _ctx(task: type[Task] = StoreTask) -> GateContext:
    return GateContext(
        task_id="abc-123", task_name=task.task_info.name, queue="default", task_info=task.task_info, task_class=task
    )


def _events(logs, event):
    return [e for e in logs if e["logger"] == "swarmq.worker.results" and e["event"] == event]


def _failing(times: int) -> AsyncMock:
    """``store_result``, das *times*-mal scheitert und danach gelingt."""
    return AsyncMock(side_effect=[*(BackendConnectionError("valkey down") for _ in range(times)), None])


@pytest.mark.entscheidung("E-072")
async def test_store_succeeds_on_the_third_attempt_and_acks(broker, backend, config, result_store_clock, swarmq_logs):
    backend.store_result = _failing(2)
    tail, _ = _tail(broker, backend, config)
    msg = make_message(_default_payload(name="store-retry"))

    outcome = await tail.finish(msg, _ctx(), _execution())

    assert outcome == Terminal.release_in_finally()
    assert backend.store_result.await_count == 3
    broker.ack.assert_awaited_once_with(msg)
    broker.redeliver.assert_not_awaited()
    first, second = result_store_clock.delays
    assert 0.8 <= first <= 1.2
    assert 1.6 <= second <= 2.4
    [entry] = _events(swarmq_logs, "result_store_recovered")
    assert entry["log_level"] == "info"
    assert entry["store_attempts"] == 3


class FafStoreTask(Task):
    """FIRE_AND_FORGET mit explizitem ``result_ttl``: speichert, wiederholt nicht."""

    task_info = TaskInfo(
        name="store-retry-faf", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET, result_ttl=60
    )

    @override
    async def run(self) -> str:
        return "ok"


@pytest.mark.entscheidung("E-072")
async def test_fire_and_forget_stores_once_without_retry(broker, backend, config, result_store_clock):
    """KD8: die Wiederholung gilt nur für AT_LEAST_ONCE."""
    backend.store_result = _failing(1)
    tail, _ = _tail(broker, backend, config, FafStoreTask)

    outcome = await tail.finish(
        make_message(_default_payload(name="store-retry-faf")), _ctx(FafStoreTask), _execution()
    )

    assert outcome == Terminal.release_in_finally()
    backend.store_result.assert_awaited_once()
    assert result_store_clock.delays == []
    broker.ack.assert_not_awaited()
    broker.redeliver.assert_not_awaited()


@pytest.mark.entscheidung("E-072")
async def test_five_failed_attempts_redeliver_after_the_nominal_15_seconds(broker, backend, config, result_store_clock):
    backend.store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    tail, _ = _tail(broker, backend, config)
    msg = make_message(_default_payload(name="store-retry"))

    outcome = await tail.finish(msg, _ctx(), _execution())

    assert outcome is Redelivered
    assert backend.store_result.await_count == 5
    nominal = (1.0, 2.0, 4.0, 8.0)
    assert len(result_store_clock.delays) == len(nominal)
    for delay, expected in zip(result_store_clock.delays, nominal, strict=True):
        assert 0.8 * expected <= delay <= 1.2 * expected
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()


@pytest.mark.entscheidung("E-072")
async def test_wall_clock_stays_bounded_when_every_attempt_runs_into_the_op_timeout(
    broker, backend, config, result_store_clock
):
    """Jeder Versuch läuft bis ``backend_op_timeout`` (Valkey hängt): nach
    dem ersten Fehlschlag kommen höchstens vier Versuche und vier
    Wartezeiten dazu -- nominell 4 * 5 s + 15 s = 35 s, mit Jitter < 38 s."""
    op_timeout = config.backend_op_timeout
    failed_first_at: list[float] = []

    async def hanging_store(*_a: Any, **_k: Any) -> None:
        result_store_clock.now += op_timeout
        failed_first_at.append(result_store_clock.now)
        raise BackendConnectionError("timed out")

    backend.store_result = AsyncMock(side_effect=hanging_store)
    tail, _ = _tail(broker, backend, config)

    await tail.finish(make_message(_default_payload(name="store-retry")), _ctx(), _execution())

    assert result_store_clock.now - failed_first_at[0] <= 4 * op_timeout + 15.0 * 1.2
    broker.redeliver.assert_awaited_once()


# Wiederholfenster des Valkey-Clients bei abgewiesener TCP-Verbindung: fünf
# TCP-Wiederholungen mit 0,1 + 0,2 + 0,4 + 0,8 + 1,6 s, dann der Fehler
# (festgeschrieben in ``tests/unit/backend/test_valkey_handshake_retry.py::
# test_refused_tcp_is_retried_five_times_without_outer_retry``). Es liegt unter
# ``backend_op_timeout`` (5 s), ein Versuch läuft also nicht in die Frist.
_REFUSED_ATTEMPT_S = 3.1


@pytest.mark.entscheidung("E-072")
async def test_wall_clock_when_valkey_refuses_every_connection_at_once(broker, backend, config, result_store_clock):
    """Valkey weist jede Verbindung sofort ab ("Error 111 connecting"): jeder
    Versuch endet nach dem Wiederholfenster des Clients. Nach dem ersten
    Fehlschlag kommen vier Versuche und vier Wartezeiten dazu -- nominell
    4 * 3,1 s + 15 s = 27,4 s (mit Jitter 24,4 ... 30,4 s), ab dem ersten
    Versuch 30,5 s (27,5 ... 33,5 s). Gemessen in der Loadsuite-Abnahme
    0.13.0rc1: rund 29-30 s ab Ausfallbeginn.

    Charakterisierung: schreibt bestehendes Verhalten fest, war beim ersten
    Lauf grün."""
    assert config.backend_op_timeout > _REFUSED_ATTEMPT_S
    started_at = result_store_clock.now
    failed_at: list[float] = []

    async def refused_store(*_a: Any, **_k: Any) -> None:
        result_store_clock.now += _REFUSED_ATTEMPT_S
        failed_at.append(result_store_clock.now)
        raise BackendConnectionError("Error 111 connecting to valkey:6379. Connection refused.")

    backend.store_result = AsyncMock(side_effect=refused_store)
    tail, _ = _tail(broker, backend, config)

    outcome = await tail.finish(make_message(_default_payload(name="store-retry")), _ctx(), _execution())

    assert outcome is Redelivered
    assert len(failed_at) == 5
    waits = sum(result_store_clock.delays)
    after_first_failure = result_store_clock.now - failed_at[0]
    assert after_first_failure == pytest.approx(4 * _REFUSED_ATTEMPT_S + waits)
    assert 4 * _REFUSED_ATTEMPT_S + 15.0 * 0.8 <= after_first_failure <= 4 * _REFUSED_ATTEMPT_S + 15.0 * 1.2
    since_first_attempt = result_store_clock.now - started_at
    assert 5 * _REFUSED_ATTEMPT_S + 15.0 * 0.8 <= since_first_attempt <= 5 * _REFUSED_ATTEMPT_S + 15.0 * 1.2


@pytest.mark.entscheidung("E-072")
async def test_consumer_timeout_caps_the_retry_before_the_deadline(broker, backend, result_store_clock):
    """``consumer_timeout_s`` ab dem Zustellzeitpunkt: der nächste Versuch
    (Wartezeit plus ``backend_op_timeout``) muss vor der Frist enden, sonst
    geht die Nachricht sofort zurück -- RabbitMQ schlösse sonst den Kanal."""
    config = SwarmQConfig(
        broker_url="amqp://localhost", backend_url="valkey://localhost", queues=["default"], consumer_timeout_s=9.0
    )
    backend.store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    tail, _ = _tail(broker, backend, config)
    ctx = _ctx()  # zugestellt bei 1000 s
    result_store_clock.now += 2.0  # der Task lief 2 s

    outcome = await tail.finish(make_message(_default_payload(name="store-retry")), ctx, _execution())

    assert outcome is Redelivered
    assert backend.store_result.await_count == 2
    assert len(result_store_clock.delays) == 1
    assert result_store_clock.now < 1000.0 + 9.0
    broker.redeliver.assert_awaited_once()


async def test_no_room_before_the_consumer_timeout_redelivers_without_waiting(broker, backend, result_store_clock):
    """Grenzfall: schon der erste Fehlschlag liegt so spät, dass kein Versuch
    mehr vor die Frist passt -- sofort zurück wie vor E-072."""
    config = SwarmQConfig(
        broker_url="amqp://localhost", backend_url="valkey://localhost", queues=["default"], consumer_timeout_s=5.0
    )
    backend.store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    tail, _ = _tail(broker, backend, config)

    outcome = await tail.finish(make_message(_default_payload(name="store-retry")), _ctx(), _execution())

    assert outcome is Redelivered
    backend.store_result.assert_awaited_once()
    assert result_store_clock.delays == []


@pytest.mark.entscheidung("E-072")
async def test_drain_cancel_while_waiting_redelivers_without_propagating(
    broker, backend, config, result_store_clock, monkeypatch
):
    """Drain-Ende (``task.cancel()``) im Warten: die Wiederholung endet ohne
    ``CancelledError``, die Nachricht geht per ``redeliver_in_place`` zurück."""
    backend.store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    waiting = asyncio.Event()

    async def blocking_sleep(_delay_s: float) -> None:
        waiting.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("swarmq.worker.results._sleep", blocking_sleep)
    tail, _ = _tail(broker, backend, config)
    msg = make_message(_default_payload(name="store-retry"))

    handler = asyncio.create_task(tail.finish(msg, _ctx(), _execution()))
    await waiting.wait()
    handler.cancel()
    outcome = await handler

    assert outcome is Redelivered
    backend.store_result.assert_awaited_once()
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()


@pytest.mark.entscheidung("E-072")
async def test_drain_cancel_during_a_repeated_store_attempt_redelivers_without_propagating(
    broker, backend, config, result_store_clock
):
    """Der wiederholte Versuch hängt (bis ``backend_op_timeout``), das
    Drain-Ende trifft ihn: ebenso ``Redelivered`` statt ``CancelledError``.
    Der erste Versuch bleibt davon unberührt (``test_pipeline_settle``)."""
    in_second_attempt = asyncio.Event()

    async def store(*_a: Any, **_k: Any) -> None:
        if backend.store_result.await_count == 1:
            raise BackendConnectionError("valkey down")
        in_second_attempt.set()
        await asyncio.Event().wait()

    backend.store_result = AsyncMock(side_effect=store)
    tail, _ = _tail(broker, backend, config)
    msg = make_message(_default_payload(name="store-retry"))

    handler = asyncio.create_task(tail.finish(msg, _ctx(), _execution()))
    await in_second_attempt.wait()
    handler.cancel()
    outcome = await handler

    assert outcome is Redelivered
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()


@pytest.fixture
async def guard_backend() -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(FakeClock())
    await impl.connect()
    yield impl
    await impl.close()


@pytest.mark.entscheidung("E-072")
async def test_drain_cancel_in_the_worker_leaves_no_completed_marker_and_no_unique_release(
    worker, broker, backend, guard_backend, monkeypatch
):
    """Im Worker: der Abbruch erreicht den Terminal-Default im ``finally``
    nicht -- kein Schedfire-Completed-Marker, keine Unique-Freigabe, der
    Running-Marker ist frei (die Neuzustellung läuft sofort)."""
    worker._guard_store = guard_backend.execution_guard
    backend.release_unique = AsyncMock(return_value=True)
    backend.store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    waiting = asyncio.Event()

    async def blocking_sleep(_delay_s: float) -> None:
        waiting.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("swarmq.worker.results._sleep", blocking_sleep)
    headers = {"x-swarmq-schedfire": "cron_a:1700000000000", HEADER_UNIQUE_HASH: "a" * 64}
    msg = make_message(_default_payload(task_id="t-drain"), headers=headers)

    await worker._on_message(msg)
    [handler] = worker._active_tasks
    await waiting.wait()
    handler.cancel()
    outcomes: list[BaseException | None] = await asyncio.gather(handler, return_exceptions=True)

    backend.schedules.mark_fire_completed.assert_not_awaited()
    backend.release_unique.assert_not_awaited()
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()
    assert outcomes == [None]

    backend.store_result = AsyncMock()
    redelivery = make_message(_default_payload(task_id="t-drain"), headers={"x-delivery-count": 1})
    await worker._handle_message(redelivery)
    backend.store_result.assert_awaited_once()
    broker.ack.assert_awaited_once_with(redelivery)


RUNS: list[str] = []


class FailingNoDlqTask(Task):
    """Terminaler Fehler ohne Dead-Letter: das Fehlerergebnis geht über den Tail."""

    task_info = TaskInfo(name="store-retry-fail", queues=["default"], max_retries=0, dead_letter=False)

    @override
    async def run(self) -> None:
        RUNS.append("run")
        raise RuntimeError("task boom")


@pytest.mark.entscheidung("E-072")
async def test_error_result_without_dead_letter_is_stored_on_the_second_attempt_and_acked(
    worker, broker, backend, result_store_clock
):
    RUNS.clear()
    worker._registry.register(FailingNoDlqTask)
    backend.store_result = _failing(1)
    msg = make_message(_default_payload(task_id="t-err", name="store-retry-fail", args=[], kwargs={}))

    await worker._handle_message(msg)

    assert RUNS == ["run"]
    assert backend.store_result.await_count == 2
    stored = decode_result(backend.store_result.await_args.args[1])
    assert stored["is_error"] is True
    assert len(result_store_clock.delays) == 1
    broker.ack.assert_awaited_once_with(msg)
    broker.redeliver.assert_not_awaited()
    broker.dead_letter.assert_not_awaited()


@pytest.mark.entscheidung("E-072")
async def test_cancelled_result_is_stored_on_the_second_attempt_and_acked(broker, backend, config):
    backend.store_result = _failing(1)
    tail, _ = _tail(broker, backend, config)
    msg = make_message(_default_payload(name="store-retry"))

    outcome = await tail.finish(msg, _ctx(), _execution(result_value=None, is_cancelled=True))

    assert outcome == Terminal.release_in_finally()
    assert backend.store_result.await_count == 2
    assert decode_result(backend.store_result.await_args.args[1])["is_cancelled"] is True
    backend.clear_cancel_flag.assert_awaited_once()
    broker.ack.assert_awaited_once_with(msg)
    broker.redeliver.assert_not_awaited()


async def test_notify_failure_after_a_recovered_store_still_acks(broker, backend, config):
    """Bestehendes Verhalten: die Benachrichtigung ist best effort."""
    backend.store_result = _failing(1)
    backend.notify_result = AsyncMock(side_effect=ConnectionError("pubsub down"))
    tail, _ = _tail(broker, backend, config)
    msg = make_message(_default_payload(name="store-retry"))

    outcome = await tail.finish(msg, _ctx(), _execution())

    assert outcome == Terminal.release_in_finally()
    broker.ack.assert_awaited_once_with(msg)


async def test_retrying_is_logged_as_a_throttled_warning(broker, backend, config, result_store_clock, swarmq_logs):
    """``result_store_retrying``: je Worker höchstens eine WARNING je
    ``RETRY_WARNING_INTERVAL_S``, dazwischen DEBUG (ein Valkey-Ausfall trifft
    alle gerade fertigen Tasks zugleich)."""
    backend.store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    tail, _ = _tail(broker, backend, config)

    await tail.finish(make_message(_default_payload(name="store-retry")), _ctx(), _execution())
    result_store_clock.now += RETRY_WARNING_INTERVAL_S
    await tail.finish(make_message(_default_payload(name="store-retry")), _ctx(), _execution())

    entries = _events(swarmq_logs, "result_store_retrying")
    assert [e["log_level"] for e in entries] == ["warning", "debug", "debug", "debug"] * 2
    first = entries[0]
    assert first["task_id"] == "abc-123"
    assert first["store_attempt"] == 1
    assert 0.8 <= first["delay_s"] <= 1.2
    assert first["error_type"] == "BackendConnectionError"
    assert first["error"] == "valkey down"
