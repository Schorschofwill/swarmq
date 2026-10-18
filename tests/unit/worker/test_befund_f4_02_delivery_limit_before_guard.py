"""Nachweis F4-02: Am Zustelllimit wird eine erledigte oder überholte
Ausführung dead-gelettert statt als Duplikat bestätigt.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-02; Verdacht
R4Z-04; Gegenprüfung ``6-gegenpruefung-Z.md``). Abgeleitet aus den
geschrumpften Abläufen der Retry-Maschine
(``tests/property/stateful/retry_chain_scenarios.py``, ``r4z_04_*``), hier
ohne Hypothesis über ``Worker._handle_message`` mit dem Ausführungsschutz des
``InMemoryBackend``.

Das Delivery-Count-Gate (``_pre_pipeline_gates``) liegt vor dem
Ausführungsschutz. Erreicht die Neuzustellung einer Ausführung mit
Done-Marker (Erfolg, Ack verloren) oder Superseded-Marker (Retry-Kopie liegt,
Ack des Originals verloren) die Schwelle ``delivery_limit - 1``, schickt das
Gate sie als ``WorkerHungError`` in die DLQ: das Fehlerergebnis überschreibt
den Erfolg bzw. meldet einen Fehler, während die Kette über die Retry-Kopie
weiterläuft.

Erwartet: E-009 („Eine normale Neuzustellung derselben Ausführung … wird
weiter als Duplikat verworfen“), E-043 („Die Neuzustellung des Originals
überspringt der Ausführungsschutz als ``done``“), ``delivery-semantics.md:380-390``
(„Done, or superseded by a retry with a higher count: the delivery is
acknowledged and skipped“), Plan 2026-10-08-1541 KTD5 (Konflikte).

Testliste:
# [x] Erfolg, dann Neuzustellung mit x-delivery-count 19 (Limit 20) -> Ack, kein Dead-Letter, kein Fehlerergebnis
# [x] Retry-Ausgang, dann Neuzustellung des Originals mit x-delivery-count 19 -> Ack, kein Dead-Letter, kein Fehlerergebnis
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import RateLimitDecision
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message

pytestmark = pytest.mark.timeout(5)

_DELIVERY_LIMIT = 20
_AT_THE_LIMIT = {"x-delivery-count": _DELIVERY_LIMIT - 1}
RUNS: list[str] = []


class F402Done(Task):
    task_info = TaskInfo(name="f4_02_done", queues=["default"])

    @override
    async def run(self) -> str:
        RUNS.append("run")
        return "ok"


class F402Flaky(Task):
    task_info = TaskInfo(name="f4_02_flaky", queues=["default"], max_retries=3)

    @override
    async def run(self) -> str:
        RUNS.append("run")
        raise RuntimeError("retryable failure")


@pytest.fixture(autouse=True)
def _reset_runs() -> None:
    RUNS.clear()


@pytest.fixture
async def guard_backend() -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(FakeClock())
    await impl.connect()
    yield impl
    await impl.close()


def _payload(name: str) -> dict[str, Any]:
    return {"task": {"id": "t-1", "queue": "default", "name": name}, "args": [], "kwargs": {}}


def _worker(guard_backend: InMemoryBackend, task: type[Task]) -> tuple[Worker, MagicMock, MagicMock]:
    broker = MagicMock(spec=RabbitMQBroker)
    for verb in ("ack", "redeliver", "retry_later", "dead_letter", "route_via_dlx"):
        setattr(broker, verb, AsyncMock())
    backend = MagicMock(spec=ValkeyBackend)
    backend.store_result = AsyncMock()
    backend.notify_result = AsyncMock()
    backend.is_cancelled = AsyncMock(return_value=False)
    backend.is_workflow_cancelled = AsyncMock(return_value=False)
    backend.clear_cancel_flag = AsyncMock()
    backend.release_unique = AsyncMock(return_value=True)
    registry = TaskRegistry()
    registry.register(task)
    config = SwarmQConfig(
        broker_url="amqp://x", backend_url="valkey://x", queues=["default"], delivery_limit=_DELIVERY_LIMIT
    )
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    limiter = MagicMock()
    limiter.check = AsyncMock(return_value=RateLimitDecision(allowed=True, remaining=9, retry_after_ms=0))
    worker._rate_limiter = limiter
    worker._lock_manager = MagicMock()
    worker._guard_store = guard_backend.execution_guard
    return worker, broker, backend


def _stored_errors(backend: MagicMock) -> list[str]:
    """Fehlerergebnisse, die ``store_result`` geschrieben hat."""
    errors: list[str] = []
    for call in backend.store_result.await_args_list:
        raw = next(arg for arg in (*call.args, *call.kwargs.values()) if isinstance(arg, bytes))
        result = decode_result(raw)
        if result.get("is_error"):
            errors.append(str(result.get("error_message")))
    return errors


@pytest.mark.entscheidung("E-043")
@pytest.mark.befund("F4-02")
async def test_completed_execution_redelivered_at_the_limit_stays_a_duplicate(
    guard_backend: InMemoryBackend,
) -> None:
    worker, broker, backend = _worker(guard_backend, F402Done)
    await worker._handle_message(make_message(_payload("f4_02_done")))
    assert RUNS == ["run"]
    broker.ack.reset_mock()

    await worker._handle_message(make_message(_payload("f4_02_done"), headers=_AT_THE_LIMIT))

    assert RUNS == ["run"]
    assert broker.dead_letter.await_count == 0, "erledigte Ausführung am Zustelllimit in der DLQ (E-009, E-043)"
    assert _stored_errors(backend) == [], "Erfolgsergebnis mit einem Fehlerergebnis überschrieben"
    assert broker.ack.await_count == 1


@pytest.mark.entscheidung("E-043")
@pytest.mark.befund("F4-02")
async def test_superseded_attempt_redelivered_at_the_limit_stays_a_duplicate(
    guard_backend: InMemoryBackend,
) -> None:
    worker, broker, backend = _worker(guard_backend, F402Flaky)
    await worker._handle_message(make_message(_payload("f4_02_flaky")))
    assert broker.retry_later.await_args is not None
    assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-retry-count"] == 1
    broker.ack.reset_mock()

    # Das Original (ohne Retry-Zähler) kommt erneut, sein Ack ging verloren.
    await worker._handle_message(make_message(_payload("f4_02_flaky"), headers=_AT_THE_LIMIT))

    assert RUNS == ["run"]
    assert broker.dead_letter.await_count == 0, "überholter Versuch in der DLQ, die Retry-Kopie läuft weiter"
    assert _stored_errors(backend) == [], "Fehlerergebnis, obwohl die Kette über die Retry-Kopie weiterläuft"
