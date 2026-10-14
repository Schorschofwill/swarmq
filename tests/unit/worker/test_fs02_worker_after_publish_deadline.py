"""Worker-Pfade nach verstrichener Publish-Endfrist (FS-02 Teil a, Plan
2026-10-14-0748, U3; Key Decision "Drain-Abbruch nach FS-02a hinnehmen").

Mit echtem ``RabbitMQBroker`` (Kanal gemockt), damit die Pruefung vor dem
Senden wirklich durchlaufen wird. Nach der Endfrist geht keine Retry- oder
DLQ-Kopie raus; das Original wird per ``redeliver`` zurueckgestellt, ein
Schedfire-Claim vorher freigegeben.

Ein vom Drain abgebrochener Handler endet nach E-001 als Fehlerergebnis.
Weil die Endfrist dann verstrichen ist, kommt das Original neu, statt per
Retry/DLQ kopiert zu werden (Nachtrag E-001). Dieser Test haelt die Folge
fest, ohne ``entscheidung``-Marker.

Testliste:
- [x] Retry-Publish nach der Endfrist: nichts gesendet, Claim frei, dann nack(requeue)
- [x] Drain-Abbruch bei max_retries=0 im Workflow-Schritt: Fehlerergebnis,
      fail_workflow, keine DLQ-Kopie, Neuzustellung, kein Completed-Marker
- [x] Drain-Abbruch, nack scheitert mit aio-pika-interner CancelledError: kein
      Completed-Marker, keine Unique-Freigabe, kein durchgereichter Abbruch
"""

from __future__ import annotations

import asyncio
from typing import Any, cast, override
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_UNIQUE_HASH, HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

FIRE = "cron_a:1700000000000"
WF_ID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"
UNIQUE = "a" * 64


class FailingTask(Task):
    task_info = TaskInfo(name="fs02-failing", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        raise RuntimeError("task boom")


class LongStep(Task):
    task_info = TaskInfo(name="fs02-long-step", queues=["default"], max_retries=0)
    started: asyncio.Event | None = None

    @override
    async def run(self) -> None:
        assert LongStep.started is not None
        LongStep.started.set()
        await asyncio.Event().wait()


def _rabbit() -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker.publish_timeout_s = 60.0
    broker._connection = Mock(transport=None, is_closed=False)
    broker._connection.ready = AsyncMock()
    broker._publish_channel = AsyncMock(is_closed=False)
    return broker


def _sent(broker: RabbitMQBroker) -> list[str]:
    publish = cast("AsyncMock", broker._publish_channel).default_exchange.publish
    return [call.kwargs["routing_key"] for call in publish.await_args_list]


def _worker(broker: RabbitMQBroker, backend: Any, config: Any, task: type[Task]) -> Worker:
    registry = TaskRegistry()
    registry.register(task)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.fail = AsyncMock()
    return worker


def _elapse(broker: RabbitMQBroker) -> None:
    broker.set_publish_deadline(asyncio.get_running_loop().time() - 1.0)


def _record(backend: Any, msg: Any) -> list[str]:
    calls: list[str] = []
    backend.schedules.release_fire_claim = AsyncMock(side_effect=lambda *a, **k: calls.append("release"))
    msg.nack = AsyncMock(side_effect=lambda *a, **k: calls.append(f"nack(requeue={k.get('requeue')})"))
    return calls


async def test_retry_after_the_deadline_sends_no_copy_and_redelivers_after_the_claim_release(
    backend: Any, config: Any
) -> None:
    broker = _rabbit()
    worker = _worker(broker, backend, config, FailingTask)
    msg = make_message(_default_payload(name="fs02-failing", args=[], kwargs={}), headers={"x-swarmq-schedfire": FIRE})
    calls = _record(backend, msg)
    _elapse(broker)

    await worker._handle_message(msg)

    assert _sent(broker) == []
    assert calls == ["release", "nack(requeue=True)"]
    backend.schedules.mark_fire_completed.assert_not_awaited()
    msg.ack.assert_not_awaited()


async def _drain_cancel(worker: Worker, broker: RabbitMQBroker, msg: Any) -> asyncio.Task[None]:
    """Startet den Handler, bricht ihn wie ``_drain_in_flight`` nach der
    Endfrist ab und wartet sein Ende ab."""
    LongStep.started = asyncio.Event()
    handler = asyncio.create_task(worker._handle_message(msg))
    await LongStep.started.wait()
    _elapse(broker)
    handler.cancel()
    await asyncio.wait({handler})
    return handler


def _step_message() -> Any:
    return make_message(
        _default_payload(name="fs02-long-step", args=[], kwargs={}),
        headers={
            "x-swarmq-schedfire": FIRE,
            HEADER_WORKFLOW_ID: WF_ID,
            HEADER_WORKFLOW_STEP: "/1",
            HEADER_UNIQUE_HASH: UNIQUE,
        },
    )


async def test_drain_cancelled_workflow_step_without_retries_is_redelivered_not_dead_lettered(
    backend: Any, config: Any
) -> None:
    broker = _rabbit()
    worker = _worker(broker, backend, config, LongStep)
    backend.release_unique = AsyncMock(return_value=True)
    msg = _step_message()
    calls = _record(backend, msg)

    _ = await _drain_cancel(worker, broker, msg)

    worker._workflow_advancer.fail.assert_awaited_once()  # type: ignore[union-attr]  # MagicMock aus _worker
    backend.store_result.assert_awaited_once()
    assert _sent(broker) == []
    assert calls == ["release", "nack(requeue=True)"]
    backend.schedules.mark_fire_completed.assert_not_awaited()
    backend.release_unique.assert_not_awaited()
    msg.ack.assert_not_awaited()


async def test_lost_channel_on_the_nack_after_a_drain_cancel_is_not_terminal(backend: Any, config: Any) -> None:
    broker = _rabbit()
    worker = _worker(broker, backend, config, LongStep)
    backend.release_unique = AsyncMock(return_value=True)
    msg = _step_message()
    # aio-pika bricht bei Kanalverlust das ``drain_future`` des nack ab.
    msg.nack = AsyncMock(side_effect=asyncio.CancelledError)

    handler = await _drain_cancel(worker, broker, msg)

    assert _sent(broker) == []
    backend.schedules.mark_fire_completed.assert_not_awaited()
    backend.release_unique.assert_not_awaited()
    assert not handler.cancelled(), "der Kanalverlust darf nicht als eigener Abbruch durchgehen"
