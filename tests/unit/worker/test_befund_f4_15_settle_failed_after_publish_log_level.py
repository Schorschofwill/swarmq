"""Nachweis F4-15: Liegt die Retry-Kopie bestätigt in der Delay-Queue und
scheitert nur das Ack des Originals, meldet der ``RetryHandler`` trotzdem
ERROR ``retry_publish_failed``.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-15; Verdacht
R4I-03; Gegenprüfung ``6-gegenpruefung-I.md``). Beobachtet im ``toxic_down``
des Loadsuite-Laufs ``stoerung-1h-20261017T135831`` (Backlog B28).

``RetryHandler.evaluate`` (``worker/retry.py``) fängt jede Ausnahme von
``broker.retry_later`` mit ``except Exception``, loggt ERROR
``retry_publish_failed`` („Retry could not be published; the message is
redelivered“) und ruft ``redeliver_in_place``. ``SettleFailedAfterPublish``
heißt aber: Die Kopie ist veröffentlicht (Publisher-Confirm), nur das
Bestätigen des Originals scheiterte (``exceptions.py``). Der Ausführungsschutz
überspringt die Neuzustellung des Originals als ``superseded``; der Retry ist
erfolgt, es fehlt die INFO ``task_retrying``.

Erwartet: Level-Regel aus 0.8.0 (``changelog.md:2574-2576``: ERROR nur für
Datenverlust oder einen gescheiterten Task/Prozess, WARNING für
selbstheilende Störungen); Katalogtext ``logging.md:660``; gleiche Regel für
die DLQ-Kopie in E-043 (``dlq_ack_failed_after_publish`` auf WARNING,
festgeschrieben in ``test_retry.py::test_dead_letter_with_failed_ack_after_publish_is_terminal``).
Die Ausgänge (Kopie liegt, Original übersprungen) sind richtig und nicht Teil
des Nachweises.

Grenze gemockt: ``broker.retry_later`` wirft ``SettleFailedAfterPublish``
(wie ``RabbitMQBroker`` nach bestätigtem Publish und totem Kanal).

Testliste:
# [x] retry_later wirft SettleFailedAfterPublish -> kein ERROR retry_publish_failed, task_retrying geloggt,
#     keine Neuzustellung des Originals
"""

from __future__ import annotations

from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import SettleFailedAfterPublish
from swarmq.middleware import MessageContext, MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)


class F415Retrying(Task):
    task_info = TaskInfo(name="f4_15_retrying", queues=["default"], max_retries=3)

    @override
    async def run(self) -> str:
        return "ok"


def _ctx() -> GateContext:
    return GateContext(
        task_id="abc-123",
        task_name=F415Retrying.task_info.name,
        queue="default",
        task_info=F415Retrying.task_info,
        task_class=F415Retrying,
    )


def _retry_logs(logs: list[dict[str, Any]], event: str) -> list[dict[str, Any]]:
    return [e for e in logs if e["logger"] == "swarmq.worker.retry" and e["event"] == event]


@pytest.mark.entscheidung("E-043")
@pytest.mark.befund("F4-15")
async def test_retry_copy_published_but_ack_failed_is_not_reported_as_failed_publish(
    broker: MagicMock, backend: MagicMock, config: SwarmQConfig, swarmq_logs: list[dict[str, Any]]
) -> None:
    registry = TaskRegistry()
    registry.register(F415Retrying)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    broker.retry_later = AsyncMock(side_effect=SettleFailedAfterPublish("channel closed after publisher confirm"))
    broker.redeliver = AsyncMock()

    await worker._retry_handler.evaluate(
        make_message(_default_payload(name=F415Retrying.task_info.name)),
        _ctx(),
        RuntimeError("task boom"),
        "task boom",
        MessageContext(
            task_id="abc-123",
            task_name=F415Retrying.task_info.name,
            queue="default",
            args=[],
            kwargs={},
            delivery_mode="at_least_once",
            retry_count=0,
        ),
        MiddlewareChain([]),
    )

    errors = [e for e in _retry_logs(swarmq_logs, "retry_publish_failed") if e["log_level"] == "error"]
    assert errors == [], (
        "ERROR retry_publish_failed ('Retry could not be published; the message is redelivered'), "
        "obwohl die Retry-Kopie bestätigt liegt (SettleFailedAfterPublish); erwartet WARNING wie "
        "dlq_ack_failed_after_publish (E-043, Level-Regel 0.8.0, logging.md:660)"
    )
    assert _retry_logs(swarmq_logs, "task_retrying"), "der Retry ist erfolgt, task_retrying fehlt"
    broker.redeliver.assert_not_awaited()
