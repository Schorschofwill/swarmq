"""Nachweis BZ-06: Unbekannter Task-Name geht sofort in die DLQ.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-06).

``WorkerPipeline._lookup_task_or_route``: ``TaskNotFoundError`` ->
``route_via_dlx(reason="unregistered_task")`` beim ersten Kontakt. Beim
Rolling Deploy (neuer Producer, alter Worker) haette ein neuer Worker die
Nachricht Sekunden spaeter verarbeitet; stattdessen liegt sie ohne Ergebnis
und ohne Ablaufzeit im ``.XQ`` und braucht ``dlq retry`` von Hand.

Testliste:
# [x] erste Zustellung eines unbekannten Tasks: Rueckstellung (Gnadenfrist),
#     kein DLX-Routing
"""

from __future__ import annotations

import pytest

from swarmq.registry import TaskRegistry
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import AddTask, _default_payload

pytestmark = pytest.mark.timeout(5)


@pytest.mark.befund("BZ-06")
async def test_first_delivery_of_an_unknown_task_is_deferred_not_dead_lettered(broker, backend, config) -> None:
    # Alter Worker: kennt nur ``add``; ein neuer Producer schickt ``new_task``.
    registry = TaskRegistry()
    registry.register(AddTask)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    message = make_message(_default_payload(task_id="bz06-task", name="new_task"))
    message.queue = "default"

    await worker._handle_message(message)

    assert broker.route_via_dlx.await_count == 0, "unbekannter Task sofort per DLX ins .XQ geroutet"
    assert broker.dead_letter.await_count == 0
    broker.retry_later.assert_awaited_once()
