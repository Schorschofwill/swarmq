"""Nachweis BZ-21: ``dlq list``/``dlq inspect`` lesen zerstörend.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-21). Das Lesen der
DLQ entnimmt jede Nachricht mit ``get(no_ack=True)`` und stellt sie erst im
``finally`` wieder ein. Stirbt der Lesende dazwischen (Kill, OOM, Netz), sind
die bereits gelesenen Nachrichten weg.

Simulation des Abbruchs: nach drei gelesenen Nachrichten bricht die
Verbindung des Lesenden ab — so, wie sie beim Kill der CLI wegfiele. Ab dann
kann der Lesende nichts mehr zurückstellen. Mit bestätigungspflichtigem
Lesen gäbe der Broker die Nachrichten beim Verbindungsende von selbst zurück.

Erwartet (Nutzersicht): ein abgebrochenes Lesen verliert keine DLQ-Nachricht.

Test-Liste:
- [x] Abbruch mitten im ``list_dead_letters`` lässt die DLQ vollständig (BZ-21)
"""

from __future__ import annotations

import json
import uuid

import aio_pika
import aio_pika.queue
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]

_MESSAGES = 5
_ABORT_AFTER = 3


class _ReaderKilled(ConnectionError):
    """Steht für den Wegfall des lesenden Prozesses."""


@pytest.fixture
async def dlq(rabbitmq_url: str):
    queue = f"bz21-{uuid.uuid4().hex[:10]}"
    setup = RabbitMQBroker(rabbitmq_url)
    await setup.connect()
    await setup.declare_queue(queue)
    for i in range(_MESSAGES):
        body = json.dumps({"task": {"id": f"t-{i}", "queue": queue, "name": "t"}, "args": [], "kwargs": {}})
        await setup.publish(f"{queue}.XQ", body.encode())
    yield setup, queue
    await delete_queue_family(rabbitmq_url, queue)
    await setup.close()


async def test_aborted_dlq_listing_loses_no_message(
    rabbitmq_url: str, dlq: tuple[RabbitMQBroker, str], monkeypatch
) -> None:
    observer, queue = dlq
    assert await observer.dead_letter_depth(queue) == _MESSAGES

    reader = RabbitMQBroker(rabbitmq_url)
    reader.publish_timeout_s = 1.0
    await reader.connect()
    original_get = aio_pika.queue.Queue.get
    reads = 0

    async def get_then_die(self, *args, **kwargs):
        nonlocal reads
        if reads == _ABORT_AFTER:
            # Der Lesende stirbt: seine Verbindung ist weg.
            await reader.close()
            raise _ReaderKilled(f"reader gone after {_ABORT_AFTER} messages")
        reads += 1
        return await original_get(self, *args, **kwargs)

    monkeypatch.setattr(aio_pika.queue.Queue, "get", get_then_die)
    try:
        with pytest.raises(Exception):  # noqa: PT011 - je nach Zeitpunkt schlaegt _ReaderKilled oder der Folgefehler beim Kopieren/Ack auf der toten Verbindung durch
            await reader.list_dead_letters(queue, limit=100)
    finally:
        monkeypatch.undo()
        await reader.close()

    # Unbestaetigte Nachrichten gibt der Broker erst asynchron nach dem
    # Verbindungsende zurueck; unter Last dauert das messbar. Ein echter
    # Verlust bleibt rot, weil die Tiefe dann nie wieder _MESSAGES erreicht.
    remaining = 0

    async def all_back() -> bool:
        nonlocal remaining
        remaining = await observer.dead_letter_depth(queue)
        return remaining == _MESSAGES

    assert await wait_until(all_back, timeout=5.0), (
        f"{_MESSAGES - remaining} of {_MESSAGES} DLQ messages lost after the reader "
        f"died having read {_ABORT_AFTER} of them"
    )
