"""Nachweis BZ-16: Publish und Konsum teilen eine Verbindung.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-16). Bei einem
Ressourcenalarm (Speicher/Platte) hört RabbitMQ auf, vom Socket einer
Verbindung zu lesen, sobald darüber publiziert wird. Der Worker publiziert
(Retry, DLQ, Defer) und bestätigt über dieselbe Verbindung; RabbitMQ empfiehlt
getrennte Verbindungen. ``connection.blocked`` wird nicht behandelt.

Erwartet (Nutzersicht): während ein Publish im Alarm hängt, kommen Acks
anderer Nachrichten desselben Workers beim Broker an.

Den Speicher-Alarm löst die Fixture ``memory_alarm`` aus
(``tests/integration/broker/conftest.py``); sie setzt die Wasserlinie danach
exakt zurück.

Test-Liste:
- [x] Ack kommt an, während ein Publish desselben Workers im Alarm hängt (BZ-16)
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from tests.integration.broker.conftest import rabbitmq_eval, wait_for_alarm
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

# exclusive: der Speicheralarm blockiert jeden Verleger des Brokers, auch die
# parallelen xdist-Prozesse (tests/support/xdist_isolation.py).
pytestmark = [pytest.mark.integration, pytest.mark.timeout(30), pytest.mark.exclusive]

_ACK_BUDGET_S = 3.0


def _unacked(queue: str) -> int:
    out = rabbitmq_eval(
        f'{{ok,Q}}=rabbit_amqqueue:lookup(rabbit_misc:r(<<"/">>,queue,<<"{queue}">>)),'
        "proplists:get_value(messages_unacknowledged, rabbit_amqqueue:info(Q,[messages_unacknowledged]))."
    )
    return int(out)


async def test_ack_reaches_broker_while_publish_is_blocked_by_alarm(rabbitmq_url: str, memory_alarm) -> None:
    queue = f"bz16-{uuid.uuid4().hex[:10]}"
    # Classic statt Quorum: der Effekt liegt an der Verbindung, nicht am
    # Queue-Typ, und nur Classic-Queues melden unbestätigte Nachrichten live.
    worker = RabbitMQBroker(rabbitmq_url, queue_type="classic")
    worker.publish_timeout_s = 20.0
    await worker.connect()
    deliveries: asyncio.Queue[Delivery] = asyncio.Queue()
    blocked_publish: asyncio.Task[None] | None = None
    try:
        await worker.declare_queue(queue)
        await worker.publish(queue, b"A")
        await worker.publish(queue, b"B")

        async def on_delivery(delivery: Delivery) -> None:
            await deliveries.put(delivery)

        await worker.consume(queue, on_delivery, concurrency=10)
        got = [await asyncio.wait_for(deliveries.get(), timeout=5) for _ in range(2)]
        by_body = {d.body: d for d in got}
        assert await asyncio.to_thread(_unacked, queue) == 2, "precondition: A and B are in flight"

        memory_alarm()
        await wait_for_alarm(active=True)

        # A wird zurückgestellt: der Publish hängt im Alarm.
        blocked_publish = asyncio.create_task(worker.retry_later(by_body[b"A"], delay_ms=60_000, headers={}))
        done, _ = await asyncio.wait({blocked_publish}, timeout=1.0)
        assert not done, "precondition: the retry publish should block during the alarm"

        # B ist fertig und wird bestätigt.
        await worker.ack(by_body[b"B"])

        unacked = 2

        async def ack_arrived() -> bool:
            nonlocal unacked
            unacked = await asyncio.to_thread(_unacked, queue)
            return unacked <= 1

        # Kein Broker-Ereignis meldet ein angekommenes Ack; Pollen.
        await wait_until(ack_arrived, timeout=_ACK_BUDGET_S, interval=0.1)

        assert unacked == 1, (
            f"{unacked} unacked messages in {queue!r} {_ACK_BUDGET_S}s after acking B — "
            "the ack is stuck behind the alarm-blocked publish on the shared connection"
        )
    finally:
        if blocked_publish is not None:
            blocked_publish.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await blocked_publish
        with contextlib.suppress(Exception):
            await asyncio.wait_for(worker.close(), timeout=5)
        # Das Löschen ist kein Publish und läuft auch im Alarm; die Fixture setzt
        # den Alarm erst danach zurück.
        await delete_queue_family(rabbitmq_url, queue)
