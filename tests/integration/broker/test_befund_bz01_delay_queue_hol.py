"""Nachweis BZ-01: Head-of-Line-Blocking in der Delay-Queue ``{queue}.DQ``.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-01). Alle
Rückstellungen einer Queue (Retry-Backoff, Lock-/Rate-Limit-Konflikt,
Backend-Defer, Guard, Schedfire) teilen sich eine DQ mit TTL pro Nachricht.
RabbitMQ lässt Nachrichten nur am Kopf verfallen.

Erwartet (Nutzersicht): eine kurze Rückstellung (100 ms) kommt nach ihrer
eigenen Wartezeit zurück, auch wenn vorher eine lange (10 s) in dieselbe
Queue zurückgestellt wurde.

Test-Liste:
- [x] kurze Rückstellung hinter langer kommt zuerst zurück (BZ-01)
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from tests.support.queues import delete_queue_family

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]

_LONG_DELAY_MS = 10_000
_SHORT_DELAY_MS = 100
# Großzügig gegenüber 100 ms, deutlich unter 10 s.
_SHORT_RETURN_BUDGET_S = 3.0


@pytest.fixture
async def broker_and_queue(rabbitmq_url: str):
    b = RabbitMQBroker(rabbitmq_url)
    await b.connect()
    queue = f"bz01-{uuid.uuid4().hex[:10]}"
    await b.declare_queue(queue)
    yield b, queue
    try:
        await b.cancel_consume()
    except Exception:
        pass
    await delete_queue_family(rabbitmq_url, queue)
    await b.close()


@pytest.mark.befund("BZ-01")
@pytest.mark.xfail(
    strict=True,
    reason=(
        "BZ-01: eine kurze Rückstellung hängt in {queue}.DQ hinter einer "
        "früher eingestellten langen fest (TTL verfällt nur am Kopf)"
    ),
)
async def test_short_delay_returns_before_earlier_long_delay(broker_and_queue) -> None:
    broker, queue = broker_and_queue
    first_round: asyncio.Queue[Delivery] = asyncio.Queue()
    returned: asyncio.Queue[Delivery] = asyncio.Queue()

    async def on_delivery(delivery: Delivery) -> None:
        if delivery.headers.get("x-bz01-returned"):
            await broker.ack(delivery)
            await returned.put(delivery)
        else:
            await first_round.put(delivery)

    await broker.publish(queue, b"A")
    await broker.publish(queue, b"B")
    await broker.consume(queue, on_delivery, concurrency=10)
    first = await asyncio.wait_for(first_round.get(), timeout=5)
    second = await asyncio.wait_for(first_round.get(), timeout=5)
    by_body = {first.body: first, second.body: second}

    # A zuerst lang, dann B kurz — in dieselbe {queue}.DQ.
    await broker.retry_later(by_body[b"A"], delay_ms=_LONG_DELAY_MS, headers={"x-bz01-returned": 1})
    await broker.retry_later(by_body[b"B"], delay_ms=_SHORT_DELAY_MS, headers={"x-bz01-returned": 1})

    try:
        back = await asyncio.wait_for(returned.get(), timeout=_SHORT_RETURN_BUDGET_S)
    except TimeoutError:
        pytest.fail(
            f"B (delay {_SHORT_DELAY_MS} ms) did not return within {_SHORT_RETURN_BUDGET_S}s — "
            f"it is stuck behind A (delay {_LONG_DELAY_MS} ms) at the head of {queue}.DQ"
        )
    assert back.body == b"B", f"expected the short delay B first, got {back.body!r}"
