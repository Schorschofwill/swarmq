"""Spike S1 (Plan 2026-10-12-1105, MR 8, E6): Kopie eines Bodys ueber ``max_message_body_bytes`` nach ``.XQ``.

Beobachtet auf 4.3.6 (5699; der 4.2.9-Container auf 5742 lief nicht):

- Broker-Grenze: ``max_message_size`` = 16777216 (16 MiB, ``rabbitmqctl eval
  'application:get_env(rabbit, max_message_size).'``), gleich dem Default von
  ``SWARMQ_MAX_MESSAGE_BODY_BYTES``. Genau 16 MiB nimmt der Broker an, 16 MiB + 1
  schliesst den Kanal mit 406 ``PRECONDITION_FAILED - message size 16777217 is
  larger than configured max size 16777216``. ``body_too_large`` kann also nur
  auftreten, wenn der swarmq-Deckel unter der Broker-Grenze liegt.
- Deckel 8 MiB, Body 8 MiB + 1 (JSON mit ``task.id``): ``dead_letter`` legt die
  Kopie mit Ablaufzeit, ``x-swarmq-dead-lettered-at`` und Generation nach
  ``.XQ`` in rund 30 ms; ``dead_letter_generation`` (``json.loads`` des
  ganzen Bodys) braucht rund 10 ms und rund das Doppelte des Bodys an Speicher.
- Kopie ueber der Broker-Grenze: der Publish endet nach rund 20 ms als
  ``PublishIndeterminate`` (der Broker schliesst den Publish-Kanal nach dem
  Senden). Der Consume-Kanal bleibt offen: ``route_via_dlx`` auf dem Original
  legt es ueber den DLX nach ``.XQ`` (``x-death``, ohne Ablaufzeit), eine
  Neuzustellung bleibt aus, der naechste Publish gelingt.

Ergebnis: S1 besteht, E6 = A.

Test-Liste:
- [x] Broker-Grenze ``max_message_size``: genau die Grenze wird angenommen, Grenze + 1 abgelehnt
- [x] Body ``cap + 1`` (cap unter der Broker-Grenze) per ``RabbitMQBroker.dead_letter`` als Kopie nach ``.XQ``:
      Ablaufzeit, Stempel, Original bestaetigt; Dauer und Speicher von ``dead_letter_generation``
- [x] Kopie ueber der Broker-Grenze: der Publish scheitert, der Consume-Kanal bleibt, der DLX-Rueckfall greift
"""

from __future__ import annotations

import contextlib
import time
import tracemalloc
from collections.abc import AsyncIterator, Callable

import aio_pika
import pytest
from aio_pika.abc import AbstractConnection
from aio_pika.exceptions import ChannelPreconditionFailed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts.message import Delivery
from swarmq.exceptions import PublishIndeterminate
from swarmq.headers import HEADER_DEAD_LETTERED_AT
from swarmq.identity import dead_letter_generation
from tests.contracts._support import consume_into
from tests.integration.spikes._broker import QUORUM, declare, depth, get_message, publish
from tests.support.queues import delete_queue_family
from tests.support.services import RabbitMQNode

pytestmark = [pytest.mark.integration, pytest.mark.spike, pytest.mark.timeout(60)]

_MIB = 1024 * 1024
_BROKER_MAX = 16 * _MIB
_CAP = 8 * _MIB
_TTL_S = 3600.0


def _json_body(size: int) -> bytes:
    """swarmq-foermiger JSON-Body von genau *size* Bytes (ein grosses String-Argument)."""
    head = b'{"task": {"id": "t-big", "name": "big", "queue": "q"}, "args": ["'
    tail = b'"], "kwargs": {}}'
    return head + b"x" * (size - len(head) - len(tail)) + tail


@pytest.fixture
async def broker(node: RabbitMQNode, queue_name: Callable[[str], str]) -> AsyncIterator[tuple[RabbitMQBroker, str]]:
    impl = RabbitMQBroker(node.amqp_url)
    impl.dead_letter_ttl_s = _TTL_S
    await impl.connect()
    queue = queue_name("p5b")
    await impl.declare_queue(queue)
    yield impl, queue
    with contextlib.suppress(Exception):
        await impl.cancel_consume()
    await delete_queue_family(node.amqp_url, queue)
    await impl.close()


async def test_broker_max_message_size_is_16_mib(amqp: AbstractConnection, queue_name: Callable[[str], str]) -> None:
    queue = queue_name("p5b-max")
    assert await declare(amqp, queue, QUORUM) is None

    await publish(amqp, queue, aio_pika.Message(b"x" * _BROKER_MAX))
    with pytest.raises(ChannelPreconditionFailed, match="larger than configured max size 16777216"):
        await publish(amqp, queue, aio_pika.Message(b"x" * (_BROKER_MAX + 1)))
    assert await depth(amqp, queue) == 1


async def test_copy_of_body_over_cap_lands_in_xq_with_expiration(
    broker: tuple[RabbitMQBroker, str], amqp: AbstractConnection
) -> None:
    impl, queue = broker
    body = _json_body(_CAP + 1)
    await publish(amqp, queue, aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT))
    inbox = await consume_into(impl, queue)
    delivery = await inbox.next()

    tracemalloc.start()
    try:
        dead_letter_generation(delivery.headers, delivery.body, 0)
        _current, generation_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    start = time.perf_counter()
    await impl.dead_letter(delivery, headers={**delivery.headers, "x-swarmq-traceback": "body_too_large: spike"})
    dead_letter_s = time.perf_counter() - start

    copy = await get_message(amqp, f"{queue}.XQ")
    assert copy.body == body
    assert copy.expiration == _TTL_S
    assert HEADER_DEAD_LETTERED_AT in (copy.headers or {})
    assert (copy.headers or {})["x-swarmq-traceback"] == "body_too_large: spike"
    await inbox.expect_nothing()
    assert await depth(amqp, queue) == 0
    # Aufwand der Generation: ein json.loads des Bodys (gemessen ~2x Body, ~10 ms).
    assert generation_peak < 3 * len(body)
    assert dead_letter_s < 5.0


async def test_copy_over_broker_limit_fails_and_consume_channel_survives(
    broker: tuple[RabbitMQBroker, str], amqp: AbstractConnection
) -> None:
    impl, queue = broker
    await publish(amqp, queue, aio_pika.Message(_json_body(1024), delivery_mode=aio_pika.DeliveryMode.PERSISTENT))
    inbox = await consume_into(impl, queue)
    real = await inbox.next()
    oversized = Delivery(body=b"x" * (_BROKER_MAX + 1), headers=real.headers, queue=real.queue, handle=real.handle)

    with pytest.raises(PublishIndeterminate, match="larger than configured max size"):
        await impl.dead_letter(oversized, headers=dict(real.headers))

    await impl.route_via_dlx(real)
    via_dlx = await get_message(amqp, f"{queue}.XQ")
    assert "x-death" in (via_dlx.headers or {})
    assert via_dlx.expiration is None
    await inbox.expect_nothing()
    await impl.publish(queue, _json_body(512))
    assert (await inbox.next()).body == _json_body(512)
