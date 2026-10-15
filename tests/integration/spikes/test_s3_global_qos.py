"""Spike S3 (MR 18, BZ-20): globales QoS beim Consume einer Quorum-Queue.

Beobachtet:
- 4.2.9: ``basic.qos(global)`` geht durch, das Consume einer Quorum-Queue
  schließt die **Verbindung** mit 540 ``NOT_IMPLEMENTED - queue '<q>' in
  vhost '/' does not support global qos`` (``ConnectionNotImplemented``).
- 4.3.6: kein Fehler, aber auch keine Wirkung: mit globalem Prefetch 1 und
  zwei Consumern auf zwei Quorum-Queues hält der Channel zwei unbestätigte
  Nachrichten (der Prefetch gilt je Consumer).

Test-Liste:
- [x] globales QoS + Consume einer Quorum-Queue, je Version
"""

from __future__ import annotations

from collections.abc import Callable

import aio_pika
import pytest
from aio_pika.abc import AbstractConnection, AbstractIncomingMessage

from tests.integration.spikes._broker import QUORUM, declare, publish
from tests.support.known_leaks import aiormq_streamwriter_leak
from tests.support.services import RabbitMQNode
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.spike]


# TS-02: der Server schliesst die Verbindung (tests/support/known_leaks.py).
@aiormq_streamwriter_leak
async def test_global_qos_on_quorum_consume(
    node: RabbitMQNode, version: str, amqp: AbstractConnection, queue_name: Callable[[str], str]
) -> None:
    queues = [queue_name("s3-a"), queue_name("s3-b")]
    for queue in queues:
        assert await declare(amqp, queue, QUORUM) is None
        for _ in range(3):
            await publish(amqp, queue, aio_pika.Message(b"s3"))

    connection = await aio_pika.connect(node.amqp_url)
    received: list[AbstractIncomingMessage] = []
    error: str | None = None
    try:
        assert connection.transport is not None
        underlying = connection.transport.connection
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1, global_=True)

        async def keep(message: AbstractIncomingMessage) -> None:
            received.append(message)

        try:
            for queue in queues:
                await (await channel.get_queue(queue, ensure=False)).consume(keep)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc.args[0]}"
        # Prefetch-Auslieferungen meldet kein Ereignis; die Zahl muss stehen bleiben.
        await wait_until(lambda: len(received) > 2, timeout=1.0)
        connection_closed = underlying.is_closed
    finally:
        if not connection.is_closed:
            await connection.close()

    if version == "4.2":
        assert error == (
            f"ConnectionNotImplemented: NOT_IMPLEMENTED - queue '{queues[0]}' in vhost '{node.vhost}' does not support global qos"
        )
        assert connection_closed
        assert received == []
    else:
        assert error is None
        assert not connection_closed
        assert len(received) == 2, "global prefetch 1 is ignored: one unacked message per consumer"
