"""Integration tests for RabbitMQBroker against a real RabbitMQ instance."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker


def _unique(prefix: str) -> str:
    """Return a queue name with a random suffix to avoid collisions."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def broker(rabbitmq_url: str):
    """Yield a connected RabbitMQBroker; close + clean up queues on teardown."""
    b = RabbitMQBroker(rabbitmq_url)
    await b.connect()
    declared_queues: list[str] = []

    # Expose a helper so tests can register queues for cleanup.
    b._declared_queues = declared_queues  # type: ignore[attr-defined]

    yield b

    # Teardown: delete every queue that was declared during the test.
    for qname in declared_queues:
        try:
            ch = await b._connection.channel()
            q = await ch.get_queue(qname, ensure=False)
            await q.delete(if_empty=False, if_unused=False)
            await ch.close()
        except Exception:  # noqa: BLE001
            pass
    await b.close()


# -- helpers -----------------------------------------------------------------

async def _declare_and_track(broker: RabbitMQBroker, queue_name: str) -> None:
    """Declare the queue triplet and register all three names for cleanup."""
    await broker.declare_queue(queue_name)
    broker._declared_queues.extend(  # type: ignore[attr-defined]
        [queue_name, f"{queue_name}.DQ", f"{queue_name}.XQ"]
    )


# -- tests -------------------------------------------------------------------


class TestConnectDisconnect:
    async def test_connect_and_close(self, broker: RabbitMQBroker) -> None:
        """connect() establishes a connection; close() tears it down."""
        assert broker._connection is not None
        assert not broker._connection.is_closed
        await broker.close()
        assert broker._connection is None


class TestDeclareQueue:
    async def test_creates_three_queues(self, broker: RabbitMQBroker) -> None:
        """declare_queue creates main, delay (.DQ), and dead-letter (.XQ) queues."""
        qname = _unique("test_declare")
        await _declare_and_track(broker, qname)

        # Publishing to each queue verifies it exists and accepts messages.
        for suffix in ("", ".DQ", ".XQ"):
            await broker.publish(f"{qname}{suffix}", b"probe")


class TestPublishConsumeAck:
    async def test_roundtrip(self, broker: RabbitMQBroker) -> None:
        """Publish a message, consume it, verify content, and ack."""
        qname = _unique("test_pub_ack")
        await _declare_and_track(broker, qname)

        payload = b'{"task": "hello"}'
        await broker.publish(qname, payload)

        received: asyncio.Future = asyncio.get_event_loop().create_future()

        async def _on_message(msg) -> None:
            if not received.done():
                received.set_result(msg)

        await broker.consume(qname, _on_message, prefetch_count=1)
        msg = await asyncio.wait_for(received, timeout=5.0)

        assert msg.body == payload
        await broker.ack(msg)


class TestNackRequeue:
    async def test_nack_requeue_redelivers(self, broker: RabbitMQBroker) -> None:
        """nack(requeue=True) makes the message available for re-consumption."""
        qname = _unique("test_nack")
        await _declare_and_track(broker, qname)

        payload = b"retry-me"
        await broker.publish(qname, payload)

        # Collect deliveries via a single consumer callback that nacks the
        # first delivery and acks the second.
        deliveries: list[asyncio.Future] = [
            asyncio.get_event_loop().create_future(),
            asyncio.get_event_loop().create_future(),
        ]
        delivery_count = 0

        async def _on_message(msg) -> None:
            nonlocal delivery_count
            idx = delivery_count
            delivery_count += 1
            if idx == 0:
                # First delivery: nack with requeue.
                deliveries[0].set_result(msg)
                await broker.nack(msg, requeue=True)
            elif idx == 1:
                # Second delivery: ack and record.
                deliveries[1].set_result(msg)
                await broker.ack(msg)

        await broker.consume(qname, _on_message, prefetch_count=1)

        msg1 = await asyncio.wait_for(deliveries[0], timeout=5.0)
        msg2 = await asyncio.wait_for(deliveries[1], timeout=5.0)

        assert msg1.body == payload
        assert msg2.body == payload
