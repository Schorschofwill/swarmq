"""Integration tests for RabbitMQBroker connection reconnect behavior (U1.1).

The 2026-04-17 toxiproxy spike found that aio_pika's RobustChannel
survives a clean `connection.reconnect()` with a short unusable-window,
but operations initiated during that window fail with CancelledError
or ChannelInvalidStateError. Spike evidence:
docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md

These tests reproduce the failure in-process by calling the public
`await connection.reconnect()` API on the underlying RobustConnection
(no toxiproxy required — validates the pure aio_pika reopen path).
The U2.x suite will add toxiproxy-based variants as end-to-end
regression once the chaos-compose-stack lands.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker

pytestmark = [pytest.mark.timeout(20)]


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def broker(rabbitmq_url: str):
    b = RabbitMQBroker(rabbitmq_url)
    await b.connect()
    declared: list[str] = []
    yield b, declared
    # Cleanup
    if b._connection is not None and not b._connection.is_closed:
        for qname in declared:
            try:
                ch = await b._connection.channel()
                q = await ch.get_queue(qname, ensure=False)
                await q.delete(if_empty=False, if_unused=False)
                await ch.close()
            except Exception:
                pass
        await b.close()


async def test_publish_succeeds_after_connection_reconnect(broker):
    """After an explicit reconnect, publish must work on the next call.

    Baseline sanity: the channel eventually reopens on its own. This
    test pins the contract even without concurrent publishes — a
    regression here means the reopen path itself is broken.
    """
    b, declared = broker
    q = _unique("reconnect-basic")
    declared.append(q)
    await b.declare_queue(q)
    await b.publish(q, b"before")

    await b._connection.reconnect()
    # The channel is briefly is_closed=True after reconnect — let
    # aio_pika settle before the next op.
    await asyncio.sleep(0.3)

    # This MUST succeed — channel auto-reopens or our _ensure_channel
    # guard recreates it.
    await b.publish(q, b"after")


async def test_publish_during_reconnect_window_waits_for_channel(broker):
    """Publishing while the channel is mid-reopen must not raise.

    This is the bug the spike surfaced: a publish initiated while
    RobustChannel.is_closed=True crashed with CancelledError /
    ChannelInvalidStateError. The fix has the broker block on
    channel readiness (bounded by op_timeout) instead of failing.
    """
    b, declared = broker
    q = _unique("reconnect-window")
    declared.append(q)
    await b.declare_queue(q)
    await b.publish(q, b"pre-outage")

    # Kick off reconnect without awaiting — publish attempts happen
    # while the channel is transitioning.
    reconnect_task = asyncio.create_task(b._connection.reconnect())
    try:
        results: list[tuple[int, bool, str]] = []
        for i in range(10):
            try:
                await b.publish(q, f"in-window-{i}".encode())
                results.append((i, True, ""))
            except Exception as e:
                results.append((i, False, f"{type(e).__name__}: {e}"))
            await asyncio.sleep(0.05)
    finally:
        await reconnect_task

    failures = [r for r in results if not r[1]]
    assert not failures, (
        f"expected every in-window publish to succeed (broker should "
        f"wait for channel reopen); got {len(failures)}/10 failures: "
        f"{failures[:3]!r}"
    )


async def test_consume_survives_connection_reconnect(broker, rabbitmq_url):
    """An active consumer must keep receiving messages after reconnect.

    Covers the consume_channel recreate path. A producer on a separate
    broker instance publishes messages; the primary consumer broker
    triggers a reconnect mid-flow. After the reconnect window, further
    publishes must still be delivered.
    """
    b, declared = broker
    q = _unique("reconnect-consume")
    declared.append(q)
    await b.declare_queue(q)

    received: list[bytes] = []

    async def handler(msg) -> None:
        received.append(msg.body)
        await b.ack(msg)

    await b.consume(q, handler, concurrency=10)

    # Publish before outage
    producer = RabbitMQBroker(rabbitmq_url)
    await producer.connect()
    try:
        for i in range(3):
            await producer.publish(q, f"pre-{i}".encode())
        await asyncio.sleep(0.5)
        pre_count = len(received)
        assert pre_count == 3, f"baseline: got {received}"

        # Force reconnect on the consumer
        await b._connection.reconnect()
        await asyncio.sleep(1.0)  # let consumer re-attach

        # Publish after outage
        for i in range(3):
            await producer.publish(q, f"post-{i}".encode())
        await asyncio.sleep(1.5)

        post_count = len(received) - pre_count
        assert post_count == 3, (
            f"consumer lost post-reconnect messages: received "
            f"{len(received)} total, expected 6; got {received!r}"
        )
    finally:
        await producer.close()


async def test_qos_prefetch_preserved_across_reconnect(broker, rabbitmq_url):
    """Prefetch setting must be reapplied after channel recreate.

    Without the fix, QoS is silently lost after reconnect — the
    consumer ends up in a different prefetch regime without the caller
    knowing. Test pins the invariant by publishing 2× prefetch messages
    and verifying that only prefetch-many are delivered until each is
    acked.
    """
    b, declared = broker
    q = _unique("reconnect-qos")
    declared.append(q)
    await b.declare_queue(q)

    pending_acks: list = []

    async def slow_handler(msg) -> None:
        # Intentionally do NOT ack — hold the message to stress prefetch.
        pending_acks.append(msg)

    await b.consume(q, slow_handler, concurrency=2)

    producer = RabbitMQBroker(rabbitmq_url)
    await producer.connect()
    try:
        # Burn the prefetch with 2 pre-reconnect messages
        for i in range(2):
            await producer.publish(q, f"p{i}".encode())
        await asyncio.sleep(0.5)
        assert len(pending_acks) == 2

        # Reconnect
        await b._connection.reconnect()
        await asyncio.sleep(1.0)

        # Release the old un-acked messages (they were redelivered on
        # reconnect anyway, we just want a clean slate)
        for m in pending_acks:
            try:
                await b.ack(m)
            except Exception:
                pass
        pending_acks.clear()

        # Publish 4 more; prefetch=2 should hold back 2
        for i in range(4):
            await producer.publish(q, f"q{i}".encode())
        await asyncio.sleep(1.0)

        # With prefetch=2 (re-applied after reconnect) we should see
        # exactly 2 un-acked holds; if QoS was lost we'd see 4.
        assert len(pending_acks) == 2, (
            f"QoS prefetch=2 not preserved after reconnect: "
            f"pending={len(pending_acks)}"
        )

        # Drain
        for m in pending_acks:
            await b.ack(m)
    finally:
        await producer.close()
