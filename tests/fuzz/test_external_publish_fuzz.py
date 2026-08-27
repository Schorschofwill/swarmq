"""Fuzz: external AMQP + Valkey publishers cannot crash the worker.

E2E-Hardening Phase 3 U3.7.

Two scenarios against the full stack:

1. ``test_amqp_random_headers_no_crash`` — a direct ``aio_pika``
   publisher sends messages with random header dicts and random
   bodies directly onto the task queue. The worker must drop each
   one (DLX or nack) and keep its consume loop alive. After the
   fuzz burst we schedule a valid canary task through the normal
   client API; it must execute end-to-end.
2. ``test_pubsub_random_payloads_dropped_by_watch_progress`` — a
   direct valkey PUBLISH targets the progress channel
   ``sq:progress:channel:{task_id}`` with random bytes while
   ``watch_progress`` is iterating. The iterator must drop each
   garbled frame silently and remain iterable; a real progress
   update published via ``set_progress`` after the noise must still
   reach the subscriber.

Both use ``chaos_stack`` so each test class owns an isolated
RabbitMQ + Valkey container pair. Hypothesis is bounded with
``@settings(max_examples=20)`` because full-stack roundtrips are
expensive.

Run via::

    SWARMQ_TEST_RABBITMQ_PORT=5673 SWARMQ_TEST_VALKEY_PORT=6380 \
        uv run --extra test pytest tests/fuzz/test_external_publish_fuzz.py \
        -v --tb=short -m fuzz
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import uuid
from typing import Any

import aio_pika
import pytest
import valkey.asyncio as valkey_asyncio
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task, TaskInfo
from tests.chaos.conftest import ChaosStack

pytestmark = [pytest.mark.timeout(300)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_queue() -> str:
    return f"fuzz-{uuid.uuid4().hex[:10]}"


_SAFE_HEADER_CHARS = (
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789_-"
)


def _make_random_headers(rng: random.Random) -> dict[str, Any]:
    """Draw a single AMQP-compatible random header dict.

    We deliberately restrict value types to those aio_pika's header
    codec accepts (str/int/bytes) so the publish does NOT fail client-
    side — the worker is what we want to exercise, not the codec.
    """
    out: dict[str, Any] = {}
    for _ in range(rng.randint(0, 5)):
        key_len = rng.randint(1, 12)
        key = "".join(rng.choices(_SAFE_HEADER_CHARS, k=key_len))
        kind = rng.randint(0, 2)
        if kind == 0:
            out[key] = "".join(rng.choices(_SAFE_HEADER_CHARS, k=rng.randint(0, 24)))
        elif kind == 1:
            out[key] = rng.randint(-(2**31), 2**31 - 1)
        else:
            out[key] = bytes(rng.randint(0, 255) for _ in range(rng.randint(0, 24)))
    return out


def _make_random_body(rng: random.Random) -> bytes:
    """Random bytes body — never parseable as swarmq JSON."""
    size = rng.randint(0, 256)
    return bytes(rng.randint(0, 255) for _ in range(size))


class TestAmqpHeaderFuzz:
    """Fuzz random header dicts + bodies into a live worker's queue.

    One ``chaos_stack`` for the whole class so the worker boots once.
    Mirrors the setup pattern from
    ``tests/chaos/test_resource_exhaustion.py`` (async-with + sleep
    before publish) to avoid the prefetch / QoS race.
    """

    async def test_amqp_random_headers_no_crash(
        self, chaos_stack: ChaosStack,
    ) -> None:
        """Random AMQP header dicts + random bodies go to the worker.

        Invariant: the worker's consume loop remains alive through an
        arbitrary burst of malformed messages, and a valid task
        scheduled afterwards completes normally.
        """
        queue = _unique_queue()
        canary_name = f"fuzz_canary_{uuid.uuid4().hex[:8]}"

        class CanaryTask(Task):
            task_info = TaskInfo(name=canary_name, queues=[queue], max_retries=0)

            async def run(self, x: int) -> int:
                return x + 1

        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        backend = ValkeyBackend(chaos_stack.valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)

            worker_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            worker_backend = ValkeyBackend(chaos_stack.valkey_url)
            worker_app = SwarmQ(
                worker_broker, worker_backend,
                queues=[queue], concurrency=1,
            )
            worker_task = asyncio.create_task(worker_app.start_worker())

            try:
                # Give the worker time to connect + attach consumer
                # before we publish anything — otherwise the first
                # message may race prefetch application.
                await asyncio.sleep(0.8)

                # Fuzz burst: a handful of random-header + random-body
                # messages directly onto the queue. Seeded RNG for
                # reproducibility. Publish one-at-a-time, each via its
                # own connection, waiting between publishes so the
                # worker has time to deserialize-fail + nack + DLX-
                # route before the next arrival. Sharing one connection
                # across the burst has been observed to starve the
                # consumer under concurrent publish pressure in
                # aio_pika; the per-message connection is slow but
                # unambiguously safe.
                rng = random.Random(0xFACE_B00C)
                for _ in range(6):
                    headers = _make_random_headers(rng)
                    body = _make_random_body(rng)
                    conn = await aio_pika.connect(chaos_stack.rabbitmq_url)
                    try:
                        ch = await conn.channel()
                        try:
                            msg = aio_pika.Message(
                                body=body,
                                headers=headers,
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            )
                            await ch.default_exchange.publish(
                                msg, routing_key=queue,
                            )
                        except Exception:
                            # Some header codec edges still slip
                            # through — skip those; they never reach
                            # the worker.
                            pass
                    finally:
                        await conn.close()
                    await asyncio.sleep(0.4)

                # Let the worker drain any straggler.
                await asyncio.sleep(1.0)

                # Canary: a well-formed task must still succeed post-fuzz.
                tid = await app.schedule(canary_name, 41)
                result = await app.get_result(tid, timeout=30)
                assert result == 42, (
                    f"post-fuzz canary failed: got {result!r}"
                )
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await worker_task
                with contextlib.suppress(Exception):
                    await worker_broker.close()
                with contextlib.suppress(Exception):
                    await worker_backend.close()


# ---------------------------------------------------------------------------
# Pub/Sub fuzz
# ---------------------------------------------------------------------------


class TestPubsubPayloadFuzz:
    """Fuzz the progress Pub/Sub channel while a subscriber iterates."""

    @given(payload=st.binary(max_size=512))
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[
            HealthCheck.too_slow,
            HealthCheck.function_scoped_fixture,
        ],
    )
    def test_pubsub_random_payloads_dropped_by_watch_progress(
        self, chaos_stack: ChaosStack, payload: bytes,
    ) -> None:
        """Random bytes on the progress channel must be silently dropped.

        Strategy: stand up a ``Client.watch_progress`` iterator,
        publish `payload` directly via a raw valkey connection, then
        publish one well-formed progress event and assert the iterator
        surfaces exactly the valid event.
        """

        async def _run() -> None:
            backend = ValkeyBackend(chaos_stack.valkey_url)
            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            app = SwarmQ(broker, backend, queues=[_unique_queue()])
            task_id = f"fuzz-pubsub-{uuid.uuid4().hex[:8]}"
            async with app:
                iterator = app.client.watch_progress(task_id).__aiter__()

                # Give the iterator a chance to register its pubsub.
                # We trigger the inner subscribe by running one step
                # with a short timeout; on the get_message path the
                # iterator will return nothing and loop.
                probe_task = asyncio.create_task(iterator.__anext__())
                await asyncio.sleep(0.4)

                # Raw publish of the fuzz payload.
                raw = valkey_asyncio.Valkey.from_url(chaos_stack.valkey_url)
                try:
                    channel = backend.progress_channel_name(task_id)
                    await raw.publish(channel, payload)
                    await asyncio.sleep(0.15)

                    # Then a valid update — watch_progress should
                    # surface ONLY this one.
                    valid = {
                        "percent": 50.0,
                        "current": 5,
                        "total": 10,
                        "message": "halfway",
                        "updated_at": "2026-04-17T00:00:00+00:00",
                        "metadata": {},
                    }
                    await raw.publish(
                        channel, json.dumps(valid).encode("utf-8"),
                    )
                finally:
                    await raw.aclose()

                try:
                    info = await asyncio.wait_for(probe_task, timeout=4.0)
                except TimeoutError:
                    probe_task.cancel()
                    with contextlib.suppress(Exception):
                        await probe_task
                    pytest.fail(
                        "watch_progress did not yield the valid update; "
                        f"fuzz payload {payload!r} likely broke the iterator"
                    )
                else:
                    assert info.percent == 50.0, (
                        f"iterator surfaced wrong event: {info!r}"
                    )
                    # Close the async generator cleanly.
                    with contextlib.suppress(Exception):
                        await iterator.aclose()

        asyncio.run(_run())
