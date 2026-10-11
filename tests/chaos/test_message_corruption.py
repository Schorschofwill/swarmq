"""F7 — Message corruption (U2.7).

Plan: docs/plans/2026-04-17-007-feat-e2e-hardening-plan.md U2.7.

Five failure scenarios where garbled bytes enter the system from
*outside* SwarmQ's normal encode path (a rogue external publisher, a
direct ``valkey-cli HSET``, a partial-write corruption). In each case
the system must degrade gracefully — drop/reject the bad input, never
crash the consumer, never surface a generic decode traceback to the
API caller.

All tests bypass :class:`Client` for the *injection* side so the
corruption really is at the wire level:

* Raw AMQP writes via :class:`aio_pika.Message` directly against the
  toxiproxy-wrapped broker URL.
* Raw :class:`valkey.asyncio.Valkey` commands against the toxiproxy-
  wrapped backend URL.

The consumer side (Worker / Client.get_result / Client.watch_progress /
Client.get_progress) is untouched — we're pinning its contract.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from typing import Any

import aio_pika
import pytest
import valkey.asyncio

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from swarmq.exceptions import ResultSerializationError
from swarmq.headers import HEADER_UNIQUE_HASH, HEADER_UNIQUE_UNTIL
from swarmq.task import Task
from tests.chaos.conftest import ChaosStack
from tests.support.worker_subprocess import (
    kill_subprocess,
    spawn_worker,
    wait_for_worker_children,
    worker_children,
)

pytestmark = [pytest.mark.timeout(120)]


# ---------------------------------------------------------------------------
# Subprocess-worker helpers (shared pattern with test_worker_crash.py).
# ---------------------------------------------------------------------------


def _ensure_fixture_registered(queue: str) -> None:
    """Register the chaos fixture tasks in THIS process so ``schedule()``
    accepts task names. Worker subprocess gets the same module via
    ``--module tests.chaos._chaos_fixture_tasks`` + ``CLI_TEST_QUEUE``."""
    os.environ["CLI_TEST_QUEUE"] = queue
    mod_name = "tests.chaos._chaos_fixture_tasks"
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])
    else:
        importlib.import_module(mod_name)


def _spawn_worker(
    stack: ChaosStack,
    queue_name: str,
    *,
    concurrency: int = 2,
) -> subprocess.Popen[str]:
    """Worker subprocess in its own process group (teardown reaps the
    worker children too)."""
    return spawn_worker(
        queue_name,
        fixture_module="tests.chaos._chaos_fixture_tasks",
        broker_url=stack.rabbitmq_url,
        backend_url=stack.valkey_url,
        concurrency=concurrency,
    )


def _wait_for_worker_started(
    proc: subprocess.Popen[str],
    timeout: float = 30.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stdout.readline() if proc.stdout else ""
        if not line:
            if proc.poll() is not None:
                raise RuntimeError(f"worker exited before starting; rc={proc.returncode}")
            continue
        if "worker_started" in line:
            return
    raise TimeoutError("worker did not emit worker_started in time")


def _kill_subprocess(proc: subprocess.Popen[str]) -> None:
    kill_subprocess(proc)


def _child_pids(proc: subprocess.Popen[str]) -> set[int]:
    """PIDs of the supervisor's worker children. The supervisor restarts a
    crashed child, so "supervisor alive" alone does not prove "no crash"."""
    return {child.pid for child in worker_children(proc)}


async def _queue_message_count(stack: ChaosStack, queue: str) -> int:
    """Count messages in *queue* via a passive ``declare_queue``."""
    conn = await aio_pika.connect_robust(stack.rabbitmq_url)
    try:
        ch = await conn.channel()
        decl = await ch.declare_queue(queue, passive=True)
        return decl.declaration_result.message_count
    finally:
        await conn.close()


async def _wait_amqp_reachable(url: str, timeout: float = 30.0) -> None:
    """Retry opening an AMQP connection until it succeeds.

    The ``chaos_stack`` fixture waits for the toxiproxy admin port and
    for ``docker compose`` healthchecks, but the path
    ``client → toxiproxy → rabbitmq`` can still flake the first few
    hundred milliseconds after RabbitMQ reports "healthy" (Erlang VM
    warm-up, AMQP handshake queue not drained yet). Retrying here
    absorbs the race without slowing the happy path — one try is
    usually enough.
    """
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = await aio_pika.connect_robust(url, timeout=3)
            await conn.close()
            return
        except Exception as exc:
            last = exc
            await asyncio.sleep(0.5)
    raise RuntimeError(f"amqp never reachable at {url}: {last!r}")


async def _drain_queue_count(
    stack: ChaosStack,
    queue: str,
    limit: int = 50,
) -> int:
    """Non-destructive count: basic_get + nack(requeue=True)."""
    conn = await aio_pika.connect_robust(stack.rabbitmq_url)
    try:
        ch = await conn.channel()
        q = await ch.declare_queue(queue, passive=True)
        count = 0
        for _ in range(limit):
            msg = await q.get(no_ack=False, fail=False)
            if msg is None:
                break
            count += 1
            await msg.nack(requeue=True)
        return count
    finally:
        await conn.close()


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


# ---------------------------------------------------------------------------
# Test class — one chaos_stack per class (amortise 10s startup).
# ---------------------------------------------------------------------------


class TestMessageCorruption:
    """F7 — five scenarios for garbled external input."""

    # ----- 1. Corrupt AMQP headers ------------------------------------------

    async def test_corrupt_amqp_headers_dlq_rejection(
        self,
        chaos_stack: ChaosStack,
        _cleanup_tasks,
    ) -> None:
        """Raw aio_pika publish with garbage headers + malformed body.

        Contract pinned: the worker must NOT crash, must eventually
        ack-and-discard or route the bad message to ``{queue}.XQ``,
        and must keep consuming subsequent valid messages.

        We publish a message with
          - binary blobs in several ``x-swarmq-*`` header fields,
          - a non-SwarmQ-shaped JSON body (missing ``task`` key),
        then we also schedule a valid task afterwards and confirm the
        worker completes it.
        """
        queue = f"chaos-corrupt-headers-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)
        await _wait_amqp_reachable(chaos_stack.rabbitmq_url)

        # Declare queues BEFORE starting the worker so the broker
        # accepts the raw publish. The worker will declare them too on
        # connect, but we need them ready for the direct inject.
        pre_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        await pre_broker.connect()
        try:
            await pre_broker.declare_queue(queue)
        finally:
            await pre_broker.close()

        # --- inject corrupt message via raw aio_pika ---
        # AMQP header values must be AMQP-supported primitives: str,
        # int, bool, float, list, dict, bytearray (pamqp rejects plain
        # ``bytes``). We stuff pseudo-binary content into ``bytearray``
        # and tangled-shape values into the int-typed retry-count —
        # exactly what a rogue client or a schema-evolution mistake
        # would emit. The body itself is not valid SwarmQ JSON, so the
        # worker must fail at the deserialize step and route via the
        # nack(requeue=False) → DLX path.
        conn = await aio_pika.connect_robust(chaos_stack.rabbitmq_url)
        try:
            ch = await conn.channel()
            bad_msg = aio_pika.Message(
                body=b"\x00\x01\x02 not-json {not really",
                headers={
                    "x-swarmq-retry-count": "not-an-int-either",
                    "x-swarmq-unique-hash": bytearray(
                        b"\x00\x00 garbage hash bytes",
                    ),
                    "x-swarmq-schedfire": bytearray(b"\xde\xad\xbe\xef"),
                    "x-swarmq-random-noise": "\x00\x01\x02",
                },
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await ch.default_exchange.publish(bad_msg, routing_key=queue)
        finally:
            await conn.close()

        # --- start a worker and schedule a valid task ---
        proc = _spawn_worker(chaos_stack, queue, concurrency=2)
        try:
            _wait_for_worker_started(proc)
            children = {c.pid for c in wait_for_worker_children(proc)}

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])
            async with app:
                # The bad message should be picked up + rejected within a
                # couple of seconds.
                await asyncio.sleep(1.5)

                # Worker still alive -- supervisor AND the same worker child
                # (a crashed child would have been restarted).
                assert proc.poll() is None, f"worker crashed on corrupt headers; rc={proc.returncode}"
                assert _child_pids(proc) == children, "worker child crashed on corrupt headers"

                # Valid task still processed end-to-end.
                tid = await app.schedule("chaos_sleep", 0.1, queue=queue)
                result = await app.get_result(tid, timeout=20)
                assert result == "slept:0.1"

                # Main queue must be empty of the garbled msg (it was
                # either acked-and-discarded or dead-lettered).
                main_count = await _queue_message_count(chaos_stack, queue)
                assert main_count == 0, f"main queue not drained; count={main_count}"

                # Pin landing spot. ``_handle_message`` nacks a
                # deserialize-fail with requeue=False, which routes to
                # ``{queue}.XQ`` via the DLX. Accept either outcome so
                # future refactors that ack-and-discard still pass
                # while still documenting current behaviour.
                dlq_count = await _queue_message_count(
                    chaos_stack,
                    f"{queue}.XQ",
                )
                # Current behaviour: 1 (routed to DLQ via nack(requeue=False)
                # + DLX). Document so a future change surfaces here.
                assert dlq_count in (0, 1), f"unexpected DLQ count for corrupt headers: {dlq_count}"

                proc.send_signal(signal.SIGTERM)
                rc = proc.wait(timeout=30)
                assert rc == 0
        finally:
            _kill_subprocess(proc)

    # ----- 2. Corrupt result blob in Valkey ---------------------------------

    async def test_corrupt_result_blob_in_valkey_surfaces_serialization_error(
        self,
        chaos_stack: ChaosStack,
        _cleanup_tasks,
    ) -> None:
        """Overwrite a stored result with garbage → get_result raises
        ResultSerializationError, not a generic crash.

        Pins the msgpack decode boundary in
        :func:`swarmq.serialization.decode_result` / :meth:`Client._unpack`:
        any decode failure must be caught and re-raised as the typed
        SwarmQ exception.
        """
        queue = f"chaos-corrupt-result-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)
        await _wait_amqp_reachable(chaos_stack.rabbitmq_url)

        proc = _spawn_worker(chaos_stack, queue, concurrency=2)
        try:
            _wait_for_worker_started(proc)

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])
            async with app:
                tid = await app.schedule("chaos_sleep", 0.05, queue=queue)
                # Wait until the result lands naturally.
                result = await app.get_result(tid, timeout=15)
                assert result == "slept:0.05"

                # --- overwrite the result key with garbage ---
                raw = valkey.asyncio.Valkey.from_url(chaos_stack.valkey_url)
                try:
                    await raw.set(f"sq:result:{tid}", b"\xff\xff garbage")
                finally:
                    await raw.aclose()

                # Caller path must surface a typed error, not a raw
                # msgpack / codec exception.
                with pytest.raises(ResultSerializationError):
                    await app.get_result(tid, timeout=2, polling=0.05)

                proc.send_signal(signal.SIGTERM)
                rc = proc.wait(timeout=30)
                assert rc == 0
        finally:
            _kill_subprocess(proc)

    # ----- 3. Rogue Pub/Sub publisher does not crash watch_progress ---------

    async def test_rogue_pubsub_publisher_does_not_crash_watch_progress(
        self,
        chaos_stack: ChaosStack,
        _cleanup_tasks,
    ) -> None:
        """Publish garbled bytes to the progress channel while a
        ``watch_progress`` iterator is active.

        Regression for R3.7 fix: :meth:`Client.watch_progress` must
        drop malformed payloads (non-JSON or missing required progress
        fields) and keep iterating. The iterator must subsequently
        yield a valid progress update that lands AFTER the garbled
        payload.
        """
        # No worker needed — we're exercising the pure client-side
        # consumer loop against Valkey directly.
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        # Broker does NOT need to be connected for watch_progress —
        # the iterator only touches the backend.
        client = Client(broker, backend)

        task_id = f"rogue-{uuid.uuid4().hex[:10]}"
        received: list[Any] = []
        iterator_ready = asyncio.Event()
        stop = asyncio.Event()

        async def collect() -> None:
            gen = client.watch_progress(task_id)
            try:
                iterator_ready.set()
                async for info in gen:
                    received.append(info)
                    if stop.is_set():
                        break
            finally:
                with contextlib.suppress(Exception):
                    await gen.aclose()

        collect_task = asyncio.create_task(collect())
        try:
            await iterator_ready.wait()
            # Give the subscriber a generous moment to finish the
            # subscribe + drain handshake before we start injecting.
            await asyncio.sleep(0.5)

            # Separate publisher client — simulates a rogue service.
            pub = valkey.asyncio.Valkey.from_url(chaos_stack.valkey_url)
            channel = backend.progress_channel_name(task_id)
            try:
                # Salvo 1: total garbage (non-JSON bytes).
                await pub.publish(channel, b"\xff\xfe\x00 not json at all")
                # Salvo 2: JSON but missing required progress fields.
                await pub.publish(
                    channel,
                    json.dumps({"oops": "wrong shape"}).encode(),
                )
                # Salvo 3: JSON with wrong types.
                await pub.publish(
                    channel,
                    json.dumps(
                        {"current": "abc", "total": "xyz", "percent": "!"},
                    ).encode(),
                )
                # Give watch_progress a moment to swallow all three.
                await asyncio.sleep(0.4)

                # The subscriber must still be alive — the task must
                # not have raised out of the iterator.
                assert not collect_task.done(), (
                    f"watch_progress crashed on garbled publish: {collect_task.exception()!r}"
                )

                # Salvo 4: a well-shaped publish must now land.
                good_fields = {
                    "current": "5",
                    "total": "10",
                    "percent": "50.0",
                    "message": "mid",
                    "updated_at": "1700000005.0",
                }
                # Set HASH too so a resubscribe-fallback has something
                # to read (belt + braces for the regression).
                await backend.set_progress(
                    task_id,
                    good_fields,
                    ttl_ms=60_000,
                )
                # A handful of publishes — the subscriber loop polls
                # with a 100 ms timeout per iteration so spacing
                # matters less than salvo count.
                for _ in range(5):
                    await pub.publish(
                        channel,
                        json.dumps(good_fields).encode(),
                    )
                    await asyncio.sleep(0.15)
            finally:
                await pub.aclose()

            # Wait up to ~5s for a well-shaped event to reach us.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if any(
                    getattr(info, "message", None) == "mid" or getattr(info, "current", None) == 5 for info in received
                ):
                    break
                await asyncio.sleep(0.1)

            assert received, "subscriber saw no events — watch_progress may have crashed silently on garbled payloads"
            assert any(getattr(info, "current", None) == 5 for info in received), (
                f"post-garbage valid update never delivered; received={received!r}"
            )
        finally:
            stop.set()
            collect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await collect_task
            await backend.close()

    # ----- 4. Corrupt unique-hash header ------------------------------------

    async def test_corrupt_unique_hash_header_rejected_or_normalised(
        self,
        chaos_stack: ChaosStack,
        _cleanup_tasks,
    ) -> None:
        """Valid task body + garbled ``x-swarmq-unique-hash`` header.

        Pins current worker behaviour: a bad unique-hash header value
        is forwarded to :meth:`ValkeyBackend.release_unique` on the
        terminal path; the underlying Lua script does a no-op DEL when
        the key doesn't exist, so the task completes normally. The
        dedup flag machinery is owner-check-safe and treats the
        unknown hash as "already released" — no crash, no DLQ.

        Alternative observed outcomes the assertion tolerates:
          * Worker treats the message as a valid regular task and
            produces a result (current behaviour).
          * Worker rejects the message and it lands in the DLQ (a
            stricter future policy).

        Either outcome is acceptable; what's NOT acceptable is the
        worker crashing or looping.
        """
        queue = f"chaos-bad-unique-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)
        await _wait_amqp_reachable(chaos_stack.rabbitmq_url)

        # Declare queues so the direct publish has somewhere to land.
        pre_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        await pre_broker.connect()
        try:
            await pre_broker.declare_queue(queue)
        finally:
            await pre_broker.close()

        # --- craft a message the worker will accept as a real task ---
        task_id = str(uuid.uuid4())
        body = json.dumps(
            {
                "task": {
                    "id": task_id,
                    "queue": queue,
                    "name": "chaos_sleep",
                },
                "args": [0.05],
                "kwargs": {},
            }
        ).encode()

        conn = await aio_pika.connect_robust(chaos_stack.rabbitmq_url)
        try:
            ch = await conn.channel()
            msg = aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                headers={
                    HEADER_UNIQUE_HASH: "not-a-real-hash",
                    HEADER_UNIQUE_UNTIL: "completion",
                },
            )
            await ch.default_exchange.publish(msg, routing_key=queue)
        finally:
            await conn.close()

        # --- start a worker and poll for the result ---
        proc = _spawn_worker(chaos_stack, queue, concurrency=2)
        try:
            _wait_for_worker_started(proc)
            children = {c.pid for c in wait_for_worker_children(proc)}

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])
            async with app:
                # Accept either outcome:
                #  (a) task completes → get_result returns "slept:0.05"
                #  (b) task routed to DLQ → get_result times out
                try:
                    result = await app.get_result(
                        task_id,
                        timeout=8,
                        polling=0.1,
                    )
                    pinned_outcome = "accepted"
                    assert result == "slept:0.05", f"unexpected result for bad-unique-hash task: {result!r}"
                except Exception as exc:
                    # ResultNotReadyError is the expected timeout signal
                    # when the message was routed to DLQ instead.
                    pinned_outcome = "rejected"
                    assert "not ready" in str(exc).lower() or (type(exc).__name__ == "ResultNotReadyError"), (
                        f"unexpected error shape: {type(exc).__name__}: {exc}"
                    )

                # Worker still alive regardless of outcome -- supervisor AND
                # the same worker child.
                assert proc.poll() is None, f"worker crashed on bad unique-hash; rc={proc.returncode}"
                assert _child_pids(proc) == children, "worker child crashed on bad unique-hash"

                # Document pinned outcome via a marker — future
                # refactors that change this will fail here and force
                # a plan update.
                if pinned_outcome == "rejected":
                    dlq = await _queue_message_count(
                        chaos_stack,
                        f"{queue}.XQ",
                    )
                    assert dlq >= 1, "pinned 'rejected' but nothing in DLQ"

                # Also verify a subsequent valid schedule() completes
                # normally — no wedging from the bad predecessor.
                tid2 = await app.schedule(
                    "chaos_sleep",
                    0.05,
                    queue=queue,
                )
                r2 = await app.get_result(tid2, timeout=15)
                assert r2 == "slept:0.05"

                proc.send_signal(signal.SIGTERM)
                rc = proc.wait(timeout=30)
                assert rc == 0

            # Stash the outcome on the test item for diagnostic
            # visibility (-v output).
            print(f"\n[U2.7#4 pinned_outcome={pinned_outcome}]")
        finally:
            _kill_subprocess(proc)

    # ----- 5. Manually-broken progress HASH ---------------------------------

    async def test_manually_broken_progress_hash_returns_none(
        self,
        chaos_stack: ChaosStack,
        _cleanup_tasks,
    ) -> None:
        """Direct HSET of a malformed progress HASH → ``get_progress``
        returns None, never raises.

        Pins the malformed-handling path in
        :meth:`Client.get_progress` (R3.7 fix): a HASH that decodes
        into bytes but fails :meth:`ProgressInfo.from_hash` validation
        is treated as "nothing observable". Callers poll safely
        without wrapping in try/except.
        """
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client = Client(broker, backend)

        task_id = f"broken-progress-{uuid.uuid4().hex[:10]}"

        raw = valkey.asyncio.Valkey.from_url(chaos_stack.valkey_url)
        try:
            # Manually insert a HASH with parse-hostile fields:
            # current is non-numeric, total missing, percent a symbol.
            # This is exactly what a rogue external writer or a future
            # incompatible schema could deposit.
            await raw.hset(
                f"sq:progress:{task_id}",
                mapping={
                    "current": "abc",
                    "percent": "!!!",
                    "message": "garbage",
                },
            )
        finally:
            await raw.aclose()

        try:
            # Contract: malformed HASH → None, no exception.
            result = await client.get_progress(task_id)
            assert result is None, f"get_progress should swallow malformed HASH and return None; got {result!r}"

            # Also confirm a completely missing task_id still returns
            # None (baseline no-regression for the happy path).
            absent = await client.get_progress(
                f"never-written-{uuid.uuid4().hex[:6]}",
            )
            assert absent is None
        finally:
            await backend.close()
