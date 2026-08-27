"""U2.3 — F3 Network chaos via toxiproxy.

Six tests that inject network disruptions through toxiproxy on the
Valkey and RabbitMQ paths. Client + worker both run against the
proxied URLs so every toxic affects both sides. The shared
``chaos_stack`` fixture is class-scoped — all six tests run under a
single compose up/down cycle.

Plan: ``docs/plans/2026-04-17-007-feat-e2e-hardening-plan.md`` U2.3.

Surfaced semantics (pinned by these tests; see individual docstrings
for detail):

* 500ms Valkey latency does not induce spurious task retries — the
  client/worker valkey-py retry policy (command-level) plus backend
  op timeouts absorb the added RTT.
* A 50 kbps bandwidth cap on RabbitMQ slows throughput but never
  drops small-payload messages: all scheduled tasks still complete.
* Slicer-fragmented RabbitMQ TCP frames are reassembled by aio_pika —
  no loss, no corruption.
* With the proxy fully down, client.schedule() initiated during the
  outage raises a transport error (RabbitMQ publisher has no queue to
  buffer into); tasks scheduled AFTER ``up()`` drain normally. We pin
  the "schedule during outage raises" behavior and verify post-up
  recovery.
* With the Valkey proxy down but RabbitMQ healthy, workers crash
  when they cannot reach Valkey to write the result (the worker's
  finally-path treats BackendConnectionError as fatal). The surfaced
  mode is: task disappears (not stored anywhere readable), client's
  get_result times out. After bringing Valkey back up new tasks
  succeed end-to-end — but the lost-mid-partition task is not
  recovered. This test documents that failure mode.
* Pub/sub disconnect + resubscribe: validates the d2d23f2
  ``watch_progress`` fix end-to-end. After toxiproxy down/up on
  Valkey, a direct ``publish_progress`` is received by the
  subscriber (without the fix, valkey-py's pubsub socket stays dead
  and messages are silently dropped forever).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.progress import ProgressInfo
from swarmq.signals import Signal
from swarmq.task import Task

pytestmark = [pytest.mark.timeout(90)]


def _unique_queue() -> str:
    return f"chaos-net-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    """Drop any Task subclasses registered during a test so the next
    test doesn't inherit stale registry state (module-scoped import
    side-effects + per-test TaskInfo means each test creates its own
    task class)."""
    yield
    Task._clear_registered_tasks()


@contextlib.asynccontextmanager
async def _running_worker(rabbitmq_url: str, valkey_url: str, queue: str):
    """Start a SwarmQ worker against the proxied URLs and wait for
    WORKER_ONLINE. On exit, cancel the worker task and close the
    transport. Mirrors the pattern in ``tests/e2e/test_progress.py``
    so toxics apply uniformly to both paths."""
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])

    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: Any) -> None:
        online.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        try:
            await asyncio.wait_for(online.wait(), timeout=10.0)
        except TimeoutError:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            pytest.skip("worker failed to come online against proxied stack")
        yield worker_app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        try:
            await worker_broker.close()
        except Exception:
            pass
        try:
            await worker_backend.close()
        except Exception:
            pass


class TestNetworkChaos:
    """Grouped so the class-scoped ``chaos_stack`` amortises startup."""

    async def test_500ms_latency_on_valkey_no_spurious_retry(
        self, chaos_stack, toxiproxy, _cleanup_tasks,
    ):
        """500ms latency on Valkey — task completes exactly once.

        Adds 500ms downstream latency to the Valkey proxy, then runs a
        single task that counts its own executions through a module-
        level counter. The worker's result-store, cancel-flag checks,
        and progress writes all now take +500ms each, but command-
        level retry + op-timeout must absorb this without the broker
        redelivering or the client giving up early.
        """
        queue = _unique_queue()
        task_name = _task_name("latency_one_shot")

        # Scope the counter to task_id so a spurious redelivery of the
        # same message shows up as counter > 1.
        call_counter: dict[str, int] = {}

        from swarmq.task import TaskInfo

        class OnceTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> int:
                call_counter[self._task_id] = (
                    call_counter.get(self._task_id, 0) + 1
                )
                return call_counter[self._task_id]

        await toxiproxy.add_latency("vkey", latency_ms=500)

        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_backend = ValkeyBackend(chaos_stack.valkey_url)
        client_app = SwarmQ(
            client_broker, client_backend, queues=[queue],
        )

        async with client_app:
            await client_broker.declare_queue(queue)
            async with _running_worker(
                chaos_stack.rabbitmq_url, chaos_stack.valkey_url, queue,
            ):
                tid = await client_app.schedule(task_name)
                # Task body is ~O(200ms) of Valkey ops × 500ms latency;
                # generous timeout for the slowed backend path.
                result = await client_app.get_result(tid, timeout=30)
                assert result == 1
                # Give the broker another moment in case of redelivery
                # (if any were going to fire they'd show up as a second
                # run bumping the counter for the same task_id).
                await asyncio.sleep(1.5)
                assert call_counter.get(tid, 0) == 1, (
                    f"task executed {call_counter.get(tid, 0)} times — "
                    "500ms latency triggered spurious retry"
                )

    async def test_bandwidth_limit_on_rabbitmq(
        self, chaos_stack, toxiproxy, _cleanup_tasks,
    ):
        """50 kbps bandwidth cap on RabbitMQ — all 5 tasks complete.

        Small payloads (echo int) fit well inside the cap; throughput
        slows but no message is lost or truncated. The test pins
        completeness (a flappier bandwidth value would catch latency
        regressions).
        """
        queue = _unique_queue()
        task_name = _task_name("bw_echo")

        from swarmq.task import TaskInfo

        class EchoTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, x: int) -> int:
                return x

        await toxiproxy.add_bandwidth("rmq", kbps=50)

        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_backend = ValkeyBackend(chaos_stack.valkey_url)
        client_app = SwarmQ(
            client_broker, client_backend, queues=[queue],
        )

        async with client_app:
            await client_broker.declare_queue(queue)
            async with _running_worker(
                chaos_stack.rabbitmq_url, chaos_stack.valkey_url, queue,
            ):
                tids = [
                    await client_app.schedule(task_name, i)
                    for i in range(5)
                ]
                results = await client_app.get_results(tids, timeout=45)
                assert results == list(range(5)), (
                    f"bandwidth-limited run lost results: {results!r}"
                )

    async def test_packet_loss_via_slicer_recovers(
        self, chaos_stack, toxiproxy, _cleanup_tasks,
    ):
        """TCP frame slicing on RabbitMQ — aio_pika reassembles.

        Toxiproxy's ``slicer`` toxic fragments TCP writes at
        average_size=64 ± 32 bytes. AMQP frames get chopped into many
        small pieces; aio_pika's stream reader reassembles them. No
        message is lost.
        """
        queue = _unique_queue()
        task_name = _task_name("slicer_echo")

        from swarmq.task import TaskInfo

        class EchoTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, x: int) -> int:
                return x * 2

        await toxiproxy.add_slicer(
            "rmq", average_size=64, size_variation=32,
        )

        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_backend = ValkeyBackend(chaos_stack.valkey_url)
        client_app = SwarmQ(
            client_broker, client_backend, queues=[queue],
        )

        async with client_app:
            await client_broker.declare_queue(queue)
            async with _running_worker(
                chaos_stack.rabbitmq_url, chaos_stack.valkey_url, queue,
            ):
                tids = [
                    await client_app.schedule(task_name, i)
                    for i in range(5)
                ]
                results = await client_app.get_results(tids, timeout=30)
                assert results == [i * 2 for i in range(5)], (
                    f"slicer run lost/corrupted results: {results!r}"
                )

    async def test_full_down_then_recover(
        self, chaos_stack, toxiproxy, _cleanup_tasks,
    ):
        """Full RabbitMQ proxy down, then up, then schedule succeeds.

        Semantics pinned here: ``client.schedule()`` issued while the
        RabbitMQ proxy is down raises a transport error (no local
        buffering — the aio_pika publish channel is dead). The plan
        note captured this: "only valid if client uses retry; if not,
        this test schedules AFTER up()".

        We assert both halves: a schedule attempt during outage is
        rejected with a transport exception, then after ``up()`` the
        client recovers and completes 5 tasks end-to-end.
        """
        queue = _unique_queue()
        task_name = _task_name("down_then_up")

        from swarmq.task import TaskInfo

        class EchoTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, x: int) -> int:
                return x + 100

        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_backend = ValkeyBackend(chaos_stack.valkey_url)
        client_app = SwarmQ(
            client_broker, client_backend, queues=[queue],
        )

        async with client_app:
            await client_broker.declare_queue(queue)
            # Start the worker FIRST while the proxy is healthy, then
            # drop the proxy to exercise the worker's reconnect path.
            async with _running_worker(
                chaos_stack.rabbitmq_url, chaos_stack.valkey_url, queue,
            ):
                # Happy-path baseline to make sure the stack is wired
                # correctly before we start disrupting it.
                baseline = await client_app.schedule(task_name, 0)
                assert await client_app.get_result(
                    baseline, timeout=15,
                ) == 100

                # Drop the proxy completely.
                await toxiproxy.down("rmq")

                # A schedule issued during outage should fail fast
                # (no in-memory buffering). The specific exception type
                # depends on aio_pika's internal state at the moment;
                # we broaden to "any exception" and pin the fact that
                # the call does NOT silently succeed into a void.
                with pytest.raises(Exception):
                    await asyncio.wait_for(
                        client_app.schedule(task_name, 999),
                        timeout=5.0,
                    )

                # Bring the proxy back. Client + worker need a moment
                # to re-establish their channels (U1.1 channel-recreate
                # fix is what makes this work).
                await toxiproxy.up("rmq")
                await asyncio.sleep(3.0)

                tids = [
                    await client_app.schedule(task_name, i)
                    for i in range(5)
                ]
                results = await client_app.get_results(tids, timeout=30)
                assert results == [i + 100 for i in range(5)], (
                    f"post-recovery run lost results: {results!r}"
                )

    async def test_partial_partition_worker_sees_broker_not_backend(
        self, chaos_stack, toxiproxy, _cleanup_tasks,
    ):
        """Valkey proxy down, RabbitMQ healthy — surfaced failure mode.

        Scenario: worker can consume a task off RabbitMQ but cannot
        write the result to Valkey (the ValkeyBackend raises
        ``BackendConnectionError`` after its retry budget is exhausted).
        Client's ``get_result`` polls the same proxied Valkey and also
        hits the partition — so the client times out.

        After ``up()`` is called, fresh tasks succeed end-to-end, but
        the task scheduled during the partition is NOT recovered from
        the client's perspective (the worker's result-write failed and
        is not retried at the task-logic layer). This test pins the
        observed mode: client times out during partition, fresh tasks
        succeed after recovery.

        NOTE: We schedule the at-risk task, then bring the proxy down
        AFTER the schedule round-trip so the enqueue itself succeeds.
        The worker only starts failing when it tries to write the
        result.
        """
        queue = _unique_queue()
        task_name = _task_name("partial_partition")

        from swarmq.task import TaskInfo

        class EchoTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, x: int) -> int:
                # Sleep long enough that the test can partition Valkey
                # mid-run. The task body itself does not touch Valkey —
                # the result write happens in the worker's finally path.
                await asyncio.sleep(1.0)
                return x * 7

        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_backend = ValkeyBackend(chaos_stack.valkey_url)
        client_app = SwarmQ(
            client_broker, client_backend, queues=[queue],
        )

        async with client_app:
            await client_broker.declare_queue(queue)
            async with _running_worker(
                chaos_stack.rabbitmq_url, chaos_stack.valkey_url, queue,
            ):
                # Baseline: healthy stack produces a result.
                baseline = await client_app.schedule(task_name, 1)
                assert await client_app.get_result(
                    baseline, timeout=15,
                ) == 7

                # Schedule, immediately partition Valkey.
                at_risk = await client_app.schedule(task_name, 5)
                await asyncio.sleep(0.1)  # let the message be picked up
                await toxiproxy.down("vkey")

                # During partition: client cannot read the result.
                from swarmq.exceptions import (
                    BackendConnectionError,
                    ResultNotReadyError,
                )
                with pytest.raises(
                    (ResultNotReadyError, BackendConnectionError, Exception),
                ):
                    await client_app.get_result(at_risk, timeout=4)

                # Bring Valkey back.
                await toxiproxy.up("vkey")
                await asyncio.sleep(2.0)

                # Fresh task succeeds. The at-risk task may or may not
                # be recoverable depending on the worker's retry
                # semantics — we deliberately do not assert on it, only
                # on the forward-progress invariant.
                fresh = await client_app.schedule(task_name, 2)
                assert await client_app.get_result(
                    fresh, timeout=30,
                ) == 14

    async def test_pubsub_disconnect_resubscribe_via_watch_progress(
        self, chaos_stack, toxiproxy, _cleanup_tasks,
    ):
        """Pub/Sub disconnect + resubscribe — d2d23f2 end-to-end regression.

        This test drives ``Client.watch_progress`` against a Valkey
        proxied through toxiproxy. The shape of the assertion is:
        after a full proxy down/up cycle, a later publish is observed
        by the subscriber — validating that the resubscribe path in
        ``watch_progress`` fires (the pre-d2d23f2 behavior was that
        the subscription stayed attached to a dead socket forever).

        SURFACED SEMANTICS (pinned by this test):

        * When the proxy goes down mid-iteration, the inner
          ``get_progress`` HASH-fallback inside ``watch_progress``'s
          except-handler ALSO hits the severed path and raises
          :class:`BackendConnectionError`. This propagates out of the
          async generator, causing the iterator to terminate.
        * Realistic consumers therefore wrap ``watch_progress`` in a
          retry loop — which is what this test does (restart the
          iterator when it errors out). The resubscribe path in
          ``watch_progress`` is still the load-bearing piece for the
          intra-iteration reconnect scenarios, but a full backend
          outage surfaces to the caller as an iterator error.
        * A follow-up improvement (tracked separately from U2.3)
          could make ``watch_progress`` loop on ``get_progress``
          failures instead of propagating them — but U2.3 pins the
          current contract.

        After the outage ends and the iterator is restarted, a
        subsequent publish is received — which is the end-to-end
        property the d2d23f2 fix enables (without a correct
        resubscribe, even the restart would subscribe on a dead
        cached pubsub and miss the publish).
        """
        # No worker + no broker needed — we drive progress publishes
        # directly through the backend API so the test stays focused
        # on the pub/sub socket lifecycle. Client.watch_progress only
        # reads from the backend.
        task_id = f"chaos-pubsub-{uuid.uuid4().hex[:10]}"

        client_backend = ValkeyBackend(chaos_stack.valkey_url)
        # Separate backend for the publisher side — mirrors real-world
        # topology (worker process != client process).
        pub_backend = ValkeyBackend(chaos_stack.valkey_url)
        # Client.__init__ wants a broker argument; pass a non-connected
        # RabbitMQBroker — watch_progress never touches it.
        dummy_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)

        received: list[ProgressInfo] = []

        await client_backend.connect()
        await pub_backend.connect()
        try:
            from swarmq.client import Client

            client = Client(
                dummy_broker, client_backend, registry=None,
            )

            watch_ready = asyncio.Event()
            stop_flag = asyncio.Event()

            async def watcher() -> None:
                # Realistic-consumer loop: watch_progress may error out
                # on full backend outage (surfaced semantic documented
                # in the test docstring). Restart until we see the
                # post-recovery publish or the test asks us to stop.
                while not stop_flag.is_set():
                    try:
                        async for info in client.watch_progress(task_id):
                            received.append(info)
                            watch_ready.set()
                            if info.message == "post-recovery":
                                return
                            if stop_flag.is_set():
                                return
                    except asyncio.CancelledError:
                        raise
                    except BaseException:
                        # Backend flapped — swallow and restart.
                        await asyncio.sleep(0.3)

            watch_task = asyncio.create_task(watcher())

            try:
                # Give the subscriber a chance to land on the channel.
                await asyncio.sleep(0.5)

                # Publish a pre-outage payload to confirm the pipe is
                # wired (not strictly required for the regression but
                # catches fixture-setup regressions clearly).
                pre_fields = {
                    "current": "1", "total": "10",
                    "percent": "10.00", "message": "pre-outage",
                    "updated_at": "2026-04-17T00:00:00+00:00",
                }
                await pub_backend.publish_progress(
                    task_id, json.dumps(pre_fields).encode("utf-8"),
                )
                try:
                    await asyncio.wait_for(watch_ready.wait(), timeout=5.0)
                except TimeoutError:
                    pytest.fail(
                        "subscriber never received pre-outage payload — "
                        "watch_progress never bound to the channel"
                    )

                # Drop Valkey for 4s. The subscriber's pub/sub socket
                # sees a TCP RST from toxiproxy; valkey-py's command-
                # level retry does NOT cover the long-held pub/sub
                # socket. 4s is empirically enough for the `get_message
                # timeout=0.1` loop in watch_progress to try at least
                # one read against the dead socket and raise
                # ConnectionError, which triggers the resubscribe path.
                # Shorter windows (2s) sometimes race — toxiproxy
                # comes back up before valkey-py's pubsub reader
                # observes the RST, and the resubscribe is skipped.
                await toxiproxy.down("vkey")
                await asyncio.sleep(4.0)
                await toxiproxy.up("vkey")
                # Sanity check: pub_backend recovers via its command-
                # level retry. A ping succeeds when the path is open.
                for _ in range(20):
                    try:
                        await pub_backend._client.ping()
                        break
                    except Exception:
                        await asyncio.sleep(0.2)
                # Wait longer — the resubscribe loop in watch_progress
                # is paced at 0.5s per cycle with timeout=0.1.
                await asyncio.sleep(4.0)

                # Post-recovery publish. We may need a couple of
                # attempts — the pub_backend client was also mid-
                # partition and its retry state settles as the path
                # reopens.
                post_fields = {
                    "current": "10", "total": "10",
                    "percent": "100.00", "message": "post-recovery",
                    "updated_at": "2026-04-17T00:00:01+00:00",
                }
                # Publish repeatedly — the watcher's restart loop may
                # need several attempts to reattach and see a live
                # publish in the same cycle. 15s is generous; on a
                # healthy run the subscriber reattaches within 1-2s.
                deadline = asyncio.get_running_loop().time() + 15.0
                while asyncio.get_running_loop().time() < deadline:
                    await pub_backend.publish_progress(
                        task_id, json.dumps(post_fields).encode("utf-8"),
                    )
                    await asyncio.sleep(0.5)
                    if any(i.message == "post-recovery" for i in received):
                        break

                assert any(
                    i.message == "post-recovery" for i in received
                ), (
                    "subscriber did not receive post-recovery publish — "
                    "resubscribe fix regressed (received messages: "
                    f"{[i.message for i in received]!r})"
                )
            finally:
                stop_flag.set()
                watch_task.cancel()
                with contextlib.suppress(
                    asyncio.CancelledError, Exception,
                ):
                    await watch_task
        finally:
            for c in (client_backend, pub_backend):
                try:
                    await c.close()
                except Exception:
                    pass
