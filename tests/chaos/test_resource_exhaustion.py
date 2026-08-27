"""F5 Resource-exhaustion chaos tests (U2.5).

Three failure-mode probes against the chaos_stack's proxied URLs:

1. Prefetch-saturation — a single-worker pool with concurrency=1
   (so effective AMQP prefetch=1) must process 20 × 0.3s tasks
   strictly serially, completing in at least ~6s and losing none.

2. Valkey connection-pool exhaustion — 100 concurrent SETs against
   one ValkeyBackend. valkey-py's default pool caps at 50 connections
   per client, so 50+ parallel operations saturate the pool. The test
   pins actual behaviour: either the pool queues cleanly (all succeed)
   or a BackendConnectionError surfaces promptly (no hung coroutines).

3. RabbitMQ channel-limit exhaustion — the underlying aiormq
   library (9.6.x) does NOT honour a client-requested ``channel_max``
   (URL query param OR kwarg): it echoes the server's tune value back
   verbatim in TuneOk (see aiormq/connection.py:590–599). The plan's
   suggested ``channel_max=10`` URL trick therefore cannot cap the
   per-connection channel budget at 10 — negotiation stays at 2047.

   **Bug surfaced (aiormq).** Client-side channel_max negotiation is
   broken. Rather than skip the test, we exhaust the server-side
   default instead: open 2048 channels and expect the 2048th to raise
   ``aiormq.exceptions.ConnectionNotAllowed`` (typed, quick — ~1s).
   This still exercises the real invariant: resource exhaustion is
   surfaced as a typed exception, not a silent hang.

All three use the chaos_stack fixture so the proxied URLs give us
deterministic port isolation and let us strip toxiproxy pass-through
cleanly.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid

import aio_pika
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BackendConnectionError
from swarmq.task import Task, TaskInfo
from tests.chaos.conftest import ChaosStack


def _unique_queue() -> str:
    return f"chaos-rx-{uuid.uuid4().hex[:10]}"


async def _warmup_proxied_endpoints(chaos_stack: ChaosStack) -> None:
    """Open + close a short-lived broker/backend connection.

    The ``chaos_stack`` fixture waits on the toxiproxy admin port but
    not on the two proxied upstream ports. Connecting through a freshly
    registered proxy sometimes hits a transient "server connection
    unexpectedly closed" on the first attempt — toxiproxy hasn't yet
    established the upstream TCP session. A throwaway connect inside a
    small retry loop makes the proxies deterministic before the real
    test starts touching them.
    """
    last_exc: BaseException | None = None
    for _ in range(5):
        try:
            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            try:
                await broker.connect()
                await backend.connect()
            finally:
                await broker.close()
                await backend.close()
            return
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(0.8)
    raise RuntimeError(
        f"chaos_stack proxied endpoints never warmed up: {last_exc!r}",
    )


class TestResourceExhaustion:
    """All three tests share the per-class chaos_stack fixture."""

    @pytest.fixture(autouse=True)
    async def _warmup(self, chaos_stack: ChaosStack) -> None:
        """Ensure the proxied broker/backend ports respond before each
        test. See :func:`_warmup_proxied_endpoints` — this absorbs the
        first-connect race window after toxiproxy proxy registration."""
        await _warmup_proxied_endpoints(chaos_stack)

    # ------------------------------------------------------------------
    # Test 1 — prefetch=1 (concurrency=1) → strictly serial execution
    # ------------------------------------------------------------------

    async def test_prefetch_one_serial_consumption(
        self, chaos_stack: ChaosStack,
    ) -> None:
        """concurrency=1 worker processes 20 × 0.3s tasks serially.

        Worker concurrency → AMQP prefetch_count. With prefetch=1 and a
        sleep-heavy task the wall-clock must be >= 20 * 0.3 = 6.0s (we
        allow a small margin below that for scheduling jitter; primary
        invariant is serial-not-parallel, not exact time).
        """
        queue = _unique_queue()
        task_name = f"chaos_sleep_{uuid.uuid4().hex[:8]}"
        sleep_s = 0.3
        total = 20

        class SleepTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

            async def run(self, i: int) -> int:
                await asyncio.sleep(sleep_s)
                return i

        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        backend = ValkeyBackend(chaos_stack.valkey_url)
        app = SwarmQ(
            broker, backend, queues=[queue], concurrency=1,
        )

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
                # Let the worker connect + start consuming before we
                # publish — otherwise prefetch has not been applied yet
                # and the first batch may bypass the QoS.
                await asyncio.sleep(0.8)

                task_ids = [
                    await app.schedule(task_name, i) for i in range(total)
                ]

                t0 = time.monotonic()
                results = await app.get_results(
                    task_ids, timeout=60.0, poll_interval=0.05,
                )
                elapsed = time.monotonic() - t0

                # No loss: every task produced a result.
                assert len(results) == total, (
                    f"expected {total} results, got {len(results)}"
                )
                assert set(results) == set(range(total)), (
                    f"missing / duplicate results: {sorted(results)}"
                )

                # Strictly serial: wall clock >= total * sleep * 0.95.
                # The 5% slack absorbs a single message that may have
                # been handed to the worker before QoS was applied.
                min_expected = total * sleep_s * 0.95
                assert elapsed >= min_expected, (
                    f"elapsed={elapsed:.2f}s < min_expected={min_expected:.2f}s "
                    f"— worker ran tasks in parallel (prefetch not honoured)"
                )
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
                await worker_broker.close()
                await worker_backend.close()

    # ------------------------------------------------------------------
    # Test 2 — Valkey connection-pool exhaustion
    # ------------------------------------------------------------------

    async def test_connection_pool_exhaustion_yields_typed_error(
        self, chaos_stack: ChaosStack,
    ) -> None:
        """100 concurrent SET ops against one ValkeyBackend.

        valkey-py defaults to max_connections=50 per client. 100
        concurrent ops will either queue cleanly (pool-wait ok) OR a
        BackendConnectionError surfaces. What we REFUSE to accept is a
        hung coroutine — the whole gather must complete within a bounded
        timeout, and any failure must be typed (BackendConnectionError),
        not a raw ValkeyError.

        This pins the current behaviour: pinning "all succeed" or
        "clean typed error" both pass the test, but a hang fails via
        the outer wait_for.
        """
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        try:
            n = 100

            async def one_set(idx: int) -> None:
                await backend.store_result(
                    f"chaos-pool-{idx}-{uuid.uuid4().hex}",
                    b"x", ttl_ms=5_000,
                )

            # Bound the entire gather — a silent hang in pool acquisition
            # would keep a coroutine waiting indefinitely; we want to
            # detect that as a test failure, not hide it.
            async def _run_all() -> list[BaseException | None]:
                tasks = [one_set(i) for i in range(n)]
                return await asyncio.gather(*tasks, return_exceptions=True)

            try:
                results = await asyncio.wait_for(_run_all(), timeout=30.0)
            except TimeoutError:
                pytest.fail(
                    "Pool exhaustion hung the gather — neither succeeded "
                    "nor surfaced a typed error within 30s.",
                )

            # Classify outcomes.
            successes = [r for r in results if r is None]
            failures = [r for r in results if isinstance(r, BaseException)]

            # Invariant: any failure must be BackendConnectionError.
            # Raw ValkeyError, OSError, or TimeoutError escaping would
            # indicate the backend's exception-translation layer missed
            # a case (BACKEND-layer bug).
            for exc in failures:
                assert isinstance(exc, BackendConnectionError), (
                    f"un-translated exception escaped from backend: "
                    f"{type(exc).__name__}: {exc!r}"
                )

            # Pin behaviour. Both outcomes are acceptable; log which one
            # so the next reviewer can see what production hits today.
            # (print is captured by pytest -v and visible on failure.)
            print(
                f"[pool-exhaustion] n={n}, successes={len(successes)}, "
                f"typed failures={len(failures)}",
            )

            # Weakest useful assertion: either the pool queued
            # everything OR it rejected some — both are acceptable,
            # but all-hang is not (already caught above) and a mix of
            # untyped failures is not (already caught in loop above).
            assert len(successes) + len(failures) == n
        finally:
            await backend.close()

    # ------------------------------------------------------------------
    # Test 3 — RabbitMQ channel-limit exhaustion
    # ------------------------------------------------------------------

    async def test_channel_limit_exhaustion_typed_error(
        self, chaos_stack: ChaosStack,
    ) -> None:
        """Exhaust RabbitMQ's server-side channel_max and expect a typed raise.

        **aiormq-bug pinned.** The plan suggested capping negotiation at
        ``channel_max=10`` via URL query param; experiment showed aiormq
        (9.6.x) ignores client-side channel_max entirely — it echoes the
        server's 2047 back in TuneOk (aiormq/connection.py:590). The URL
        param trick is therefore dead. We instead open the full default
        budget (2047) and assert the 2048th raises
        ``aiormq.exceptions.ConnectionNotAllowed`` (typed, ~1s) rather
        than hanging.

        Bug report (informational, NOT fixed here):
          * upstream: mosquito/aiormq — client-requested channel_max is
            discarded during Connection.Tune handling.
          * impact on SwarmQ: defensive per-connection channel caps can
            only be enforced server-side; library-level limits have no
            effect.
        """
        url = chaos_stack.rabbitmq_url

        # connect_robust is what RabbitMQBroker uses in production. Same
        # path means we exercise the same negotiation + error surface.
        connection = await aio_pika.connect_robust(url)
        channels: list[aio_pika.abc.AbstractChannel] = []

        # Import the aiormq exception eagerly so the pytest.raises types
        # include it. aiormq lives below aio_pika and sometimes the
        # exception escapes un-wrapped through RobustConnection.
        try:
            from aiormq.exceptions import (
                AMQPError as _AiormqAMQPError,
            )
            from aiormq.exceptions import (
                ConnectionNotAllowed as _ConnectionNotAllowed,
            )
        except ImportError:  # pragma: no cover
            _ConnectionNotAllowed = Exception  # type: ignore[assignment,misc]
            _AiormqAMQPError = Exception  # type: ignore[assignment,misc]

        try:
            # Open the full budget. Empirically this completes in < 2s
            # against a local rabbit. 2047 is the default server
            # channel_max and what aiormq always ends up negotiating.
            open_ok = 0
            for i in range(2047):
                try:
                    ch = await asyncio.wait_for(
                        connection.channel(), timeout=10.0,
                    )
                except TimeoutError:
                    pytest.fail(
                        f"channel {i+1}/2047 timed out during "
                        f"pre-exhaustion ramp-up — broker unhealthy?",
                    )
                channels.append(ch)
                open_ok += 1
            assert open_ok == 2047, (
                f"could only open {open_ok} channels before exhaustion — "
                f"broker's channel_max lower than expected?"
            )

            # 2048th must fail — bounded wait distinguishes a typed
            # raise from a silent hang. We accept any exception from
            # the aiormq/aio_pika family; the assertion that matters is
            # "raised, not hung".
            with pytest.raises(
                (
                    _ConnectionNotAllowed,
                    _AiormqAMQPError,
                    aio_pika.exceptions.AMQPChannelError,
                    aio_pika.exceptions.AMQPError,
                    aio_pika.exceptions.ConnectionClosed,
                    aio_pika.exceptions.ChannelClosed,
                ),
            ):
                try:
                    ch = await asyncio.wait_for(
                        connection.channel(), timeout=10.0,
                    )
                    # If aio_pika hands us a channel object, record it
                    # so the teardown still closes it cleanly — then
                    # fail, because exhaustion should have raised.
                    channels.append(ch)
                    pytest.fail(
                        "2048th channel opened successfully — the "
                        "broker did not enforce channel_max (did the "
                        "server config get changed?)",
                    )
                except TimeoutError as exc:
                    pytest.fail(
                        f"2048th channel.open() hung (no typed error "
                        f"within 10s): {exc!r}",
                    )
        finally:
            for ch in channels:
                with contextlib.suppress(Exception):
                    await ch.close()
            with contextlib.suppress(Exception):
                await connection.close()

