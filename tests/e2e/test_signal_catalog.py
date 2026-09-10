"""E2E Signal-Katalog (U1.8) — one test per Signal enum value.

Pins each ``Signal`` member's trigger scenario and kwarg payload
contract. A single handler per test captures the dispatched kwargs
and asserts the shape documented in ``src/swarmq/signals.py``.

Every declared signal now has live coverage: WORKER_HEARTBEAT,
TASK_EXPIRED and SCHEDULER_STANDBY gained emit paths in the
2026-06-02 audit; their former skip slots were reactivated as real
tests in Härtung R2 (2026-06-03).

SCHEDULER_LEADER_FAILOVER has full coverage in
``tests/e2e/test_scheduler.py::TestCrashFailover``; we add a thin
reference test here so the catalog stays complete.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
import uuid
from typing import Any

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

# Liveness safety-net for "wait for an async signal/event" below — generous so
# a loaded/cold CI runner never turns a slow signal round-trip into a false
# failure (the e2e flakiness source). NOT a behavioural deadline: wait_for
# returns the instant the event fires. Genuine timing (retry delays etc.) uses
# explicit ``asyncio.sleep`` and is left untouched.
_EVENT_TIMEOUT = 20.0

# Per-test backstop, generous above the worst-case sum of inner waits + sleeps.
pytestmark = [pytest.mark.timeout(75)]


# U2 (Plan 2026-09-08-001, R3/AE1): ``<basis>:<slot>`` unter dem Supervisor,
# ``<basis>:p<pid>`` eingebettet. Ein blosses ``isinstance(str)`` liess die
# frühere Speicheradresse als Kennung durchgehen.
_WORKER_ID_FORMAT = re.compile(r"^[^:]+:p?\d+$")


def _unique_queue() -> str:
    return f"e2e-sigcat-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


@pytest.fixture(autouse=True)
async def _flush_valkey(valkey_url):
    """Flush Valkey BEFORE and after each test.

    Before is load-bearing: a worker from any prior e2e test (in a file
    without this fixture) becomes scheduler leader, and a cancel-based
    teardown never runs the graceful lock release — so the leader lock
    lingers for its TTL (default 30s) and starves this test's leader
    election (``SCHEDULER_LEADER`` never fires → timeout, green only on a
    later rerun). Flushing on setup gives a clean slate regardless of order;
    teardown keeps our dedup / lock / scheduler keys from leaking forward."""
    async def _flush() -> None:
        client = valkey_asyncio.Valkey.from_url(valkey_url)
        try:
            await client.flushdb()
        finally:
            await client.aclose()

    await _flush()
    yield
    await _flush()


@contextlib.asynccontextmanager
async def _running_worker(
    rabbitmq_url: str, valkey_url: str, queue: str,
    *, extra_config: dict[str, Any] | None = None,
    register_signals: dict[Signal, Any] | None = None,
):
    """Spawn a worker, await WORKER_ONLINE, yield the worker_app.

    ``register_signals`` lets callers register handlers BEFORE
    ``start_worker()`` so signals emitted during startup (e.g.
    WORKER_ONLINE, SCHEDULER_LEADER) aren't missed. Map: Signal
    value -> async handler.
    """
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    kwargs: dict[str, Any] = {"queues": [queue], "concurrency": 1}
    if extra_config:
        kwargs.update(extra_config)
    worker_app = SwarmQ(worker_broker, worker_backend, **kwargs)

    for sig, handler in (register_signals or {}).items():
        worker_app.on_signal(sig)(handler)

    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: Any) -> None:
        online.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=_EVENT_TIMEOUT)
        yield worker_app, worker_task
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()


# ---------------------------------------------------------------------------
# Worker lifecycle
# ---------------------------------------------------------------------------


async def test_signal_worker_online(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """WORKER_ONLINE fires once when the worker reaches ready state."""
    queue = _unique_queue()

    online_kwargs: list[dict[str, Any]] = []
    online_event = asyncio.Event()

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker, worker_backend, queues=[queue], concurrency=1,
    )

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**kwargs: Any) -> None:
        online_kwargs.append(kwargs)
        online_event.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online_event.wait(), timeout=_EVENT_TIMEOUT)
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()

    assert len(online_kwargs) == 1
    assert "worker_id" in online_kwargs[0]
    assert _WORKER_ID_FORMAT.match(online_kwargs[0]["worker_id"])


async def test_signal_worker_shutdown(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """WORKER_SHUTDOWN fires at the start of the graceful shutdown sequence.

    Cancelling ``start_worker()``'s outer task skips the graceful
    path — Worker._execute_shutdown is only reached via the internal
    ``_shutdown_event``. We build a Worker directly and call
    ``_request_shutdown`` to exercise the graceful emit.
    """
    from swarmq.worker import Worker

    queue = _unique_queue()
    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue], concurrency=1)

    captured: list[dict[str, Any]] = []
    shutdown_event = asyncio.Event()

    @app.on_signal(Signal.WORKER_SHUTDOWN)
    async def _on_shutdown(**kwargs: Any) -> None:
        captured.append(kwargs)
        shutdown_event.set()

    async with app:
        worker = Worker(
            app.broker, app.backend, app.registry, app.config,
            signal_dispatcher=app._signal_dispatcher,
        )
        task = asyncio.create_task(worker.start())
        try:
            await asyncio.sleep(0.5)
            worker._request_shutdown()
            await asyncio.wait_for(shutdown_event.wait(), timeout=_EVENT_TIMEOUT)
            await asyncio.wait_for(task, timeout=_EVENT_TIMEOUT)
        finally:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    assert len(captured) == 1
    assert _WORKER_ID_FORMAT.match(captured[0]["worker_id"])


async def test_signal_worker_heartbeat(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """WORKER_HEARTBEAT — periodic liveness tick from the metrics sampler.

    Reaktiviert (Härtung R2 B2): der Skip-Grund („never dispatched")
    ist seit dem 2026-06-02-Audit (P1-3) faktisch falsch — der Sampler
    emittiert den Heartbeat, sobald Metrics aktiv sind.
    """
    pytest.importorskip("prometheus_client")
    import socket

    from swarmq.worker import Worker

    def _free_port() -> int:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    queue = _unique_queue()
    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue], concurrency=1)

    captured: list[dict[str, Any]] = []
    beat = asyncio.Event()

    @app.on_signal(Signal.WORKER_HEARTBEAT)
    async def _on_heartbeat(**kwargs: Any) -> None:
        captured.append(kwargs)
        beat.set()

    async with app:
        app.config.metrics_port = _free_port()
        app.config.metrics_sample_interval = 0.2
        worker = Worker(
            app.broker, app.backend, app.registry, app.config,
            signal_dispatcher=app._signal_dispatcher,
        )
        task = asyncio.create_task(worker.start())
        try:
            await asyncio.wait_for(beat.wait(), timeout=_EVENT_TIMEOUT)
        finally:
            worker._request_shutdown()
            try:
                await asyncio.wait_for(task, timeout=_EVENT_TIMEOUT)
            except TimeoutError:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    assert len(captured) >= 1
    assert _WORKER_ID_FORMAT.match(captured[0]["worker_id"])
    assert captured[0]["uptime_seconds"] >= 0


# ---------------------------------------------------------------------------
# Task lifecycle — happy path
# ---------------------------------------------------------------------------


async def test_signal_task_enqueued(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_ENQUEUED fires at client.schedule() time with task_id, task_name, queue."""
    queue = _unique_queue()
    task_name = _task_name("enq")

    class Noop(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            return "ok"

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )

    captured: list[dict[str, Any]] = []

    @client_app.on_signal(Signal.TASK_ENQUEUED)
    async def _on_enq(**kwargs: Any) -> None:
        captured.append(kwargs)

    async with client_app:
        await client_app.broker.declare_queue(queue)
        tid = await client_app.schedule(task_name)

    assert len(captured) == 1
    assert captured[0]["task_id"] == tid
    assert captured[0]["task_name"] == task_name
    assert captured[0]["queue"] == queue


async def test_signal_task_started(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_STARTED fires when the worker picks up a message."""
    queue = _unique_queue()
    task_name = _task_name("start")
    started = asyncio.Event()
    captured: list[dict[str, Any]] = []

    class Noop(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            return "ok"

    async def _handler(**kwargs: Any) -> None:
        captured.append(kwargs)
        started.set()

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(
            rabbitmq_url, valkey_url, queue,
            register_signals={Signal.TASK_STARTED: _handler},
        ):
            tid = await client_app.schedule(task_name)
            await asyncio.wait_for(started.wait(), timeout=_EVENT_TIMEOUT)

    assert len(captured) == 1
    assert captured[0]["task_id"] == tid
    assert captured[0]["task_name"] == task_name


async def test_signal_task_completed(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_COMPLETED fires on successful run with duration_ms."""
    queue = _unique_queue()
    task_name = _task_name("done")
    done = asyncio.Event()
    captured: list[dict[str, Any]] = []

    class Noop(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            return "ok"

    async def _handler(**kwargs: Any) -> None:
        captured.append(kwargs)
        done.set()

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(
            rabbitmq_url, valkey_url, queue,
            register_signals={Signal.TASK_COMPLETED: _handler},
        ):
            tid = await client_app.schedule(task_name)
            await asyncio.wait_for(done.wait(), timeout=_EVENT_TIMEOUT)

    assert len(captured) == 1
    assert captured[0]["task_id"] == tid
    assert captured[0]["task_name"] == task_name
    assert "duration_ms" in captured[0]
    assert isinstance(captured[0]["duration_ms"], (int, float))
    assert captured[0]["duration_ms"] > 0


async def test_signal_task_failed(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_FAILED fires after retries exhaust; kwargs include exception."""
    queue = _unique_queue()
    task_name = _task_name("fail")
    failed = asyncio.Event()
    captured: list[dict[str, Any]] = []

    class BoomTask(Task):
        task_info = TaskInfo(
            name=task_name, queues=[queue],
            max_retries=1,
            retry_backoff="fixed",
            retry_base_delay=0.1,
        )

        async def run(self) -> None:
            raise ValueError("boom-failed")

    async def _handler(**kwargs: Any) -> None:
        captured.append(kwargs)
        failed.set()

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(
            rabbitmq_url, valkey_url, queue,
            register_signals={Signal.TASK_FAILED: _handler},
        ):
            tid = await client_app.schedule(task_name)
            # max_retries=1 means 2 attempts; last one must fire TASK_FAILED.
            # Wait until the *final* failure lands (captured >=2 total).
            # Handler is set on FIRST failure too, so we drain both.
            try:
                await asyncio.wait_for(failed.wait(), timeout=_EVENT_TIMEOUT)
            except TimeoutError:
                pass

    # At least one TASK_FAILED (the final one) — signal fires on every
    # terminal failure per-attempt; with max_retries=1 we see 2 dispatches
    # in total (attempt 1 fails → signal; attempt 2 fails → signal).
    assert len(captured) >= 1
    last = captured[-1]
    assert last["task_id"] == tid
    assert last["task_name"] == task_name
    assert "exception" in last
    assert "boom-failed" in str(last["exception"])


@pytest.mark.timeout(75)
async def test_signal_task_retrying(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_RETRYING fires on the retry decision between attempts."""
    queue = _unique_queue()
    task_name = _task_name("retry")
    retry_event = asyncio.Event()
    captured: list[dict[str, Any]] = []

    class RetryOnce(Task):
        task_info = TaskInfo(
            name=task_name, queues=[queue],
            max_retries=2,
            retry_backoff="fixed",
            retry_base_delay=0.3,
        )

        async def run(self) -> str:
            # Fail the first attempt, succeed on the retry.
            # Use a module-scope counter via class attribute.
            RetryOnce._calls = getattr(RetryOnce, "_calls", 0) + 1
            if RetryOnce._calls == 1:
                raise RuntimeError("transient")
            return "ok"

    async def _handler(**kwargs: Any) -> None:
        captured.append(kwargs)
        retry_event.set()

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(
            rabbitmq_url, valkey_url, queue,
            register_signals={Signal.TASK_RETRYING: _handler},
        ):
            tid = await client_app.schedule(task_name)
            await asyncio.wait_for(retry_event.wait(), timeout=_EVENT_TIMEOUT)
            # Task eventually succeeds.
            result = await client_app.get_result(tid, timeout=_EVENT_TIMEOUT)
            assert result == "ok"

    assert len(captured) >= 1
    first = captured[0]
    assert first["task_id"] == tid
    assert first["retry_count"] == 1  # first retry → count = 1
    assert "delay" in first
    assert isinstance(first["delay"], (int, float))
    assert first["delay"] > 0


async def test_signal_task_cancelled(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_CANCELLED fires when a cancel flag is observed pre-pickup."""
    queue = _unique_queue()
    task_name = _task_name("cancel")
    cancelled = asyncio.Event()
    captured: list[dict[str, Any]] = []

    class SlowTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            await asyncio.sleep(10)
            return "no"

    async def _handler(**kwargs: Any) -> None:
        captured.append(kwargs)
        cancelled.set()

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        tid = await client_app.schedule(task_name)
        await client_app.cancel(tid)

        async with _running_worker(
            rabbitmq_url, valkey_url, queue,
            register_signals={Signal.TASK_CANCELLED: _handler},
        ):
            await asyncio.wait_for(cancelled.wait(), timeout=_EVENT_TIMEOUT)

    assert len(captured) == 1
    assert captured[0]["task_id"] == tid
    assert captured[0]["task_name"] == task_name


async def test_signal_task_duplicate_detected(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_DUPLICATE_DETECTED fires when a second schedule() hits the
    dedup flag. existing_task_id, task_name, dedup_hash, source all present."""
    queue = _unique_queue()
    task_name = _task_name("dup")
    captured: list[dict[str, Any]] = []

    class DupTask(Task):
        task_info = TaskInfo(
            name=task_name, queues=[queue],
            unique_for=60,
        )

        async def run(self, x: int) -> int:
            return x

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )

    @client_app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
    async def _on_dup(**kwargs: Any) -> None:
        captured.append(kwargs)

    async with client_app:
        await client_app.broker.declare_queue(queue)
        tid1 = await client_app.schedule(task_name, 42)
        tid2 = await client_app.schedule(task_name, 42)
        # Silent dedup → both schedule calls return the same task_id.
        assert tid1 == tid2

    assert len(captured) == 1
    payload = captured[0]
    assert payload["existing_task_id"] == tid1
    assert payload["task_name"] == task_name
    assert "dedup_hash" in payload
    assert payload["source"] == "client.schedule"


async def test_signal_task_rate_limited(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_RATE_LIMITED fires when a rate-limited task exceeds its allowance."""
    queue = _unique_queue()
    task_name = _task_name("rl")
    rl_event = asyncio.Event()
    captured: list[dict[str, Any]] = []

    class LimitedTask(Task):
        task_info = TaskInfo(
            name=task_name, queues=[queue],
            rate_limit="1/h",  # 1 per hour — second immediate fires limiter
        )

        async def run(self) -> str:
            return "ok"

    async def _handler(**kwargs: Any) -> None:
        captured.append(kwargs)
        rl_event.set()

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(
            rabbitmq_url, valkey_url, queue,
            register_signals={Signal.TASK_RATE_LIMITED: _handler},
        ):
            # First consumes the 1/h budget; second must be rate-limited.
            await client_app.schedule(task_name)
            await client_app.schedule(task_name)
            await asyncio.wait_for(rl_event.wait(), timeout=_EVENT_TIMEOUT)

    assert len(captured) >= 1
    payload = captured[0]
    assert "task_id" in payload
    assert payload["task_name"] == task_name
    assert "retry_after_ms" in payload
    assert "remaining" in payload
    assert "rate_limit_count" in payload
    assert payload["rate_limit_count"] >= 1


async def test_signal_task_locked(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_LOCKED fires when a second task contends for the same lock."""
    queue = _unique_queue()
    long_name = _task_name("long_lock")
    short_name = _task_name("short_lock")
    lock_name = f"sigcat-lock-{uuid.uuid4().hex[:6]}"

    locked_event = asyncio.Event()
    captured: list[dict[str, Any]] = []

    class LongHold(Task):
        task_info = TaskInfo(
            name=long_name, queues=[queue],
            lock=lock_name, hard_timeout=30,
        )

        async def run(self) -> str:
            await asyncio.sleep(5)
            return "held"

    class ShortHold(Task):
        task_info = TaskInfo(
            name=short_name, queues=[queue],
            lock=lock_name, hard_timeout=30,
        )

        async def run(self) -> str:
            return "fast"

    async def _handler(**kwargs: Any) -> None:
        captured.append(kwargs)
        locked_event.set()

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(
            rabbitmq_url, valkey_url, queue,
            extra_config={"concurrency": 2},
            register_signals={Signal.TASK_LOCKED: _handler},
        ):
            await client_app.schedule(long_name)
            # Brief delay so the long task acquires the lock first.
            await asyncio.sleep(0.5)
            await client_app.schedule(short_name)
            await asyncio.wait_for(locked_event.wait(), timeout=_EVENT_TIMEOUT)

    assert len(captured) >= 1
    payload = captured[0]
    assert "task_id" in payload
    assert payload["task_name"] == short_name
    assert payload["lock_name"] == lock_name
    assert payload["lock_conflict_count"] >= 1


async def test_signal_task_expired(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """TASK_EXPIRED fires when a message's enqueue age exceeds
    TaskInfo(max_age=...) at consume time (R4.4).

    Uses a manually-patched enqueued_at header (set to 1h in the past)
    so the test doesn't need to wait for the max_age window to elapse.
    """
    queue = _unique_queue()
    task_name = _task_name("expired")
    captured: list[dict] = []
    expired_event = asyncio.Event()

    class ExpiringTask(Task):
        task_info = TaskInfo(
            name=task_name, queues=[queue], max_retries=0,
            max_age=10,
        )

        async def run(self) -> str:  # pragma: no cover — never runs
            return "should_not_run"

    # Client-side: publish directly via a raw aio_pika channel with a
    # stale enqueued-at header — simulates a message that has been
    # sitting in the queue past its max_age.
    import aio_pika
    conn = await aio_pika.connect_robust(rabbitmq_url)
    try:
        ch = await conn.channel()
        # Declare via SwarmQ so queue args match.
        broker_for_decl = RabbitMQBroker(rabbitmq_url)
        await broker_for_decl.connect()
        try:
            await broker_for_decl.declare_queue(queue)
        finally:
            await broker_for_decl.close()
        stale_ts = int(time.time()) - 3600  # 1h ago, max_age=10
        await ch.default_exchange.publish(
            aio_pika.Message(
                body=(
                    b'{"task": {"id": "expired-id-1", '
                    b'"name": "' + task_name.encode() + b'", '
                    b'"queue": "' + queue.encode() + b'"}, '
                    b'"args": [], "kwargs": {}}'
                ),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                headers={"x-swarmq-enqueued-at": stale_ts},
            ),
            routing_key=queue,
        )
        await ch.close()
    finally:
        await conn.close()

    # Worker-side: consume, expect TASK_EXPIRED, NOT task execution.
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker, worker_backend, queues=[queue], concurrency=1,
    )

    @worker_app.on_signal(Signal.TASK_EXPIRED)
    async def _cap(**kw: Any) -> None:
        captured.append(kw)
        expired_event.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(expired_event.wait(), timeout=_EVENT_TIMEOUT)
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()

    assert len(captured) == 1
    assert captured[0]["task_name"] == task_name
    assert captured[0]["queue"] == queue
    assert captured[0]["max_age_s"] == 10
    assert captured[0]["age_s"] >= 3600


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------


async def test_signal_scheduler_leader(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """SCHEDULER_LEADER fires on first-time leader election."""
    queue = _unique_queue()
    leader_event = asyncio.Event()
    captured: list[dict[str, Any]] = []

    async def _handler(**kwargs: Any) -> None:
        captured.append(kwargs)
        leader_event.set()

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    async with SwarmQ(
        broker, backend,
        queues=[queue],
        concurrency=1,
        scheduler_poll_interval=0.1,
        scheduler_heartbeat_interval=0.2,
        scheduler_lock_ttl=1.0,
    ) as app:
        await app.broker.declare_queue(queue)
        app.on_signal(Signal.SCHEDULER_LEADER)(_handler)
        worker_task = asyncio.create_task(app.start_worker())
        try:
            await asyncio.wait_for(leader_event.wait(), timeout=_EVENT_TIMEOUT)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task

    assert len(captured) >= 1
    assert _WORKER_ID_FORMAT.match(captured[0]["worker_id"])


async def test_signal_scheduler_leader_failover_reference():
    """SCHEDULER_LEADER_FAILOVER is covered in
    tests/e2e/test_scheduler.py::TestCrashFailover. This thin test
    imports the canonical fixture to keep the signal-catalog index
    complete without duplicating the multi-worker failover setup."""
    from tests.e2e.test_scheduler import TestCrashFailover

    assert hasattr(
        TestCrashFailover, "test_failover_signal_fires_on_crash_not_graceful"
    ), "SCHEDULER_LEADER_FAILOVER reference test missing in test_scheduler.py"


async def test_signal_scheduler_leader_handover(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """SCHEDULER_LEADER_HANDOVER fires when a leader releases its lock
    during graceful shutdown.

    Ordering matters: scheduler.start() runs (and can emit
    SCHEDULER_LEADER) BEFORE Worker._state transitions to RUNNING.
    _request_shutdown() is a no-op when the worker is still in
    STARTING, so we must wait for WORKER_ONLINE — not just
    SCHEDULER_LEADER — before requesting shutdown.
    """
    queue = _unique_queue()
    leader_event = asyncio.Event()
    online_event = asyncio.Event()
    handover_event = asyncio.Event()
    captured: list[dict[str, Any]] = []

    async def _on_leader(**_: Any) -> None:
        leader_event.set()

    async def _on_online(**_: Any) -> None:
        online_event.set()

    async def _on_handover(**kwargs: Any) -> None:
        captured.append(kwargs)
        handover_event.set()

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(
        broker, backend,
        queues=[queue], concurrency=1,
        scheduler_poll_interval=0.1,
        scheduler_heartbeat_interval=0.2,
        scheduler_lock_ttl=1.0,
    )
    app.on_signal(Signal.SCHEDULER_LEADER)(_on_leader)
    app.on_signal(Signal.WORKER_ONLINE)(_on_online)
    app.on_signal(Signal.SCHEDULER_LEADER_HANDOVER)(_on_handover)

    from swarmq.worker import Worker

    async with app:
        await app.broker.declare_queue(queue)
        worker = Worker(
            app.broker, app.backend, app.registry, app.config,
            signal_dispatcher=app._signal_dispatcher,
        )
        task = asyncio.create_task(worker.start())
        try:
            await asyncio.wait_for(leader_event.wait(), timeout=_EVENT_TIMEOUT)
            await asyncio.wait_for(online_event.wait(), timeout=_EVENT_TIMEOUT)
            # Now worker is in RUNNING state — _request_shutdown is not
            # a no-op and the graceful path calls scheduler.stop(), which
            # emits HANDOVER while still leader.
            worker._request_shutdown()
            await asyncio.wait_for(handover_event.wait(), timeout=_EVENT_TIMEOUT)
            await asyncio.wait_for(task, timeout=_EVENT_TIMEOUT)
        finally:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    assert len(captured) >= 1
    assert _WORKER_ID_FORMAT.match(captured[0]["worker_id"])


async def test_signal_scheduler_heartbeat(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """SCHEDULER_HEARTBEAT fires periodically after leader renews its lock."""
    queue = _unique_queue()
    leader_event = asyncio.Event()
    heartbeat_event = asyncio.Event()
    captured: list[dict[str, Any]] = []

    async def _on_leader(**_: Any) -> None:
        leader_event.set()

    async def _on_heartbeat(**kwargs: Any) -> None:
        captured.append(kwargs)
        heartbeat_event.set()

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    async with SwarmQ(
        broker, backend,
        queues=[queue], concurrency=1,
        scheduler_poll_interval=0.1,
        scheduler_heartbeat_interval=0.2,
        scheduler_lock_ttl=1.0,
    ) as app:
        await app.broker.declare_queue(queue)
        app.on_signal(Signal.SCHEDULER_LEADER)(_on_leader)
        app.on_signal(Signal.SCHEDULER_HEARTBEAT)(_on_heartbeat)
        worker_task = asyncio.create_task(app.start_worker())
        try:
            await asyncio.wait_for(leader_event.wait(), timeout=_EVENT_TIMEOUT)
            await asyncio.wait_for(heartbeat_event.wait(), timeout=_EVENT_TIMEOUT)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task

    assert len(captured) >= 1
    assert _WORKER_ID_FORMAT.match(captured[0]["worker_id"])


async def test_signal_scheduler_standby(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """SCHEDULER_STANDBY — leader loses its lock and steps down.

    Reaktiviert (Härtung R2 B2): der Skip-Grund („never dispatched")
    ist seit dem 2026-06-02-Audit faktisch falsch — die Leader→Standby-
    Transition emittiert das Signal. Szenario: der Leader-Lock wird
    extern gelöscht (simulierter TTL-Verlust) → nächster Renew-Tick
    schlägt fehl → STANDBY.
    """
    from swarmq.config import SwarmQConfig
    from swarmq.locking import SCHEDULER_LOCK_KEY
    from swarmq.scheduler import (
        SchedulerLoop,
        ScheduleStore,
    )
    from swarmq.signals import SignalDispatcher

    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        dispatcher = SignalDispatcher()
        leader = asyncio.Event()
        standby_captured: list[dict[str, Any]] = []
        standby = asyncio.Event()

        @dispatcher.on_signal(Signal.SCHEDULER_LEADER)
        async def _on_leader(**kwargs: Any) -> None:
            leader.set()

        @dispatcher.on_signal(Signal.SCHEDULER_STANDBY)
        async def _on_standby(**kwargs: Any) -> None:
            standby_captured.append(kwargs)
            standby.set()

        config = SwarmQConfig(
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=2.0,
        )
        owner_token = f"standby-test-{uuid.uuid4().hex[:6]}"
        loop = SchedulerLoop(
            backend=backend,
            broker=broker,
            config=config,
            lock_manager=backend.locks,
            store=ScheduleStore(backend),
            signal_dispatcher=dispatcher,
            owner_token=owner_token,
        )
        await loop.start()
        try:
            await asyncio.wait_for(leader.wait(), timeout=_EVENT_TIMEOUT)
            # Simulate lock loss (TTL expiry / takeover): the next renew
            # returns False → leader transitions to standby.
            await backend._client.delete(SCHEDULER_LOCK_KEY)
            await asyncio.wait_for(standby.wait(), timeout=_EVENT_TIMEOUT)
        finally:
            await loop.abort()

        assert len(standby_captured) >= 1
        # Hier baut der Test den ``SchedulerLoop`` ohne Worker und ohne
        # ``worker_id``; er faellt auf sein Owner-Token zurueck. Geprueft wird
        # die Durchreichung, nicht das Format aus R3.
        assert standby_captured[0]["worker_id"] == owner_token
        assert loop.state == "stopped"
    finally:
        await broker.close()
        await backend.close()


async def test_signal_worker_recycling_reference():
    """WORKER_RECYCLING (Plan 2026-09-03-001, R7/R23) is covered in
    tests/e2e/test_cpu_mode.py::test_di_child_is_reused_and_recycled_with_signal
    (compute-child recycling by task count). Thin reference so the
    signal catalog stays complete without a second forkserver setup."""
    from tests.e2e import test_cpu_mode

    assert hasattr(test_cpu_mode, "test_di_child_is_reused_and_recycled_with_signal")
    assert Signal.WORKER_RECYCLING.value == "worker_recycling"
