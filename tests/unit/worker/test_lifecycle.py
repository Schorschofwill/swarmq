"""Worker-Lifecycle (Plan U9): graceful Shutdown (Detached-Shield), Burst-Monitor
und Observability-Autowire — zusammengeführt aus ``test_graceful_shutdown.py``,
``test_burst_mode.py`` und ``test_worker_autowire.py``.

Unit tests for graceful shutdown (Unit 2.5)."""

from __future__ import annotations

import asyncio
import socket
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import LockToken
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker, WorkerState
from tests.support.messages import make_message

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------




def _default_payload(
    task_id: str = "abc-123",
    queue: str = "default",
    name: str = "shutdown_add",
    args: list | None = None,
    kwargs: dict | None = None,
) -> dict:
    return {
        "task": {"id": task_id, "queue": queue, "name": name},
        "args": args if args is not None else [1, 2],
        "kwargs": kwargs if kwargs is not None else {},
    }


class ShutdownAddTask(Task):
    task_info = TaskInfo(name="shutdown_add", queues=["default"])

    async def run(self, a: int, b: int) -> int:
        return a + b


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def broker():
    b = MagicMock(spec=RabbitMQBroker)
    b.connect = AsyncMock()
    b.declare_queue = AsyncMock()
    b.consume = AsyncMock()
    b.ack = AsyncMock()
    b.nack = AsyncMock()
    b.close = AsyncMock()
    b.cancel_consume = AsyncMock()
    b.retry_later = AsyncMock()
    b.dead_letter = AsyncMock()
    return b


@pytest.fixture()
def backend():
    be = MagicMock(spec=ValkeyBackend)
    be.connect = AsyncMock()
    be.store_result = AsyncMock()
    be.close = AsyncMock()
    # Cancel-flag primitives (Phase 3b.3). Default: no flag set.
    be.is_cancelled = AsyncMock(return_value=False)
    be.set_cancel_flag = AsyncMock(return_value=True)
    be.clear_cancel_flag = AsyncMock()
    be.is_workflow_cancelled = AsyncMock(return_value=False)
    return be


@pytest.fixture()
def config():
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=10,
        queues=["default"],
    )


@pytest.fixture()
def registry():
    reg = TaskRegistry()
    reg.register(ShutdownAddTask)
    return reg


@pytest.fixture()
def worker(broker, backend, registry, config):
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


class TestWorkerStateEnum:
    def test_worker_state_has_correct_values(self):
        assert WorkerState.STARTING.value == "STARTING"
        assert WorkerState.RUNNING.value == "RUNNING"
        assert WorkerState.DRAINING.value == "DRAINING"
        assert WorkerState.SHUTTING_DOWN.value == "SHUTTING_DOWN"
        assert WorkerState.STOPPED.value == "STOPPED"


class TestShutdownTimeoutConfig:
    def test_shutdown_timeout_default_is_30(self):
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
        )
        assert cfg.shutdown_timeout == 30

    def test_shutdown_timeout_env_var_override(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_SHUTDOWN_TIMEOUT", "60")
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
        )
        assert cfg.shutdown_timeout == 60


class TestActiveTaskTracking:
    async def test_on_message_adds_task_and_removes_on_done(self, worker, broker, backend):
        """_on_message stores created task in _active_tasks; task removed when done."""
        msg = make_message(_default_payload())

        await worker._on_message(msg)

        # Task should be tracked immediately after _on_message
        assert len(worker._active_tasks) == 1

        # Let the event loop run so the task completes
        await asyncio.gather(*worker._active_tasks)

        # After completion, done_callback should have removed it
        assert len(worker._active_tasks) == 0


class TestBrokerCancelConsume:
    async def test_cancel_consume_cancels_all_consumer_tags(self):
        """cancel_consume() calls queue.cancel() for each stored consumer tag."""
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._consume_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()

        # Simulate two queues being consumed
        mock_queue_1 = AsyncMock()
        mock_queue_1.consume.return_value = "tag-1"
        mock_queue_2 = AsyncMock()
        mock_queue_2.consume.return_value = "tag-2"
        broker._consume_channel.get_queue.side_effect = [mock_queue_1, mock_queue_2]

        callback = AsyncMock()
        await broker.consume("q1", callback, concurrency=10)
        await broker.consume("q2", callback, concurrency=10)

        await broker.cancel_consume()

        mock_queue_1.cancel.assert_awaited_once_with("tag-1")
        mock_queue_2.cancel.assert_awaited_once_with("tag-2")

    async def test_cancel_consume_isolates_per_consumer_failure(self):
        """A consumer whose cancel() raises must not prevent the remaining
        consumers from being cancelled, and _consumer_tags must be cleared
        regardless (2026-06-02 audit, P1-2). The error is surfaced only
        after every consumer has been attempted."""
        from swarmq.exceptions import BrokerConnectionError

        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._consume_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()

        mock_queue_1 = AsyncMock()
        mock_queue_1.consume.return_value = "tag-1"
        mock_queue_1.cancel.side_effect = RuntimeError("channel gone")
        mock_queue_2 = AsyncMock()
        mock_queue_2.consume.return_value = "tag-2"
        broker._consume_channel.get_queue.side_effect = [mock_queue_1, mock_queue_2]

        callback = AsyncMock()
        await broker.consume("q1", callback, concurrency=10)
        await broker.consume("q2", callback, concurrency=10)

        with pytest.raises(BrokerConnectionError):
            await broker.cancel_consume()

        # The second consumer was still cancelled despite the first raising.
        mock_queue_2.cancel.assert_awaited_once_with("tag-2")
        # Registry cleared so a redelivery window cannot reuse stale tags.
        assert broker._consumer_tags == []


class TestRequestShutdown:
    def test_request_shutdown_sets_draining_and_event(self, worker):
        """_request_shutdown transitions to DRAINING and sets the shutdown event."""
        worker._state = WorkerState.RUNNING

        worker._request_shutdown()

        assert worker._state is WorkerState.DRAINING
        assert worker._shutdown_event.is_set()

    def test_double_request_shutdown_is_idempotent(self, worker):
        """Second _request_shutdown call is ignored when already DRAINING."""
        worker._state = WorkerState.RUNNING

        worker._request_shutdown()
        assert worker._state is WorkerState.DRAINING

        # Second call should not change state
        worker._request_shutdown()
        assert worker._state is WorkerState.DRAINING


class TestShutdownSequence:
    async def test_shutdown_cancels_consume_waits_tasks_closes_connections(
        self, worker, broker, backend
    ):
        """Full shutdown: cancel_consume, await tasks, close broker+backend, STOPPED."""
        # Simulate the worker being in RUNNING state, then trigger shutdown
        worker._state = WorkerState.RUNNING

        # Create a fast task to be in-flight
        completed = asyncio.Event()

        async def _short_task():
            completed.set()

        task = asyncio.create_task(_short_task())
        worker._active_tasks.add(task)
        task.add_done_callback(worker._active_tasks.discard)

        # Trigger shutdown via the event (as start() would react to)
        worker._request_shutdown()
        await worker._execute_shutdown()

        # Verify sequence
        broker.cancel_consume.assert_awaited_once()
        broker.close.assert_awaited_once()
        backend.close.assert_awaited_once()
        assert worker._state is WorkerState.STOPPED

    async def test_drain_timeout_cancels_remaining_tasks(self, broker, backend, registry):
        """Tasks still running after shutdown_timeout are cancelled."""
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            concurrency=10,
            queues=["default"],
            shutdown_timeout=0,  # immediate timeout forces cancellation
        )
        worker = Worker(broker=broker, backend=backend, registry=registry, config=cfg)
        worker._state = WorkerState.RUNNING

        # Create a task that would block forever
        cancelled = asyncio.Event()

        async def _blocking_task():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        task = asyncio.create_task(_blocking_task())
        worker._active_tasks.add(task)
        task.add_done_callback(worker._active_tasks.discard)

        worker._request_shutdown()
        await worker._execute_shutdown()

        # Task should have been cancelled
        assert cancelled.is_set()
        assert worker._state is WorkerState.STOPPED


class TestHandleMessageDuringShutdown:
    async def test_handle_message_nacks_with_requeue_during_shutdown(
        self, worker, broker
    ):
        """After shutdown_event is set, _handle_message nacks with requeue=True."""
        # Set shutdown event before handling message
        worker._shutdown_event.set()

        msg = make_message(_default_payload())
        await worker._handle_message(msg)

        broker.redeliver.assert_awaited_once_with(msg)
        broker.ack.assert_not_awaited()


class TestShutdownNoActiveTasks:
    async def test_no_active_tasks_goes_straight_to_stopped(
        self, worker, broker, backend
    ):
        """When no tasks are in-flight, shutdown skips wait and reaches STOPPED."""
        worker._state = WorkerState.RUNNING
        assert len(worker._active_tasks) == 0

        worker._request_shutdown()
        await worker._execute_shutdown()

        broker.cancel_consume.assert_awaited_once()
        broker.close.assert_awaited_once()
        backend.close.assert_awaited_once()
        assert worker._state is WorkerState.STOPPED


class TestShutdownLockDrain:
    """Plan 008 Unit 5 step 3b — leftover handler locks must be released
    BEFORE backend.close so LockManager.release still has a working
    Valkey client. Tests the race-tolerant behavior: even if a handler's
    finally never ran (e.g. cancelled before reaching it), the leftover
    entries in _active_handler_locks get drained and the registry is
    cleared.
    """

    @pytest.mark.asyncio
    async def test_leftover_handler_locks_released_before_backend_close(
        self, worker, broker, backend
    ):
        import uuid as _uuid

        worker._state = WorkerState.RUNNING
        worker._lock_manager = MagicMock()
        worker._lock_manager.release = AsyncMock(return_value=True)

        # Simulate a handler that acquired locks but whose finally never
        # ran (cancelled mid-execute, before _release_acquired_locks).
        h1 = _uuid.uuid4()
        h2 = _uuid.uuid4()
        worker._active_handler_locks[h1] = [LockToken(name="res-A", owner="tok-1", max_concurrency=1)]
        worker._active_handler_locks[h2] = [
            LockToken(name="res-B", owner="tok-2", max_concurrency=3),
            LockToken(name="res-C", owner="tok-3", max_concurrency=1),
        ]

        # Track ordering: release calls must complete before backend.close
        release_calls_before_close: list[tuple[str, str, int]] = []
        original_release = worker._lock_manager.release

        async def _capture_release(*args):
            release_calls_before_close.append(args)
            return await original_release(*args)

        worker._lock_manager.release = _capture_release

        close_called_with_pending_releases: list[bool] = []
        original_backend_close = backend.close

        async def _check_close():
            close_called_with_pending_releases.append(
                len(release_calls_before_close) == 3
            )
            return await original_backend_close()

        backend.close = _check_close

        worker._request_shutdown()
        await worker._execute_shutdown()

        # All three locks released
        assert len(release_calls_before_close) == 3
        # Backend.close ran AFTER all releases
        assert close_called_with_pending_releases == [True]
        # Registry cleared
        assert worker._active_handler_locks == {}
        assert worker._state is WorkerState.STOPPED

    @pytest.mark.asyncio
    async def test_release_failure_during_drain_logged_but_does_not_crash_shutdown(
        self, worker, broker, backend
    ):
        """A LockManager.release that raises must not abort the shutdown
        sequence — log + continue, then proceed to backend.close. The
        TTL is the recovery safety net for any lock that ended up
        not released."""
        import uuid as _uuid

        worker._state = WorkerState.RUNNING
        worker._lock_manager = MagicMock()
        worker._lock_manager.release = AsyncMock(
            side_effect=ConnectionError("valkey down")
        )

        h1 = _uuid.uuid4()
        worker._active_handler_locks[h1] = [LockToken(name="res-X", owner="tok-X", max_concurrency=1)]

        worker._request_shutdown()
        # Must not raise
        await worker._execute_shutdown()

        # Backend still closed despite release failure
        backend.close.assert_awaited_once()
        # Registry cleared even on failure (TTL becomes the safety net)
        assert worker._active_handler_locks == {}
        assert worker._state is WorkerState.STOPPED

    @pytest.mark.asyncio
    async def test_drain_skipped_when_lock_manager_never_initialised(
        self, worker, broker, backend
    ):
        """Worker without LockManager (start() never called) must not
        crash in the drain step — the guard `if self._lock_manager is
        not None` short-circuits the empty-registry case."""
        worker._state = WorkerState.RUNNING
        worker._lock_manager = None
        # Even if entries somehow got in (defensive), drain skips entirely
        # without raising.

        worker._request_shutdown()
        await worker._execute_shutdown()

        backend.close.assert_awaited_once()
        assert worker._state is WorkerState.STOPPED


class TestMetricsSampler:
    """2026-06-02 audit P1-3: the sampler populates the previously-dead
    gauges (queue_depth / dlq_messages / worker_uptime_seconds) and emits
    WORKER_HEARTBEAT."""

    async def test_sample_metrics_once_sets_gauges_and_emits_heartbeat(
        self, worker, broker
    ):
        import time as _time

        from swarmq.metrics import Metrics
        from swarmq.signals import Signal

        m = Metrics()
        if not m.enabled:
            pytest.skip("prometheus_client missing")
        worker._metrics = m
        worker._worker_started_at = _time.monotonic() - 5
        broker.queue_depth = AsyncMock(return_value=12)
        broker.dead_letter_depth = AsyncMock(return_value=3)

        captured: list[dict] = []

        async def _on_heartbeat(**kw):
            captured.append(kw)

        worker._signal_dispatcher.connect(Signal.WORKER_HEARTBEAT, _on_heartbeat)

        await worker._sample_metrics_once()

        assert (
            m.queue_depth.labels(queue="default", kind="pending")._value.get()
            == 12
        )
        assert m.dlq_messages.labels(queue="default")._value.get() == 3
        assert (
            m.worker_uptime_seconds.labels(
                worker_id=str(id(worker))
            )._value.get()
            >= 5
        )
        assert len(captured) == 1
        assert captured[0]["uptime_seconds"] >= 5

    async def test_sample_metrics_once_survives_broker_error(self, worker, broker):
        from swarmq.exceptions import BrokerConnectionError
        from swarmq.metrics import Metrics

        m = Metrics()
        if not m.enabled:
            pytest.skip("prometheus_client missing")
        worker._metrics = m
        broker.queue_depth = AsyncMock(
            side_effect=BrokerConnectionError("broker down")
        )
        broker.dead_letter_depth = AsyncMock(return_value=0)

        # Per-queue error is logged and skipped; the call must not raise.
        await worker._sample_metrics_once()

    async def test_sample_metrics_once_survives_arbitrary_error(
        self, worker, broker,
    ):
        """Härtung R2 P2: der per-Queue-Guard fing NUR
        BrokerConnectionError — jeder andere Fehler (TimeoutError,
        ChannelInvalidStateError, Decode-Fehler) propagierte in den
        Sampler-Loop, der außer CancelledError nichts fängt -> Task tot,
        Gauges frieren still auf dem letzten Wert ein."""
        from swarmq.metrics import Metrics

        m = Metrics()
        if not m.enabled:
            pytest.skip("prometheus_client missing")
        worker._metrics = m
        broker.queue_depth = AsyncMock(
            side_effect=RuntimeError("channel in weird state")
        )
        broker.dead_letter_depth = AsyncMock(return_value=0)

        await worker._sample_metrics_once()  # must not raise

    async def test_metrics_sampler_loop_survives_iteration_error(
        self, worker, broker, monkeypatch,
    ):
        """Eine werfende Iteration darf den Sampler-Loop nicht beenden —
        die nächste Iteration muss laufen."""
        import contextlib

        from swarmq.metrics import Metrics

        m = Metrics()
        if not m.enabled:
            pytest.skip("prometheus_client missing")
        worker._metrics = m
        worker._config.metrics_sample_interval = 0.01

        calls: list[int] = []

        async def _flaky_sample():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("transient sampler explosion")

        monkeypatch.setattr(worker, "_sample_metrics_once", _flaky_sample)

        task = asyncio.create_task(worker._metrics_sampler())
        for _ in range(200):
            if len(calls) >= 2:
                break
            await asyncio.sleep(0.01)
        worker._shutdown_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert len(calls) >= 2, "sampler loop died after one error"


class TestPayloadShapeValidation:
    """2026-06-02 audit P2-6: a wrong-typed args/kwargs payload must be
    routed to the DLQ (nack requeue=False), not executed with garbage."""

    async def test_string_args_not_executed(self, worker, broker):
        # args="hello" would otherwise unpack character-by-character.
        msg = make_message(_default_payload(args="hello"))
        await worker._handle_message(msg)
        broker.route_via_dlx.assert_awaited_once_with(msg)
        # Task never ran (no result stored).
        assert len(worker._active_tasks) == 0

    async def test_non_dict_kwargs_not_executed(self, worker, broker):
        msg = make_message(_default_payload(kwargs=["not", "a", "dict"]))
        await worker._handle_message(msg)
        broker.route_via_dlx.assert_awaited_once_with(msg)


class TestLifecycleSignalTimeout:
    """Härtung R2 P1-9: Lifecycle-Dispatches (WORKER_SHUTDOWN als
    allererste Shutdown-Aktion, WORKER_ONLINE beim Start, Heartbeat)
    liefen ohne Timeout — ein blockierender User-Handler (z.B. ein
    hängender HTTP-POST an ein Alerting-System) blockierte den
    gesamten Graceful Shutdown bis zum SIGKILL: In-Flight-Tasks
    starben hart, Locks blieben bis TTL."""

    async def test_shutdown_not_blocked_by_hanging_handler(
        self, worker, broker, backend, monkeypatch,
    ):
        monkeypatch.setattr(
            "swarmq.worker.lifecycle._LIFECYCLE_DISPATCH_TIMEOUT_S", 0.1,
        )

        @worker._signal_dispatcher.on_signal(Signal.WORKER_SHUTDOWN)
        async def hanging(**kwargs):
            await asyncio.sleep(60)

        worker._request_shutdown()
        loop = asyncio.get_running_loop()
        started = loop.time()
        await asyncio.wait_for(worker._execute_shutdown(), timeout=5)
        elapsed = loop.time() - started

        assert elapsed < 4, f"shutdown blocked {elapsed:.1f}s by handler"
        # The full sequence still ran.
        broker.cancel_consume.assert_awaited_once()
        broker.close.assert_awaited_once()
        backend.close.assert_awaited_once()
        assert worker._state is WorkerState.STOPPED



# ---------------------------------------------------------------------------
# aus test_burst_mode.py
# ---------------------------------------------------------------------------

def _make_burst_worker(*, burst_mode: bool, idle_timeout: float = 0.5) -> Worker:
    cfg = SwarmQConfig(
        broker_url="amqp://x",
        backend_url="valkey://x",
        burst_mode=burst_mode,
        burst_idle_timeout=idle_timeout,
        concurrency=1,
    )
    broker = MagicMock()
    backend = MagicMock()
    registry = MagicMock()
    registry.get_all = MagicMock(return_value=[])
    w = Worker(broker, backend, registry, cfg)
    # _request_shutdown gates on RUNNING. Tests skip start() so flip
    # state manually.
    w._state = WorkerState.RUNNING
    return w


class TestBurstConfig:
    def test_burst_disabled_by_default(self):
        cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
        assert cfg.burst_mode is False
        assert cfg.burst_idle_timeout == 5.0

    def test_burst_can_be_enabled(self):
        cfg = SwarmQConfig(
            broker_url="amqp://x",
            backend_url="valkey://x",
            burst_mode=True,
            burst_idle_timeout=2.5,
        )
        assert cfg.burst_mode is True
        assert cfg.burst_idle_timeout == 2.5


class TestBurstMonitor:
    async def test_monitor_triggers_shutdown_after_idle_timeout(self):
        w = _make_burst_worker(burst_mode=True, idle_timeout=0.2)
        # Pretend the worker has been idle since startup (no message
        # ever received).
        w._last_message_received_at = time.monotonic()
        # Wait for the monitor to trip.
        await asyncio.wait_for(w._burst_monitor(), timeout=2.0)
        assert w._shutdown_event.is_set()

    async def test_monitor_does_not_trigger_when_active_tasks_present(self):
        """If a task is in-flight the monitor must NOT fire shutdown
        even if the queue has been quiet — the task itself is work."""
        w = _make_burst_worker(burst_mode=True, idle_timeout=0.2)
        w._last_message_received_at = time.monotonic()
        # Park a fake active task.
        sentinel_done = asyncio.Event()

        async def _fake_task():
            await sentinel_done.wait()

        active = asyncio.create_task(_fake_task())
        w._active_tasks.add(active)

        monitor = asyncio.create_task(w._burst_monitor())
        # Idle window elapses, but active_tasks is non-empty → no exit.
        await asyncio.sleep(0.5)
        assert not w._shutdown_event.is_set()

        # Drain the fake task and verify shutdown fires.
        sentinel_done.set()
        await active
        w._active_tasks.discard(active)
        await asyncio.wait_for(monitor, timeout=2.0)
        assert w._shutdown_event.is_set()

    async def test_monitor_clock_resets_on_message(self):
        """Each delivery resets the idle clock; the monitor only fires
        after a fresh full idle window."""
        w = _make_burst_worker(burst_mode=True, idle_timeout=0.4)
        w._last_message_received_at = time.monotonic()
        monitor = asyncio.create_task(w._burst_monitor())

        # 0.2s in, simulate a message — resets clock.
        await asyncio.sleep(0.2)
        assert not w._shutdown_event.is_set()
        w._last_message_received_at = time.monotonic()

        # 0.3s later (still within fresh window) — no shutdown.
        await asyncio.sleep(0.3)
        assert not w._shutdown_event.is_set()

        # Another 0.4s without message → must fire.
        await asyncio.wait_for(monitor, timeout=2.0)
        assert w._shutdown_event.is_set()

    async def test_monitor_returns_silently_on_cancel(self):
        w = _make_burst_worker(burst_mode=True, idle_timeout=10.0)
        w._last_message_received_at = time.monotonic()
        monitor = asyncio.create_task(w._burst_monitor())
        await asyncio.sleep(0.05)
        monitor.cancel()
        # Monitor catches CancelledError internally and returns; the
        # task therefore awaits without re-raising.
        await monitor
        assert not w._shutdown_event.is_set()



# ---------------------------------------------------------------------------
# aus test_worker_autowire.py
# ---------------------------------------------------------------------------

def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _make_autowire_worker(**cfg_kwargs) -> Worker:
    cfg = SwarmQConfig(
        broker_url="amqp://x",
        backend_url="valkey://x",
        **cfg_kwargs,
    )
    broker = MagicMock()
    backend = MagicMock()
    registry = MagicMock()
    registry.get_all = MagicMock(return_value=[])
    w = Worker(broker, backend, registry, cfg)
    return w


class TestAutoWire:
    async def test_metrics_server_not_constructed_when_port_unset(self):
        w = _make_autowire_worker()
        assert w._metrics is None
        assert w._metrics_server is None

    async def test_metrics_attrs_populated_after_start_simulation(self):
        """Simulate the auto-wire block directly — the full Worker.start()
        requires broker + backend + scheduler. We only need to verify
        that when metrics_port is set, the attributes are populated."""
        from swarmq.metrics import (
            Metrics,
            MetricsServer,
            install_signal_collectors,
        )

        port = _free_port()
        w = _make_autowire_worker(metrics_port=port, metrics_host="127.0.0.1")
        w._metrics = Metrics()
        install_signal_collectors(w._signal_dispatcher, w._metrics)
        w._metrics_server = MetricsServer(
            w._metrics, host=w._config.metrics_host,
            port=w._config.metrics_port,
        )
        await w._metrics_server.start()
        try:
            assert w._metrics is not None
            assert w._metrics_server is not None
        finally:
            await w._metrics_server.stop()

    async def test_auto_wire_cleanup_on_partial_start_failure(self):
        """ce-review COR-001 / REL-F-01: if the second HTTP server
        fails to bind, the first must be stopped before re-raise —
        otherwise its socket leaks for the process lifetime."""
        from swarmq.metrics import Metrics, MetricsServer

        # Pre-bind a port so health_server.start() will fail.
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        occupied_port = sock.getsockname()[1]
        sock.listen()

        port_m = _free_port()
        w = _make_autowire_worker(
            metrics_port=port_m,
            health_port=occupied_port,  # will conflict
        )

        # Manually run the auto-wire block (minus scheduler setup).
        from swarmq.health import HealthChecker, HealthServer

        w._metrics = Metrics()
        w._metrics_server = MetricsServer(
            w._metrics, host="127.0.0.1", port=port_m,
        )
        raised = False
        try:
            await w._metrics_server.start()
            checker = HealthChecker(w._broker, w._backend)
            w._health_server = HealthServer(
                checker, host="127.0.0.1", port=occupied_port,
            )
            try:
                await w._health_server.start()
            except RuntimeError:
                # Simulating the cleanup branch from worker.start().
                await w._metrics_server.stop()
                raised = True
                raise
        except RuntimeError:
            pass
        finally:
            sock.close()

        assert raised, "health_server.start() should have raised"
        # Post-cleanup: metrics_server socket must be stoppable a
        # second time without error (idempotent).
        await w._metrics_server.stop()


class TestConfigAutoWireDefaults:
    def test_metrics_defaults_to_none_and_loopback(self):
        cfg = SwarmQConfig(broker_url="x", backend_url="x")
        assert cfg.metrics_port is None
        assert cfg.metrics_host == "127.0.0.1"
        assert cfg.health_port is None
        assert cfg.health_host == "127.0.0.1"


@pytest.mark.timeout(5)
async def test_request_shutdown_marks_draining_on_supervisor_channel(worker):
    """R21/KTD10: im Drain gilt beim Supervisor die laengere Lebenszeichen-Frist."""
    import multiprocessing

    from swarmq import supervision as sv
    from swarmq.worker.lifecycle import WorkerState

    ch = sv.SupervisorChannel.create(multiprocessing.get_context("spawn"))
    sv.set_current_channel(ch)
    try:
        worker._state = WorkerState.RUNNING
        worker._request_shutdown()
    finally:
        sv.set_current_channel(None)
    assert ch.intent_value() == sv.INTENT_DRAINING
