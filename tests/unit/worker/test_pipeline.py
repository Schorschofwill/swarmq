"""Pipeline-Runner (Plan U19, KTD5): Gate-Reihenfolge, Ack-Ordering, FAF-Modus
und die drei terminalen Varianten des ``DeliveryOutcome``.

Die Tests sprechen nur die Aufrufreihenfolge an Broker und Backend an — nicht
die inneren Flags des Runners (AGENTS.md: Verhalten, nicht Struktur).

Test-Liste 0.9.0 (Zeitlimit):
- [x] haengende Schedfire-Finalisierung (Blackhole) blockiert den Runner
      nicht: nach ``backend_op_timeout`` Warnung, Zustellung abgeschlossen
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts import PublishReceipt
from swarmq.contracts.coordination import LockToken, RateLimitDecision
from swarmq.exceptions import BackendConnectionError
from swarmq.headers import HEADER_ENQUEUED_AT, HEADER_UNIQUE_HASH, HEADER_UNIQUE_UNTIL
from swarmq.middleware import Middleware, MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.workflow_orchestration import WorkflowStep, WorkflowStepEnqueuer
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

_HASH = "a" * 64
_SCHEDFIRE = "golden-sched:1700000000000"


class Recorder:
    """Protokolliert die Aufrufreihenfolge über Broker, Backend und Gates."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def spy(self, name: str, result: object = None, exc: Exception | None = None) -> AsyncMock:
        async def _side_effect(*args: object, **kwargs: object) -> object:
            self.calls.append(name)
            if exc is not None:
                raise exc
            return result

        return AsyncMock(side_effect=_side_effect)

    def index(self, name: str) -> int:
        return self.calls.index(name)


class GatedTask(Task):
    task_info = TaskInfo(
        name="pipe-gated",
        queues=["default"],
        delivery=DeliveryMode.FIRE_AND_FORGET,
        rate_limit="10/m",
        lock="resource",
        hard_timeout=60,
    )

    async def run(self) -> str:
        return "ok"


class PlainTask(Task):
    task_info = TaskInfo(name="pipe-plain", queues=["default"], max_retries=0)

    async def run(self) -> str:
        return "ok"


class LockedTask(Task):
    task_info = TaskInfo(name="pipe-locked", queues=["default"], lock="resource", hard_timeout=60)

    async def run(self) -> str:
        return "ok"


class ExpiringTask(Task):
    task_info = TaskInfo(name="pipe-expiring", queues=["default"], max_age=1)

    async def run(self) -> str:
        return "ok"


class FafTask(Task):
    task_info = TaskInfo(name="pipe-faf", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET)

    async def run(self) -> str:
        return "ok"


def _payload(name: str, task_id: str = "t-1") -> dict:
    return {"task": {"id": task_id, "queue": "default", "name": name}, "args": [], "kwargs": {}}


@pytest.fixture
def rec() -> Recorder:
    return Recorder()


@pytest.fixture
def broker(rec: Recorder) -> MagicMock:
    b = MagicMock(spec=RabbitMQBroker)
    b.ack = rec.spy("broker.ack")
    b.redeliver = rec.spy("broker.redeliver")
    b.route_via_dlx = rec.spy("broker.route_via_dlx")
    b.dead_letter = rec.spy("broker.dead_letter")
    return b


@pytest.fixture
def backend(rec: Recorder) -> MagicMock:
    be = MagicMock(spec=ValkeyBackend)
    be.store_result = rec.spy("backend.store_result")
    be.notify_result = rec.spy("backend.notify_result")
    be.is_cancelled = rec.spy("backend.is_cancelled", False)
    be.is_workflow_cancelled = rec.spy("backend.is_workflow_cancelled", False)
    be.clear_cancel_flag = rec.spy("backend.clear_cancel_flag")
    be.schedules.is_fire_completed = rec.spy("backend.schedules.is_fire_completed", False)
    be.schedules.claim_fire = rec.spy("backend.schedules.claim_fire", True)
    be.schedules.mark_fire_completed = rec.spy("backend.schedules.mark_fire_completed", True)
    be.schedules.release_fire_claim = rec.spy("backend.schedules.release_fire_claim")
    be.release_unique = rec.spy("backend.release_unique", True)
    return be


def _worker(broker: MagicMock, backend: MagicMock, rec: Recorder, *tasks: type[Task], **kwargs: object) -> Worker:
    registry = TaskRegistry()
    for task in tasks:
        registry.register(task)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", concurrency=4, queues=["default"])
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config, **kwargs)
    limiter = MagicMock()
    limiter.check = rec.spy("rate_limiter.check", RateLimitDecision(allowed=True, remaining=9, retry_after_ms=0))
    worker._rate_limiter = limiter
    locks = MagicMock()
    locks.acquire = rec.spy("lock_manager.acquire", LockToken(name="res", owner="tok", max_concurrency=1))
    locks.release = rec.spy("lock_manager.release", True)
    worker._lock_manager = locks
    return worker


_GATE_ORDER = [
    "backend.schedules.is_fire_completed",
    "backend.schedules.claim_fire",
    "backend.is_cancelled",
    "rate_limiter.check",
    "lock_manager.acquire",
    "backend.release_unique",  # Dedup-Start-Release
    "broker.ack",  # FAF-Ack nach dem letzten Gate
]


async def test_gates_run_in_fixed_order_and_ack_comes_last(broker, backend, rec) -> None:
    worker = _worker(broker, backend, rec, GatedTask)
    msg = make_message(
        _payload("pipe-gated"),
        headers={"x-swarmq-schedfire": _SCHEDFIRE, HEADER_UNIQUE_HASH: _HASH, HEADER_UNIQUE_UNTIL: "start"},
    )

    await worker._handle_message(msg)

    observed = [c for c in rec.calls if c in _GATE_ORDER]
    assert observed == _GATE_ORDER
    assert not any(c in rec.calls for c in ("broker.redeliver", "broker.route_via_dlx"))


async def test_no_broker_verb_before_last_gate_returns_continue(broker, backend, rec) -> None:
    worker = _worker(broker, backend, rec, GatedTask)
    msg = make_message(_payload("pipe-gated"), headers={"x-swarmq-schedfire": _SCHEDFIRE})

    await worker._handle_message(msg)

    first_verb = min(i for i, c in enumerate(rec.calls) if c.startswith("broker."))
    last_gate = max(rec.index(g) for g in ("rate_limiter.check", "lock_manager.acquire", "backend.is_cancelled"))
    assert first_verb > last_gate


async def test_faf_acks_exactly_once_and_no_verb_after_failure(broker, backend, rec) -> None:
    """FAF: genau ein Ack vor der Ausführung; ein späterer Fehler (hier: die
    Middleware-Factory) löst kein weiteres Broker-Verb mehr aus."""

    def broken_factory():
        raise RuntimeError("factory down")

    worker = _worker(broker, backend, rec, FafTask, middleware_chain=broken_factory)
    msg = make_message(_payload("pipe-faf"))

    await worker._handle_message(msg)

    assert rec.calls.count("broker.ack") == 1
    assert not any(c in rec.calls for c in ("broker.redeliver", "broker.route_via_dlx"))
    assert "broker.dead_letter" not in rec.calls


async def test_redelivered_outcome_keeps_dedup_flag_and_clears_inflight_claim(broker, backend, rec) -> None:
    backend.store_result = rec.spy("backend.store_result", exc=BackendConnectionError("down"))
    worker = _worker(broker, backend, rec, PlainTask)
    msg = make_message(
        _payload("pipe-plain"), headers={"x-swarmq-schedfire": _SCHEDFIRE, HEADER_UNIQUE_HASH: _HASH}
    )

    await worker._handle_message(msg)

    assert rec.calls.count("broker.redeliver") == 1
    broker.redeliver.assert_awaited_once_with(msg)
    assert "broker.ack" not in rec.calls
    assert "backend.release_unique" not in rec.calls
    assert "backend.schedules.release_fire_claim" in rec.calls
    assert "backend.schedules.mark_fire_completed" not in rec.calls


async def test_terminal_release_in_finally_releases_after_ack_and_writes_completed_marker(
    broker, backend, rec
) -> None:
    worker = _worker(broker, backend, rec, PlainTask)
    msg = make_message(
        _payload("pipe-plain"), headers={"x-swarmq-schedfire": _SCHEDFIRE, HEADER_UNIQUE_HASH: _HASH}
    )

    await worker._handle_message(msg)

    assert rec.index("broker.ack") < rec.index("backend.release_unique")
    assert rec.calls.count("backend.release_unique") == 1
    assert "backend.schedules.mark_fire_completed" in rec.calls
    assert "backend.schedules.release_fire_claim" not in rec.calls


async def test_terminal_already_released_does_not_release_twice(broker, backend, rec) -> None:
    """Lock-Konflikt am Cap: Dead-Letter + Freigabe inline — der finally gibt nicht erneut frei."""
    worker = _worker(broker, backend, rec, LockedTask)
    worker._lock_manager.acquire = rec.spy("lock_manager.acquire", None)
    msg = make_message(
        _payload("pipe-locked"),
        headers={HEADER_UNIQUE_HASH: _HASH, "x-swarmq-lock-conflict-count": 9},
    )

    await worker._handle_message(msg)

    assert rec.calls.count("broker.dead_letter") == 1  # bestätigt das Original selbst (U8)
    assert "broker.ack" not in rec.calls
    assert rec.calls.count("backend.release_unique") == 1
    assert rec.index("broker.dead_letter") < rec.index("backend.release_unique")


async def test_terminal_never_release_keeps_dedup_flag_on_expiry(broker, backend, rec) -> None:
    worker = _worker(broker, backend, rec, ExpiringTask)
    msg = make_message(
        _payload("pipe-expiring"),
        headers={HEADER_UNIQUE_HASH: _HASH, HEADER_ENQUEUED_AT: int(time.time()) - 3600},
    )

    await worker._handle_message(msg)

    assert rec.calls.count("broker.dead_letter") == 1  # bestätigt das Original selbst (U8)
    assert "broker.ack" not in rec.calls
    assert "backend.release_unique" not in rec.calls


async def test_unhandled_pipeline_exception_is_terminal_with_release_and_marker(
    broker, backend, rec, swarmq_logs
) -> None:
    chain = MagicMock()
    chain.run_pre_execute = AsyncMock(side_effect=RuntimeError("hook exploded"))
    worker = _worker(broker, backend, rec, PlainTask, middleware_chain=lambda: chain)
    msg = make_message(
        _payload("pipe-plain"), headers={"x-swarmq-schedfire": _SCHEDFIRE, HEADER_UNIQUE_HASH: _HASH}
    )

    await worker._handle_message(msg)

    # Seit B22 entweicht der Fehler nicht mehr aus der Task, er wird geloggt.
    [failed] = [e for e in swarmq_logs if e["event"] == "delivery_handler_failed"]
    assert failed["error"] == "hook exploded"

    assert "backend.release_unique" in rec.calls
    assert "backend.schedules.mark_fire_completed" in rec.calls


class TestRejectReasons:
    """U6 (R12/AE5): Jeder Weg ins ``.XQ`` nennt seinen Grund.

    Die Gruende sind ein fester Satz -- ein freier Text waere als Label
    unbrauchbar (Kardinalitaet) und als Alert-Bedingung unzuverlaessig.
    """

    @staticmethod
    def _worker_with_spy(broker, backend, *tasks):
        # Die Conftest-Fixtures bringen die Cancel-Flag-Defaults mit; ein
        # nackter ``MagicMock(spec=...)`` liefert dort truthy Mocks und der
        # Pre-Start-Cancel-Gate greift, bevor der Reject-Pfad erreicht ist.
        registry = TaskRegistry()
        for task in tasks:
            registry.register(task)
        config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
        worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
        events: list[dict] = []

        async def _dispatch(signal, **kwargs):
            events.append({"signal": signal, **kwargs})

        worker._signal_dispatcher.dispatch = AsyncMock(side_effect=_dispatch)
        return worker, broker, events

    @staticmethod
    def _rejections(events):
        from swarmq.signals import Signal

        return [e for e in events if e["signal"] is Signal.TASK_REJECTED]

    async def test_oversized_body(self, broker, backend):
        worker, _broker, events = self._worker_with_spy(broker, backend)
        msg = MagicMock()
        msg.body = b"x" * (worker._config.max_message_body_bytes + 1)
        msg.headers = {}
        msg.queue = "tasks.high"

        await worker._handle_message(msg)

        (rejected,) = self._rejections(events)
        assert rejected["reason"] == "body_too_large"
        assert rejected["queue"] == "tasks.high"
        assert rejected["task_id"] is None and rejected["task_name"] is None

    async def test_undecodable_body(self, broker, backend):
        worker, _broker, events = self._worker_with_spy(broker, backend)
        msg = MagicMock()
        msg.body = b"not json at all"
        msg.headers = {}
        msg.queue = "default"

        await worker._handle_message(msg)

        (rejected,) = self._rejections(events)
        assert rejected["reason"] == "undecodable"
        assert rejected["task_id"] is None

    async def test_unregistered_task_names_the_task(self, broker, backend):
        worker, _broker, events = self._worker_with_spy(broker, backend)
        msg = make_message(_default_payload(name="not-registered"))
        msg.queue = "default"

        await worker._handle_message(msg)

        (rejected,) = self._rejections(events)
        assert rejected["reason"] == "unregistered_task"
        assert rejected["task_name"] == "not-registered"

    async def test_unregistered_task_carries_the_decoded_task_id(self, broker, backend):
        """B14 (Loadsuite-Testlauf v0.6.0): Der Docstring verspricht ``task_id``
        nur dann als ``None``, wenn der Body nie dekodiert wurde. Bei einem
        unregistrierten Task ist er dekodiert -- ``task_not_found`` loggt die
        Kennung --, das Signal trug sie trotzdem nicht. Ohne Kennung ist die
        Nachricht im ``.XQ`` nicht wiederzufinden (``swarmq dlq inspect``)."""
        worker, _broker, events = self._worker_with_spy(broker, backend)
        payload = _default_payload(name="not-registered")
        msg = make_message(payload)
        msg.queue = "default"

        await worker._handle_message(msg)

        (rejected,) = self._rejections(events)
        assert rejected["task_id"] == payload["task"]["id"]

    async def test_broken_middleware_chain_factory(self, broker, backend):
        class Ok(Task):
            task_info = TaskInfo(name="reject-ok", queues=["default"])

            async def run(self) -> str:
                return "ok"

        worker, _broker, events = self._worker_with_spy(broker, backend, Ok)

        def boom():
            raise RuntimeError("factory exploded")

        worker._middleware_chain_factory = boom
        msg = make_message(_default_payload(name="reject-ok"))
        msg.queue = "default"

        await worker._handle_message(msg)

        (rejected,) = self._rejections(events)
        assert rejected["reason"] == "middleware_chain_factory"
        assert "factory exploded" in rejected["error"]

    async def test_broken_middleware_chain_factory_carries_the_identity(self, broker, backend):
        """B14, zweiter Weg mit dekodiertem Body: Auch hier stehen Kennung und
        Name fest, bevor die Kette gebaut wird."""
        class Ok(Task):
            task_info = TaskInfo(name="reject-ok-id", queues=["default"])

            async def run(self) -> str:
                return "ok"

        worker, _broker, events = self._worker_with_spy(broker, backend, Ok)

        def boom():
            raise RuntimeError("factory exploded")

        worker._middleware_chain_factory = boom
        payload = _default_payload(name="reject-ok-id")
        msg = make_message(payload)
        msg.queue = "default"

        await worker._handle_message(msg)

        (rejected,) = self._rejections(events)
        assert rejected["task_id"] == payload["task"]["id"]
        assert rejected["task_name"] == "reject-ok-id"

    async def test_broken_middleware_chain_factory_logs_error_fields(self, broker, backend, swarmq_logs):
        """Plan 2026-10-06-0831, U9 (R17)."""
        class Ok(Task):
            task_info = TaskInfo(name="reject-ok-log", queues=["default"])

            async def run(self) -> str:
                return "ok"

        worker, _broker, _events = self._worker_with_spy(broker, backend, Ok)

        def boom():
            raise RuntimeError("factory exploded")

        worker._middleware_chain_factory = boom
        msg = make_message(_default_payload(name="reject-ok-log"))
        msg.queue = "default"

        await worker._handle_message(msg)

        [entry] = [e for e in swarmq_logs if e["event"] == "middleware_chain_factory_failed"]
        assert entry["logger"] == "swarmq.worker.pipeline"
        assert entry["log_level"] == "error"
        assert entry["error_type"] == "RuntimeError" and entry["error"] == "factory exploded"

class _LockedForLog(Task):
    task_info = TaskInfo(name="locked_for_log", queues=["default"], lock="resource", hard_timeout=60)

    async def run(self) -> None:
        return None


async def test_lock_release_failure_is_a_warning_with_error_fields(broker, backend, swarmq_logs) -> None:
    """Plan 2026-10-06-0831, U9 (R17, KTD11): die Lock-TTL heilt den Fall
    selbst -- Degradierung mit automatischer Erholung, also WARNING."""
    registry = TaskRegistry()
    registry.register(_LockedForLog)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = AsyncMock(
        side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(
            name=name, owner=owner, max_concurrency=max_concurrency
        )
    )
    worker._lock_manager.release = AsyncMock(side_effect=BackendConnectionError("valkey down"))

    await worker._handle_message(make_message(_default_payload(name="locked_for_log", args=[], kwargs={})))

    [entry] = [e for e in swarmq_logs if e["event"] == "lock_release_failed"]
    assert entry["logger"] == "swarmq.worker.pipeline"
    assert entry["log_level"] == "warning"
    assert entry["lock_name"] == "resource"
    assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"


class _ScheduledForLog(Task):
    task_info = TaskInfo(name="scheduled_for_log", queues=["default"])

    async def run(self) -> str:
        return "ok"


async def test_schedfire_finalize_failure_is_a_warning_with_error_fields(broker, backend, swarmq_logs) -> None:
    """Plan 2026-10-06-0831, U9 (R17): der In-flight-Marker heilt per TTL."""
    registry = TaskRegistry()
    registry.register(_ScheduledForLog)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    backend.schedules.mark_fire_completed.side_effect = BackendConnectionError("valkey down")
    msg = make_message(
        _default_payload(name="scheduled_for_log", args=[], kwargs={}),
        headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
    )

    await worker._handle_message(msg)

    [entry] = [e for e in swarmq_logs if e["event"] == "schedfire_finalize_failed"]
    assert entry["logger"] == "swarmq.worker.pipeline"
    assert entry["log_level"] == "warning"
    assert entry["schedfire_key"] == "cron_a:1700000000000" and entry["terminal"] is True
    assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"


@pytest.mark.timeout(5)
async def test_hanging_schedfire_finalize_is_bounded_by_backend_op_timeout(broker, backend, swarmq_logs) -> None:
    registry = TaskRegistry()
    registry.register(_ScheduledForLog)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", backend_op_timeout=0.05)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    never = asyncio.Event()

    async def _hang(*_args, **_kwargs):
        await never.wait()

    backend.schedules.mark_fire_completed.side_effect = _hang
    msg = make_message(
        _default_payload(name="scheduled_for_log", args=[], kwargs={}),
        headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
    )

    await worker._handle_message(msg)

    [entry] = [e for e in swarmq_logs if e["event"] == "schedfire_finalize_failed"]
    assert entry["error_type"] == "BackendConnectionError"


class _EnqueueRecorder(Middleware):
    """Middleware, die den Enqueue-Pfad protokolliert (U11, AE6)."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def pre_enqueue(self, message) -> None:
        self.calls.append(f"pre_enqueue:{message['step_path']}:{message['delivery_mode']}")

    async def post_enqueue(self, message) -> None:
        self.calls.append(f"post_enqueue:{message['step_path']}")


async def test_workflow_step_publisher_runs_middleware_then_signals():
    """R15/R16 (AE6): Ein Folgeschritt laeuft durch dieselbe Kette wie
    ``Client.schedule`` — pre_enqueue, Publish, post_enqueue, dann
    ``TASK_ENQUEUED`` und ``WORKFLOW_STEP_ENQUEUED``."""
    calls: list[str] = []
    broker = MagicMock(spec=RabbitMQBroker)

    async def _publish(queue, body, headers=None):
        calls.append("publish")
        return PublishReceipt(queue=queue)

    broker.publish = AsyncMock(side_effect=_publish)
    backend = MagicMock(spec=ValkeyBackend)
    registry = TaskRegistry()
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    worker = Worker(
        broker=broker, backend=backend, registry=registry, config=config,
        middleware_chain=MiddlewareChain([_EnqueueRecorder(calls)]),
    )

    async def _dispatch(signal, **kwargs):
        calls.append(f"signal:{signal.value}:{kwargs.get('step_path', '-')}")

    worker._signal_dispatcher.dispatch = AsyncMock(side_effect=_dispatch)

    step = WorkflowStep(
        workflow_id="wf-1", step_path="/1", task_id="t1", task_name="echo",
        queue="default", args=[1], kwargs={}, headers={"x-swarmq-workflow-step": "/1"},
    )
    publisher = WorkflowStepEnqueuer(
        publish=worker._publish_workflow_step,
        dispatch=worker._signal_dispatcher.dispatch,
        get_chain=worker._get_chain,
    )
    await publisher.pre_enqueue([step])
    receipt = await publisher.publish(step)

    assert isinstance(receipt, PublishReceipt)
    assert calls == [
        "pre_enqueue:/1:at_least_once",
        "publish",
        "post_enqueue:/1",
        "signal:task_enqueued:-",
        "signal:workflow_step_enqueued:/1",
    ]
