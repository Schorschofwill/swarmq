"""Lock-Gate (Plan U7): aus ``test_worker.py`` verschobene Fälle plus direkte Outcome-Tests.

Test-Liste 0.9.0 (Kopie und Broker-Zaehler):
- [x] die Rueckstellungs-Kopie bei Lock-Konflikt traegt weder
      ``x-delivery-count`` noch ``x-acquired-count`` des Originals
- [x] Backend-Fehler beim Acquire -> genau eine Zeile (WARNING
      ``task_deferred_backend_unavailable``, ``stage=acquire``, ``lock_name``)
- [x] anderer Fehler beim Acquire -> weiter ERROR ``lock_acquire_failed``

Test-Liste 0.9.0 (Zeitlimit):
- [x] haengendes Acquire (Blackhole) -> nach ``backend_op_timeout``
      zurueckgestellt (``stage=acquire``), kein Lock registriert
- [x] Konflikt-Kopie traegt keinen ``x-swarmq-backend-defer-count``

Test-Liste 0.9.0 (drittes Review):
- [x] Fehlerantwort des Backends beim Acquire (``BackendResponseError``,
      z. B. WRONGTYPE auf dem Lock-Key) -> kein Ausfall: ERROR
      ``lock_acquire_failed`` und Requeue, keine Zurueckstellung
- [x] die Konflikt-Kopie einer Schedfire-Zustellung mit gewonnenem Claim
      traegt dessen Owner-Token (``x-swarmq-schedfire-claim``)

Test-Liste BZ-17 (Plan 0.12.0, MR 2):
- [x] ``x-swarmq-lock-render-error`` ist hoechstens 8 KiB lang, auch wenn der
      Renderfehler einen riesigen Argumentnamen zitiert
"""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from swarmq.contracts.coordination import LockToken
from swarmq.exceptions import BackendConnectionError, BrokerConnectionError
from swarmq.headers import HEADER_LOCK_CONFLICTS, HEADER_LOCK_WAIT_SINCE
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.lock import render_lock_name
from swarmq.worker.outcome import Continue, Redelivered
from tests.support.messages import make_message
from tests.unit.worker._helpers import (
    AddTask,
    _default_payload,
    lock_wait_exceeded_headers,
)

pytestmark = pytest.mark.timeout(5)


class LockedTask(Task):
    task_info = TaskInfo(
        name="locked", queues=["default"], lock="resource", hard_timeout=60
    )


class FireAndForgetLockedTask(Task):
    task_info = TaskInfo(
        name="ff_locked",
        queues=["default"],
        delivery=DeliveryMode.FIRE_AND_FORGET,
        lock="resource",
        hard_timeout=60,
    )


class TestWorkerLocking:
    """Unit-level coverage of the lock branch in _handle_message.

    A real Worker is built but ``_lock_manager`` is replaced with a
    mock so no Valkey is needed.
    """

    @pytest.mark.asyncio
    async def test_acquired_path_runs_the_task(self, broker, backend, config):
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(
                name=name, owner=owner, max_concurrency=max_concurrency
            )
        )
        w._lock_manager.release = AsyncMock(return_value=True)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))

        async def _run(self):
            return None

        with patch.object(LockedTask, "run", _run):
            await w._handle_message(msg)

        w._lock_manager.acquire.assert_awaited_once()
        w._lock_manager.release.assert_awaited_once()
        broker.retry_later.assert_not_called()
        broker.ack.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_conflict_path_requeues_with_delay_and_skips_execution(
        self, broker, backend, config
    ):
        from swarmq.signals import SignalDispatcher

        dispatcher = SignalDispatcher()
        seen: list[dict] = []

        @dispatcher.on_signal(Signal.TASK_LOCKED)
        async def _handler(**kwargs: object) -> None:
            seen.append(dict(kwargs))

        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(
            broker=broker, backend=backend, registry=reg, config=config,
            signal_dispatcher=dispatcher,
        )
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)
        w._lock_manager.release = AsyncMock(return_value=True)

        ran = False

        async def _run(self):
            nonlocal ran
            ran = True

        with patch.object(LockedTask, "run", _run):
            msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
            await w._handle_message(msg)

        assert ran is False, "task.run must NOT run when lock conflicted"
        broker.retry_later.assert_awaited_once()
        # U8: retry_later/dead_letter bestätigen das Original selbst
        broker.ack.assert_not_awaited()
        broker.dead_letter.assert_not_called()
        # Release is NOT called when acquire returned False
        w._lock_manager.release.assert_not_awaited()
        assert len(seen) == 1
        assert seen[0]["task_name"] == "locked"
        assert seen[0]["lock_name"] == "resource"
        assert seen[0]["lock_conflict_count"] == 1

    @pytest.mark.asyncio
    async def test_no_lock_skips_lock_manager_entirely(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(AddTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        msg = make_message(_default_payload())
        await w._handle_message(msg)

        w._lock_manager.acquire.assert_not_called()

    # --- Mandatory regression #1 ---------------------------------------------

    @pytest.mark.asyncio
    async def test_lock_set_but_lock_manager_none_falls_through_to_execution(
        self, broker, backend, config
    ):
        """Defensive guard: ``_lock_manager is None`` (Worker.start() was
        never called) must not crash. The task simply runs unprotected —
        better than silently dropping it. Same shape as
        test_rate_limit_set_but_limiter_none_falls_through_to_execution.
        """
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        # _lock_manager intentionally stays None.

        ran = False

        async def _run(self):
            nonlocal ran
            ran = True

        with patch.object(LockedTask, "run", _run):
            msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
            await w._handle_message(msg)

        assert ran is True
        broker.ack.assert_awaited_once_with(msg)
        broker.retry_later.assert_not_called()

    # --- Mandatory regression #2 ---------------------------------------------

    @pytest.mark.asyncio
    async def test_lock_check_connection_error_defers(
        self, broker, backend, config
    ):
        """Valkey unreachable mid-acquire must NOT drop the message: it is
        deferred via the delay queue (new message, own delivery budget),
        not requeued in place. The lock-conflict counter is untouched —
        a backend outage is no conflict."""
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=BackendConnectionError("valkey down")
        )

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        await w._handle_message(msg)

        broker.retry_later.assert_awaited_once()
        assert HEADER_LOCK_CONFLICTS not in broker.retry_later.await_args.kwargs["headers"]
        broker.redeliver.assert_not_awaited()
        broker.ack.assert_not_awaited()


class TestFireAndForgetLock:
    """Mandatory regression #3 — FIRE_AND_FORGET + lock-stage failure must
    NOT drop the message. Same shape as TestFireAndForgetRateLimit."""

    @pytest.mark.asyncio
    async def test_fire_and_forget_lock_failure_does_not_drop_message(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(FireAndForgetLockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=BackendConnectionError("simulated valkey hiccup")
        )

        msg = make_message(_default_payload(name="ff_locked", args=[], kwargs={}))
        await w._handle_message(msg)

        # Zurueckgestellt (retry_later bestaetigt erst nach dem Confirm der
        # Kopie); das FAF-Frueh-Ack wird nie erreicht.
        broker.retry_later.assert_awaited_once()
        broker.redeliver.assert_not_awaited()
        broker.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fire_and_forget_lock_conflict_acks_once(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(FireAndForgetLockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        msg = make_message(_default_payload(name="ff_locked", args=[], kwargs={}))
        await w._handle_message(msg)

        broker.retry_later.assert_awaited_once()
        # U8: retry_later bestätigt selbst; das FAF-Ack wird nie erreicht.
        broker.ack.assert_not_awaited()


class TestLockRequeueCounter:
    """Mandatory regression #4 — x-swarmq-lock-conflict-count header
    increments on every conflict so operators can see a wedged lock.
    Same shape as TestRateLimitRequeueCounter."""

    @pytest.mark.asyncio
    async def test_first_lock_conflict_sets_counter_to_one(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {}

        await w._handle_message(msg)

        _, kwargs = broker.retry_later.call_args
        assert kwargs["headers"][HEADER_LOCK_CONFLICTS] == 1
        # Wedged-lock recovery: DLQ (and requeue) messages must carry the
        # rendered lock name so operators can `valkey-cli DEL sq:lock:<name>`.
        assert kwargs["headers"]["x-swarmq-lock-name"] == "resource"

    @pytest.mark.asyncio
    async def test_existing_counter_is_incremented(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {HEADER_LOCK_CONFLICTS: 5}

        await w._handle_message(msg)

        _, kwargs = broker.retry_later.call_args
        assert kwargs["headers"][HEADER_LOCK_CONFLICTS] == 6

    @pytest.mark.asyncio
    async def test_wait_limit_routes_to_dlq(
        self, broker, backend, config
    ):
        """MR 13 (BZ-05): statt des Deckels 10 endet die Wartezeit an
        ``lock_wait_max_s`` ab dem ersten Konflikt -- dann DLQ."""
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = lock_wait_exceeded_headers(config)

        await w._handle_message(msg)

        # No more requeue
        broker.retry_later.assert_not_called()
        # DLQ instead, with diagnostic header
        broker.dead_letter.assert_awaited_once()
        _, kwargs = broker.dead_letter.call_args
        assert kwargs["headers"]["x-swarmq-lock-name"] == "resource"
        assert kwargs["headers"]["x-swarmq-traceback"].startswith("LockWaitExceeded: lock wait limit reached")
        # U8: retry_later/dead_letter bestätigen das Original selbst
        broker.ack.assert_not_awaited()

    async def test_wait_just_below_the_limit_still_requeues(
        self, broker, backend, config
    ):
        """Boundary check: kurz vor ``lock_wait_max_s`` wird weiter
        zurueckgestellt; die Kopie behaelt den ersten Konflikt."""
        import time

        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        since = int(time.time() * 1000) - int(config.lock_wait_max_s * 1000) + 5_000
        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {HEADER_LOCK_WAIT_SINCE: since, HEADER_LOCK_CONFLICTS: 8}

        await w._handle_message(msg)

        broker.retry_later.assert_awaited_once()
        broker.dead_letter.assert_not_called()
        _, kwargs = broker.retry_later.call_args
        assert kwargs["headers"][HEADER_LOCK_CONFLICTS] == 9
        assert kwargs["headers"][HEADER_LOCK_WAIT_SINCE] == since

    async def test_dlq_failure_at_wait_limit_drops_message_no_infinite_loop(
        self, broker, backend, config
    ):
        """Cascade-fix regression: when dead_letter fails
        AT the wait limit, falling back to nack(requeue=True) would let the
        message bounce back past the limit, hit the same DLQ failure,
        and loop forever without backoff (the requeue branch is
        unreachable past the limit). Honest data loss with a loud ERROR
        log naming the lock is the right trade-off."""
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)
        broker.dead_letter = AsyncMock(
            side_effect=ConnectionError("broker down")
        )

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = lock_wait_exceeded_headers(config)

        await w._handle_message(msg)

        # nack(requeue=False) — drop the message, do not requeue into the
        # bounce loop.
        broker.route_via_dlx.assert_awaited_once_with(msg)
        # No ack happened.
        broker.ack.assert_not_awaited()
        # No requeue either — the requeue branch is unreachable past the limit.
        broker.retry_later.assert_not_called()

    async def test_dlq_failure_at_wait_limit_emits_task_rejected(
        self, broker, backend, config
    ):
        """U6 (R12): Der Datenverlust bleibt, aber er ist nicht mehr stumm --
        vorher sah ein Betreiber nur eine ERROR-Zeile und einen Gauge."""
        from swarmq.signals import Signal

        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)
        broker.dead_letter = AsyncMock(side_effect=ConnectionError("broker down"))
        events: list[dict] = []

        async def _dispatch(signal, **kwargs):
            events.append({"signal": signal, **kwargs})

        w._signal_dispatcher.dispatch = AsyncMock(side_effect=_dispatch)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = lock_wait_exceeded_headers(config)

        await w._handle_message(msg)

        rejected = [e for e in events if e["signal"] is Signal.TASK_REJECTED]
        assert len(rejected) == 1
        assert rejected[0]["reason"] == "dlq_publish_failed"
        assert rejected[0]["task_name"] == "locked"
        assert "broker down" in rejected[0]["error"]


class FormatStringLockTask(Task):
    task_info = TaskInfo(
        name="user_charge",
        queues=["default"],
        lock="user-{user_id}",
        hard_timeout=60,
    )

    async def run(self, user_id: int, amount: int) -> None:
        return None


class TestFormatStringLock:
    """Lock names with format placeholders are rendered at acquire-time
    via inspect.signature.bind, so positional and keyword args both
    fill the template."""

    @pytest.mark.asyncio
    async def test_keyword_args_render_lock_name(self, broker, backend, config):
        reg = TaskRegistry()
        reg.register(FormatStringLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(
                name=name, owner=owner, max_concurrency=max_concurrency
            )
        )
        w._lock_manager.release = AsyncMock(return_value=True)

        msg = make_message(
            _default_payload(name="user_charge", args=[], kwargs={"user_id": 42, "amount": 100})
        )
        await w._handle_message(msg)

        args, _ = w._lock_manager.acquire.call_args
        assert args[0] == "user-42"

    @pytest.mark.asyncio
    async def test_positional_args_render_lock_name(self, broker, backend, config):
        reg = TaskRegistry()
        reg.register(FormatStringLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(
                name=name, owner=owner, max_concurrency=max_concurrency
            )
        )
        w._lock_manager.release = AsyncMock(return_value=True)

        msg = make_message(
            _default_payload(name="user_charge", args=[7, 100], kwargs={})
        )
        await w._handle_message(msg)

        args, _ = w._lock_manager.acquire.call_args
        assert args[0] == "user-7"

    @pytest.mark.asyncio
    async def test_missing_arg_routes_to_dlq_with_render_error_header(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(FormatStringLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        # Missing user_id entirely
        msg = make_message(
            _default_payload(name="user_charge", args=[], kwargs={"amount": 100})
        )
        await w._handle_message(msg)

        w._lock_manager.acquire.assert_not_called()
        broker.dead_letter.assert_awaited_once()
        _, kwargs = broker.dead_letter.call_args
        assert "x-swarmq-lock-render-error" in kwargs["headers"]

    @pytest.mark.asyncio
    async def test_render_error_header_is_bounded(self, broker, backend, config):
        reg = TaskRegistry()
        reg.register(FormatStringLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        # Der TypeError von ``bind`` zitiert den unbekannten Argumentnamen.
        msg = make_message(
            _default_payload(name="user_charge", args=[], kwargs={"user_id": 1, "amount": 1, "k" * 1_000_000: 1})
        )
        await w._handle_message(msg)

        broker.dead_letter.assert_awaited_once()
        render_error = broker.dead_letter.call_args.kwargs["headers"]["x-swarmq-lock-render-error"]
        assert len(render_error.encode()) <= 8 * 1024

    @pytest.mark.asyncio
    async def test_render_error_with_dlq_publish_failure_nacks_no_requeue(
        self, broker, backend, config
    ):
        """Regression: lock-render DLQ path must wrap publish + ack in
        try/except, AND on broker fault must nack(requeue=False) so the
        message routes via the broker DLX to {queue}.XQ instead of
        bouncing back into the consume loop. A render error is permanent
        (deterministic output of inspect.signature.bind on this payload)
        — requeueing would just hit the same render error again, then
        the same DLQ failure, then loop forever (same shape as ADV-002
        at the conflict cap)."""
        reg = TaskRegistry()
        reg.register(FormatStringLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()
        broker.dead_letter = AsyncMock(
            side_effect=ConnectionError("broker down")
        )

        msg = make_message(
            _default_payload(name="user_charge", args=[], kwargs={"amount": 100})
        )
        await w._handle_message(msg)

        # Render failed, DLQ publish failed → nack(requeue=False) routes
        # via DLX to {queue}.XQ (not lost), no ack.
        broker.route_via_dlx.assert_awaited_once_with(msg)
        broker.ack.assert_not_awaited()


class EmptyRenderingLockTask(Task):
    task_info = TaskInfo(
        name="empty_render",
        queues=["default"],
        lock="{tag}",
        hard_timeout=60,
    )

    async def run(self, tag: str) -> str:
        return tag


class TestEmptyRenderedLock:
    """Lock template that renders to "" or whitespace must be rejected
    at acquire-time. TaskInfo blocks literal `lock=""` at construction,
    but a format-string template that renders to empty (e.g. lock="{var}"
    with var="") would slip past — without this guard, lock_key("")
    returns "sq:lock:" and silently collides with every other empty-
    rendered lock in the system."""

    @pytest.mark.asyncio
    async def test_empty_render_routes_to_dlq_with_diagnostic_header(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(EmptyRenderingLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        msg = make_message(
            _default_payload(name="empty_render", args=[], kwargs={"tag": ""})
        )
        await w._handle_message(msg)

        # Acquire never called — guard fires before LockManager
        w._lock_manager.acquire.assert_not_called()
        broker.dead_letter.assert_awaited_once()
        _, kwargs = broker.dead_letter.call_args
        assert "rendered to empty string" in kwargs["headers"][
            "x-swarmq-lock-render-error"
        ]
        # U8: retry_later/dead_letter bestätigen das Original selbst
        broker.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_whitespace_render_also_rejected(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(EmptyRenderingLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        msg = make_message(
            _default_payload(name="empty_render", args=[], kwargs={"tag": "   "})
        )
        await w._handle_message(msg)

        w._lock_manager.acquire.assert_not_called()
        broker.dead_letter.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_render_with_dlq_failure_nacks_no_requeue(
        self, broker, backend, config
    ):
        """Same hazard pattern as render-fail: empty render is permanent
        for this payload, so DLQ publish failure must route via DLX
        rather than bouncing the message back into the consume loop."""
        reg = TaskRegistry()
        reg.register(EmptyRenderingLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()
        broker.dead_letter = AsyncMock(
            side_effect=ConnectionError("broker down")
        )

        msg = make_message(
            _default_payload(name="empty_render", args=[], kwargs={"tag": ""})
        )
        await w._handle_message(msg)

        broker.route_via_dlx.assert_awaited_once_with(msg)
        broker.ack.assert_not_awaited()


# ===========================================================================
# Phase 3b.3 / Unit 6 — Worker Pre-Start Cancel Check
# ===========================================================================


class TestLockGateOutcomes:
    """Direkte Gate-Tests (Plan U7): Outcome, Registrierung, Fallbacks."""

    def _ctx(self, task_class=LockedTask, **overrides):
        base = dict(
            task_id="t1", task_name=task_class.task_info.name, queue="default",
            task_info=task_class.task_info, task_class=task_class, args=[], kwargs={}, handler_id=uuid.uuid4(),
        )
        base.update(overrides)
        return GateContext(**base)

    async def test_acquire_is_continue_and_registers_entry(self, worker):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(
            side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(
                name=name, owner=owner, max_concurrency=max_concurrency
            )
        )
        ctx = self._ctx()
        assert await worker._lock_gate.apply(make_message(_default_payload()), ctx) is Continue
        assert len(ctx.acquired_locks) == 1 and ctx.acquired_locks[0].name == "resource"
        assert worker._active_handler_locks[ctx.handler_id] == ctx.acquired_locks

    async def test_conflict_under_cap_is_redelivered_with_exponential_delay(self, worker, broker):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        msg = make_message(_default_payload(), headers={HEADER_LOCK_CONFLICTS: 2})
        assert await worker._lock_gate.apply(msg, self._ctx()) is Redelivered
        kwargs = broker.retry_later.await_args.kwargs
        assert kwargs["delay_ms"] == 400  # 100 * 2**(3-1)
        assert kwargs["headers"]["x-swarmq-lock-name"] == "resource"

    async def test_conflict_copy_drops_broker_delivery_counters(self, worker, broker):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        msg = make_message(_default_payload(), headers={"x-delivery-count": 18, "x-acquired-count": 30})
        await worker._lock_gate.apply(msg, self._ctx())
        headers = broker.retry_later.await_args.kwargs["headers"]
        assert "x-delivery-count" not in headers and "x-acquired-count" not in headers
        assert headers[HEADER_LOCK_CONFLICTS] == 1

    async def test_conflict_copy_drops_backend_defer_count(self, worker, broker):
        """Das Backend hat geantwortet (Konflikt): die ueberstandene Stoerung
        zaehlt nicht mehr."""
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        msg = make_message(_default_payload(), headers={"x-swarmq-backend-defer-count": 6})
        await worker._lock_gate.apply(msg, self._ctx())
        assert "x-swarmq-backend-defer-count" not in broker.retry_later.await_args.kwargs["headers"]

    async def test_wait_limit_dead_letters_with_diagnostics(self, worker, broker):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        msg = make_message(_default_payload(), headers=lock_wait_exceeded_headers(worker._config))
        assert await worker._lock_gate.apply(msg, self._ctx()) is Redelivered
        headers = broker.dead_letter.await_args.kwargs["headers"]
        assert headers["x-swarmq-lock-name"] == "resource"
        broker.ack.assert_not_awaited()  # dead_letter bestätigt selbst (U8)

    async def test_render_error_dead_letters_and_falls_back_to_dlx(self, worker, broker):
        worker._lock_manager = MagicMock()
        broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        msg = make_message(_default_payload())
        outcome = await worker._lock_gate.apply(msg, self._ctx(FormatStringLockTask, args=[], kwargs={}))
        assert outcome is Redelivered
        broker.route_via_dlx.assert_awaited_once_with(msg)

    def test_render_lock_name_binds_run_signature(self):
        assert render_lock_name("user-{user_id}", FormatStringLockTask, [7, 1], {}) == "user-7"
        assert render_lock_name("plain", FormatStringLockTask, [], {}) == "plain"
        with pytest.raises(ValueError, match="failed to render lock name"):
            render_lock_name("user-{missing}", FormatStringLockTask, [7, 1], {})


class TestLockGateLogging:
    """Plan 2026-10-06-0831, U9 (R17, R18): feste Event-Namen, Fehler mit
    ``error_type``/``error``."""

    def _ctx(self, task_class=LockedTask, **overrides):
        base = dict(
            task_id="t1", task_name=task_class.task_info.name, queue="default",
            task_info=task_class.task_info, task_class=task_class, args=[], kwargs={}, handler_id=uuid.uuid4(),
        )
        base.update(overrides)
        return GateContext(**base)

    @staticmethod
    def _entry(logs, event):
        [entry] = [e for e in logs if e["logger"] == "swarmq.worker.gates.lock" and e["event"] == event]
        return entry

    async def test_render_dlq_failure_logs_fixed_event_with_error_fields(self, worker, broker, swarmq_logs):
        worker._lock_manager = MagicMock()
        broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        await worker._lock_gate.apply(
            make_message(_default_payload()), self._ctx(FormatStringLockTask, args=[], kwargs={})
        )

        entry = self._entry(swarmq_logs, "lock_render_dlq_failed")
        assert entry["log_level"] == "error" and entry["lock_template"] == "user-{user_id}"
        assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "down"

    async def test_empty_render_dlq_failure_logs_its_own_fixed_event(self, worker, broker, swarmq_logs):
        worker._lock_manager = MagicMock()
        broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        await worker._lock_gate.apply(
            make_message(_default_payload()), self._ctx(EmptyRenderingLockTask, args=[], kwargs={"tag": ""})
        )

        entry = self._entry(swarmq_logs, "lock_empty_render_dlq_failed")
        assert entry["log_level"] == "error" and entry["lock_template"] == "{tag}"
        assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "down"

    async def test_render_failure_logs_error_fields(self, worker, swarmq_logs):
        worker._lock_manager = MagicMock()
        await worker._lock_gate.apply(
            make_message(_default_payload()), self._ctx(FormatStringLockTask, args=[], kwargs={})
        )

        entry = self._entry(swarmq_logs, "lock_name_render_failed")
        assert entry["log_level"] == "error" and entry["lock_template"] == "user-{user_id}"
        assert entry["error_type"] == "ValueError" and "failed to render lock name" in entry["error"]

    async def test_acquire_backend_error_logs_one_deferral_line(self, worker, swarmq_logs):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        await worker._lock_gate.apply(make_message(_default_payload()), self._ctx())

        [entry] = [e for e in swarmq_logs if e["log_level"] in ("warning", "error")]
        assert entry["event"] == "task_deferred_backend_unavailable"
        assert entry["gate"] == "lock" and entry["stage"] == "acquire" and entry["lock_name"] == "resource"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"

    @pytest.mark.timeout(5)
    async def test_hanging_acquire_is_deferred_after_backend_op_timeout(self, worker, broker, swarmq_logs):
        worker._gate_deps.config.backend_op_timeout = 0.05
        never = asyncio.Event()

        async def _hang(*_args, **_kwargs):
            await never.wait()

        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = _hang
        ctx = self._ctx()
        outcome = await worker._lock_gate.apply(make_message(_default_payload()), ctx)

        assert outcome is Redelivered
        broker.retry_later.assert_awaited_once()
        assert ctx.acquired_locks == []
        [entry] = [e for e in swarmq_logs if e["log_level"] in ("warning", "error")]
        assert entry["event"] == "task_deferred_backend_unavailable"
        assert entry["gate"] == "lock" and entry["stage"] == "acquire"
        assert entry["error_type"] == "BackendConnectionError"

    async def test_acquire_failure_logs_error_fields(self, worker, swarmq_logs):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(side_effect=RuntimeError("bug"))
        await worker._lock_gate.apply(make_message(_default_payload()), self._ctx())

        entry = self._entry(swarmq_logs, "lock_acquire_failed")
        assert entry["log_level"] == "error" and entry["lock_name"] == "resource"
        assert entry["error_type"] == "RuntimeError" and entry["error"] == "bug"

    async def test_acquire_error_reply_takes_the_old_requeue_path(self, worker, broker, swarmq_logs):
        from swarmq.exceptions import BackendResponseError

        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(side_effect=BackendResponseError("WRONGTYPE"))
        outcome = await worker._lock_gate.apply(make_message(_default_payload()), self._ctx())

        assert outcome is Redelivered
        broker.retry_later.assert_not_awaited()
        broker.redeliver.assert_awaited_once()
        entry = self._entry(swarmq_logs, "lock_acquire_failed")
        assert entry["error_type"] == "BackendResponseError" and entry["error"] == "WRONGTYPE"

    async def test_conflict_dlq_failure_at_wait_limit_logs_error_fields(self, worker, broker, swarmq_logs):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        headers = {**lock_wait_exceeded_headers(worker._config), HEADER_LOCK_CONFLICTS: 7}
        msg = make_message(_default_payload(), headers=headers)
        await worker._lock_gate.apply(msg, self._ctx())

        entry = self._entry(swarmq_logs, "lock_conflict_dlq_failed")
        assert entry["log_level"] == "error" and entry["lock_conflicts"] == 8
        assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "down"
        assert "note" in entry

    async def test_conflict_requeue_failure_logs_error_fields(self, worker, broker, swarmq_logs):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        broker.retry_later = AsyncMock(side_effect=BrokerConnectionError("down"))
        await worker._lock_gate.apply(make_message(_default_payload()), self._ctx())

        entry = self._entry(swarmq_logs, "lock_conflict_requeue_failed")
        assert entry["log_level"] == "error" and entry["lock_name"] == "resource"
        assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "down"


async def test_conflict_copy_of_a_claimed_fire_carries_the_claim_token(broker, backend, config):
    reg = TaskRegistry()
    reg.register(LockedTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)
    w._lock_manager = MagicMock()
    w._lock_manager.acquire = AsyncMock(return_value=None)
    msg = make_message(
        _default_payload(name="locked", args=[], kwargs={}), headers={"x-swarmq-schedfire": "s:1"},
    )

    await w._handle_message(msg)

    owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
    assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-schedfire-claim"] == owner


class TestLockWaitLimitTerminal:
    """BZ-05/BZ-13 (Plan 0.12.0, MR 3, MR 13): Das Ende der Wartezeit laeuft
    ueber den terminalen Dead-Letter des Workers -- Fehlerergebnis,
    ``TASK_DLQ`` und Workflow-Fehlmarkierung."""

    _WF_ID = "01890a5d-ac96-774b-bcce-b302099a8057"

    def _worker(self, broker, backend, config):
        registry = TaskRegistry()
        registry.register(LockedTask)
        dlq: list[dict] = []
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        @w._signal_dispatcher.on_signal(Signal.TASK_DLQ)
        async def _on_dlq(**kwargs):
            dlq.append(kwargs)

        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)
        w._workflow_advancer = MagicMock()
        w._workflow_advancer.fail = AsyncMock()
        return w, dlq

    async def test_wait_limit_in_a_workflow_marks_it_failed_and_stores_the_error(self, broker, backend, config):
        from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP

        w, dlq = self._worker(broker, backend, config)
        msg = make_message(
            _default_payload(task_id="cap-1", name="locked", args=[], kwargs={}),
            headers={
                **lock_wait_exceeded_headers(config),
                HEADER_WORKFLOW_ID: self._WF_ID, HEADER_WORKFLOW_STEP: "/0",
            },
        )

        await w._handle_message(msg)

        w._workflow_advancer.fail.assert_awaited_once()
        assert w._workflow_advancer.fail.await_args.args[0] == self._WF_ID
        assert [c.args[0] for c in backend.store_result.await_args_list] == ["cap-1"]
        assert [d["task_id"] for d in dlq] == ["cap-1"]
        headers = broker.dead_letter.await_args.kwargs["headers"]
        assert headers["x-swarmq-lock-name"] == "resource"
        assert "lock wait limit reached" in headers["x-swarmq-traceback"]

    async def test_wait_limit_reports_lock_wait_exceeded_error_type(self, broker, backend, config, swarmq_logs):
        w, _dlq = self._worker(broker, backend, config)
        msg = make_message(
            _default_payload(task_id="cap-2", name="locked", args=[], kwargs={}),
            headers=lock_wait_exceeded_headers(config),
        )

        await w._handle_message(msg)

        [entry] = [e for e in swarmq_logs if e["event"] == "task_sent_to_dlq"]
        assert entry["error_type"] == "LockWaitExceeded"
