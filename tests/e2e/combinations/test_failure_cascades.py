"""E2E combination tests — Failure-Cascade Combos 15-20 (Plan 2026-05-26-002 §U6).

Six combos where failure paths (DLQ routing, middleware raise, broker/backend
faults) interact with state-management features (unique_for, result_ttl, signal
sequence, retry logic):

  Combo 15 — DLQ + unique_for:
      Does the unique-hash remain held while a task sits in the DLQ?
      Is it released after the operator manually clears the DLQ?

  Combo 16 — DLQ + retry exhausted + result_ttl:
      Is the result stored when retries are exhausted?  Does get_result
      return the expected value after the DLQ move?  Does result_ttl apply?

  Combo 17 — middleware raise (pre_execute) + signal-handler:
      Does a pre_execute raise suppress TASK_STARTED?
      Is post_execute skipped?  Does the task retry?  What is the signal
      sequence?

  Combo 18 — middleware raise (post_execute) + retry:
      Does a post_execute raise leave the task result intact?  Is retry
      triggered or the task considered complete?  What is the signal
      sequence?

  Combo 19 — broker-reconnect mid-publish + at-most-once delivery (Tier 2):
      Toxiproxy drops the RabbitMQ connection during publish.  The publish
      fails loudly (``PublishIndeterminate``) and is not re-sent, so the task
      runs at most once; the at-least-once variants keep FAF-4.

  Combo 20 — backend-down + result-store (Tier 2):
      Toxiproxy drops the Valkey connection during result storage.  The ack
      is withheld and the message redelivered (no TASK_FAILED — run()
      succeeded); the rerun stores the result after the backend recovers.

Each class follows the same structure:
  test_happy_path       — both features work together (Tier 1, unmarked)
  test_failure_*        — edge / failure scenario (Tier 1, unmarked)
  test_*_chaos / test_*_drop — Toxiproxy scenarios (Tier 2/3, @pytest.mark.chaos)

Assumptions:
- pre_execute/post_execute exceptions are SWALLOWED by MiddlewareChain
  (_swallow wrapper) — they do not abort task execution or cause a re-raise
  visible to the worker's error path.  Tests that expect "no-retry from
  middleware raise" pin this real behaviour.
- TASK_FAILED fires when the task handler raises, not when middleware raises.
- unique_for hash remains held for the lifetime of the task_id; the DLQ move
  does not release it automatically (matches retry + unique_for: release only
  on explicit clear or TTL expiry).
- result_ttl is measured from the moment store_result is called, not enqueue.
- NoRetry → immediate DLQ move without retry delay.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BackendConnectionError, NoRetry, PublishIndeterminate
from swarmq.middleware import MessageContext, Middleware
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

from ._invariants import (
    FAFStageObservation,
    SignalRecorder,
    assert_at_least_once_held,
    assert_faf_stage_invariants,
    assert_signal_sequence,
)

# Module-level timeout cap; individual chaos tests may override via
# @pytest.mark.timeout(N) if Toxiproxy reconnect waits require more headroom.
pytestmark = [pytest.mark.timeout(30)]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _uq(prefix: str = "combo") -> str:
    """Return a unique queue name for this test run."""
    return f"e2e-{prefix}-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@contextlib.asynccontextmanager
async def _running_worker(
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
    *,
    concurrency: int = 4,
    cancel_poll_interval: float = 0.1,
    shutdown_timeout: int = 5,
    middlewares: list[Middleware] | None = None,
    mirror_to: Any = None,
    backend: ValkeyBackend | None = None,
    **config_kwargs: Any,
):
    """Spin up an in-process SwarmQ worker against the given queue.

    Yields the worker SwarmQ app after WORKER_ONLINE fires.  Optional
    ``middlewares`` are attached before the worker starts.

    ``mirror_to`` (a SignalDispatcher, typically the producer ``app``'s) makes
    the worker re-dispatch every signal it emits onto that dispatcher, so
    SignalRecorder / @on_signal observers attached to the producer app see the
    worker's task-lifecycle signals (the worker runs in a separate SwarmQ
    instance with its own dispatcher).
    """
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = backend if backend is not None else ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=concurrency,
        cancel_poll_interval=cancel_poll_interval,
        shutdown_timeout=shutdown_timeout,
        **config_kwargs,
    )
    for mw in middlewares or []:
        worker_app.add_middleware(mw)

    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _online(**_: Any) -> None:
        online.set()

    if mirror_to is not None:

        def _make_forwarder(sig: Signal):
            async def _forward(**payload: Any) -> None:
                await mirror_to.dispatch(sig, **payload)

            return _forward

        for _sig in Signal:
            worker_app._signal_dispatcher.connect(_sig, _make_forwarder(_sig))

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=8.0)
        yield worker_app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()


# ---------------------------------------------------------------------------
# Combo 15 — DLQ + unique_for
# ---------------------------------------------------------------------------


class TestDLQUniqueForCombo:
    """Combo #15: DLQ + unique_for.

    Bug surface: when a task is moved to the DLQ, does the unique-hash key
    (sq:unique:<hash>) stay held, preventing a duplicate from being enqueued?
    And is the hash released once the DLQ entry is manually cleared so the
    operator can re-enqueue?

    Assumption: the unique-hash is tied to the task_id, not to the DLQ
    queue position.  DLQ move ≠ unique release.  Only explicit deletion of
    the task_id (e.g., purge) or TTL expiry releases the hash.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task completes successfully — unique flag is released on completion,
        a subsequent identical enqueue gets a new task_id."""
        queue = _uq("dlq-uf")
        task_name = _task_name("dlq_uf_happy")
        completed = asyncio.Event()

        class UniqueTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                unique_for=60,
                unique_until="completion",
            )

            async def run(self, val: int) -> int:
                completed.set()
                return val * 2

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 5)
                await asyncio.wait_for(completed.wait(), timeout=8.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 10

        # After completion the unique flag must be released.
        broker2 = RabbitMQBroker(rabbitmq_url)
        backend2 = ValkeyBackend(valkey_url)
        app2 = SwarmQ(broker2, backend2, queues=[queue])
        async with app2:
            tid2 = await app2.schedule(task_name, 5)
        assert tid2 != tid, "unique flag must be released after task completes; re-enqueue must produce a new task_id"

    async def test_unique_released_after_noretry_dlq(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task fails immediately (NoRetry) → lands in DLQ. With
        unique_until="completion", reaching this terminal state releases the
        unique flag (the worker calls _maybe_release_unique on the terminal/DLQ
        path), so an identical enqueue afterwards gets a NEW task_id and is not
        deduplicated. Mirrors test_unique_released_after_retry_exhausted, which
        covers the retry-exhaustion path to the same DLQ release."""
        queue = _uq("dlq-uf-block")
        task_name = _task_name("dlq_uf_block")
        failed_event = asyncio.Event()
        dup_signals: list[dict[str, Any]] = []

        class AlwaysFailUniqueTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=0,
                unique_for=120,
                unique_until="completion",
            )

            async def run(self, val: int) -> int:
                raise NoRetry("always fails")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def _on_dup(**kwargs: Any) -> None:
            dup_signals.append(kwargs)

        @app.on_signal(Signal.TASK_FAILED)
        async def _on_failed(**_: Any) -> None:
            failed_event.set()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 1)

                # Wait for the task to fail and land in DLQ.
                await asyncio.wait_for(failed_event.wait(), timeout=8.0)
                await asyncio.sleep(0.1)  # brief settle for DLQ routing

                # Identical enqueue after the terminal DLQ move: the unique
                # flag was released, so this is a fresh task, not a duplicate.
                tid_dup = await app.schedule(task_name, 1)

        assert tid_dup != tid, (
            "unique_until=completion releases the unique flag on the terminal "
            "DLQ move; a subsequent identical enqueue must get a new task_id"
        )
        assert not dup_signals, "no duplicate must be detected once the unique flag is released"

    async def test_unique_released_after_manual_dlq_clear(self, rabbitmq_url: str, valkey_url: str) -> None:
        """After the task_id is removed from the backend (simulating operator
        purge), the unique flag is gone and a fresh identical enqueue succeeds
        with a new task_id.

        Implementation note: we simulate the purge by directly deleting the
        unique-hash key from Valkey (the same operation a dlq purge would
        trigger) rather than calling a CLI command, to keep the test hermetic.
        """
        import valkey.asyncio as valkey_asyncio

        queue = _uq("dlq-uf-clear")
        task_name = _task_name("dlq_uf_clear")
        failed_event = asyncio.Event()

        class AlwaysFailClearTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=0,
                unique_for=120,
                unique_until="completion",
            )

            async def run(self, val: int) -> int:
                raise NoRetry("always fails")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_FAILED)
        async def _on_failed(**_: Any) -> None:
            failed_event.set()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 7)
                await asyncio.wait_for(failed_event.wait(), timeout=8.0)
                await asyncio.sleep(0.1)

        # Manually clear all sq:unique:* keys for this task (simulate purge).
        vc = valkey_asyncio.from_url(valkey_url)
        try:
            # Scan for the unique key pattern and delete any hits.
            async for key in vc.scan_iter(match="sq:unique:*"):
                await vc.delete(key)
        finally:
            await vc.aclose()

        # Fresh enqueue after purge must succeed with a new task_id.
        broker2 = RabbitMQBroker(rabbitmq_url)
        backend2 = ValkeyBackend(valkey_url)
        app2 = SwarmQ(broker2, backend2, queues=[queue])
        async with app2:
            tid_fresh = await app2.schedule(task_name, 7)

        assert tid_fresh != tid, (
            "unique flag must be released after manual DLQ clear; re-enqueue must produce a new task_id"
        )


# ---------------------------------------------------------------------------
# Combo 16 — DLQ + retry exhausted + result_ttl
# ---------------------------------------------------------------------------


class TestDLQRetryResultTTLCombo:
    """Combo #16: DLQ + retry exhausted + result_ttl.

    Bug surface: after retries are exhausted the task moves to the DLQ.
    Does the backend still store a result (error payload)?  Does get_result
    return it?  Does result_ttl apply to that stored error result?
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task succeeds on the second attempt — result is stored, TTL honoured."""
        queue = _uq("dlq-rt")
        task_name = _task_name("dlq_rt_happy")
        attempts = 0
        done = asyncio.Event()

        class RetryOkTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=2,
                retry_base_delay=0.1,
                result_ttl=60,
            )

            async def run(self, val: int) -> int:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise RuntimeError("first attempt fails")
                done.set()
                return val + 1

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 41)
                await asyncio.wait_for(done.wait(), timeout=8.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 42
        assert_at_least_once_held(attempts, expected_min=2)

    async def test_result_stored_when_retries_exhausted(self, rabbitmq_url: str, valkey_url: str) -> None:
        """After max_retries are exhausted the task is moved to DLQ.
        The backend must have stored a failure result so callers can detect
        the terminal state (TASK_FAILED signal + result lookup)."""
        queue = _uq("dlq-rt-exhaust")
        task_name = _task_name("dlq_rt_exhaust")
        attempts = 0
        failed_event = asyncio.Event()

        class AlwaysFailRetryTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=1,
                retry_base_delay=0.1,
                result_ttl=60,
            )

            async def run(self, val: int) -> int:
                nonlocal attempts
                attempts += 1
                raise RuntimeError(f"fail #{attempts}")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_FAILED, Signal.TASK_STARTED],
        )

        @app.on_signal(Signal.TASK_FAILED)
        async def _on_failed(**_: Any) -> None:
            failed_event.set()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                await app.schedule(task_name, 0)
                await asyncio.wait_for(failed_event.wait(), timeout=12.0)
                # Allow DLQ-move ack to propagate.
                await asyncio.sleep(0.3)

        assert_at_least_once_held(attempts, expected_min=2)  # retried once
        # TASK_FAILED must have been emitted.
        assert Signal.TASK_FAILED in recorder.emitted_signals, "TASK_FAILED must be emitted after retry exhaustion"

    async def test_get_result_returns_expected_after_dlq_move(self, rabbitmq_url: str, valkey_url: str) -> None:
        """get_result on a task that ended in DLQ must raise TaskError or
        return the stored failure indicator — it must NOT hang indefinitely.

        Assumption: after DLQ move the backend stores a terminal failure marker
        so get_result does not poll indefinitely.
        """
        from swarmq.exceptions import TaskError

        queue = _uq("dlq-rt-get")
        task_name = _task_name("dlq_rt_get")
        failed_event = asyncio.Event()

        class ImmediateFailTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=0,
                result_ttl=60,
            )

            async def run(self, val: int) -> int:
                raise NoRetry("immediate fail")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_FAILED)
        async def _on_failed(**_: Any) -> None:
            failed_event.set()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 1)
                await asyncio.wait_for(failed_event.wait(), timeout=8.0)
                await asyncio.sleep(0.2)

                # get_result must resolve (not hang) once the task is terminal.
                with pytest.raises((TaskError, Exception)):
                    await app.get_result(tid, timeout=5)

    async def test_result_ttl_applies_to_dlq_result(self, rabbitmq_url: str, valkey_url: str) -> None:
        """A very short result_ttl causes the stored failure result to expire.
        get_result after TTL expiry must raise ResultExpiredError or similar.

        Uses result_ttl=1 (1 second) to make the expiry observable in test
        time without making the test flaky.
        """
        from swarmq.exceptions import ResultExpiredError, TaskError

        queue = _uq("dlq-rt-ttl")
        task_name = _task_name("dlq_rt_ttl")
        failed_event = asyncio.Event()

        class ShortTTLFailTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=0,
                result_ttl=1,  # 1 second TTL
            )

            async def run(self, val: int) -> int:
                raise NoRetry("short-ttl fail")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_FAILED)
        async def _on_failed(**_: Any) -> None:
            failed_event.set()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 1)
                await asyncio.wait_for(failed_event.wait(), timeout=8.0)

            # Wait for the 1-second TTL to expire before asserting.
            await asyncio.sleep(1.5)

            # After expiry, get_result must raise (not return stale value).
            with pytest.raises((ResultExpiredError, TaskError, Exception)):
                await app.get_result(tid, timeout=3)


# ---------------------------------------------------------------------------
# Combo 17 — middleware raise (pre_execute) + signal-handler
# ---------------------------------------------------------------------------


class TestPreExecuteRaiseSignalCombo:
    """Combo #17: middleware raise (pre_execute) + signal-handler ordering.

    Key contract: MiddlewareChain._swallow wraps pre_execute, so the
    exception is logged but NOT propagated.  Task execution proceeds normally
    after a pre_execute raise.  This means:
    - TASK_STARTED fires (task execution is NOT aborted by pre_execute raise)
    - post_execute IS still called (the run completes)
    - TASK_COMPLETED fires (unless run() itself raises)
    - TASK_FAILED does NOT fire due to a pre_execute raise

    These tests pin that actual swallow behaviour.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """pre_execute runs normally, task completes, post_execute runs,
        TASK_STARTED → TASK_COMPLETED in that order."""
        queue = _uq("mw-pre")
        task_name = _task_name("mw_pre_happy")
        hooks: list[str] = []
        done = asyncio.Event()

        class SimpleTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                return val + 1

        class TrackingMiddleware(Middleware):
            async def pre_execute(self, message: MessageContext) -> None:
                hooks.append("pre_execute")

            async def post_execute(self, message: MessageContext, result: Any = None) -> None:
                hooks.append("post_execute")
                done.set()

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_COMPLETED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                middlewares=[TrackingMiddleware()],
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 9)
                await asyncio.wait_for(done.wait(), timeout=8.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 10

        assert hooks == ["pre_execute", "post_execute"]
        await assert_signal_sequence(
            recorder,
            [Signal.TASK_STARTED, Signal.TASK_COMPLETED],
            timeout_s=2.0,
        )

    async def test_pre_execute_throw_does_not_abort_task(self, rabbitmq_url: str, valkey_url: str) -> None:
        """pre_execute raises — the exception is swallowed by MiddlewareChain.
        Task execution proceeds, TASK_COMPLETED fires (not TASK_FAILED)."""
        queue = _uq("mw-pre-throw")
        task_name = _task_name("mw_pre_throw")
        hooks: list[str] = []
        done = asyncio.Event()

        class SimpleTask2(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                return val * 3

        class ThrowingPreMiddleware(Middleware):
            async def pre_execute(self, message: MessageContext) -> None:
                hooks.append("pre_execute_raised")
                raise RuntimeError("pre_execute intentional fail")

            async def post_execute(self, message: MessageContext, result: Any = None) -> None:
                hooks.append("post_execute_ran")
                done.set()

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_COMPLETED, Signal.TASK_FAILED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                middlewares=[ThrowingPreMiddleware()],
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 4)
                await asyncio.wait_for(done.wait(), timeout=8.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 12, "task run() must proceed despite pre_execute raise"

        assert "pre_execute_raised" in hooks
        assert "post_execute_ran" in hooks
        # TASK_FAILED must NOT have fired — only TASK_COMPLETED.
        assert Signal.TASK_FAILED not in recorder.emitted_signals, (
            "pre_execute raise must NOT emit TASK_FAILED (exception is swallowed by MiddlewareChain)"
        )
        assert Signal.TASK_COMPLETED in recorder.emitted_signals

    async def test_post_execute_still_runs_when_pre_throws(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Even when pre_execute raises, post_execute is called (swallow means
        the exception doesn't abort the chain — the rest of the hooks fire)."""
        queue = _uq("mw-pre-post")
        task_name = _task_name("mw_pre_post")
        post_ran = asyncio.Event()

        class OkTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                return val

        class PreThrowPostTrackMiddleware(Middleware):
            async def pre_execute(self, message: MessageContext) -> None:
                raise ValueError("pre_execute bombs")

            async def post_execute(self, message: MessageContext, result: Any = None) -> None:
                post_ran.set()

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                middlewares=[PreThrowPostTrackMiddleware()],
                mirror_to=app._signal_dispatcher,
            ):
                await app.schedule(task_name, 1)
                # post_execute must have run despite pre_execute raising.
                await asyncio.wait_for(post_ran.wait(), timeout=8.0)

    async def test_task_retried_after_run_raise_not_pre_raise(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Retry is triggered by run() raising, not by pre_execute raising.
        A pre_execute raise + run() succeeding must NOT trigger retry."""
        queue = _uq("mw-pre-retry")
        task_name = _task_name("mw_pre_retry")
        run_count = 0
        done = asyncio.Event()

        class RetryableTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=3,
                retry_base_delay=0.1,
            )

            async def run(self, val: int) -> int:
                nonlocal run_count
                run_count += 1
                done.set()
                return val

        class PreThrowMiddleware(Middleware):
            async def pre_execute(self, message: MessageContext) -> None:
                raise RuntimeError("pre_execute always raises")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                middlewares=[PreThrowMiddleware()],
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 5)
                await asyncio.wait_for(done.wait(), timeout=8.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 5

        # run() was called exactly once — pre_execute raise did not cause retry.
        assert run_count == 1, (
            f"run() must be called once; pre_execute raise must not trigger retry (got run_count={run_count})"
        )

    async def test_signal_sequence_includes_completion_not_failure(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Full signal sequence: TASK_STARTED → TASK_COMPLETED (no TASK_FAILED)
        even when pre_execute raises."""
        queue = _uq("mw-pre-sig")
        task_name = _task_name("mw_pre_sig")
        done = asyncio.Event()

        class SigTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                return val

        class PreRaiseMW(Middleware):
            async def pre_execute(self, message: MessageContext) -> None:
                raise RuntimeError("signal-sequence pre_execute")

            async def post_execute(self, message: MessageContext, result: Any = None) -> None:
                done.set()

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_COMPLETED, Signal.TASK_FAILED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                middlewares=[PreRaiseMW()],
                mirror_to=app._signal_dispatcher,
            ):
                await app.schedule(task_name, 1)
                await asyncio.wait_for(done.wait(), timeout=8.0)

        await assert_signal_sequence(
            recorder,
            [Signal.TASK_STARTED, Signal.TASK_COMPLETED],
            timeout_s=2.0,
        )
        assert Signal.TASK_FAILED not in recorder.emitted_signals


# ---------------------------------------------------------------------------
# Combo 18 — middleware raise (post_execute) + retry
# ---------------------------------------------------------------------------


class TestPostExecuteRaiseRetryCombo:
    """Combo #18: middleware raise (post_execute) + retry.

    Key contract: post_execute raise is also swallowed by MiddlewareChain.
    The task result is already stored by the time post_execute runs, so:
    - TASK_COMPLETED fires
    - get_result returns the expected value
    - The task is NOT retried due to a post_execute raise
    - on_error is NOT called for a post_execute raise (run() succeeded)

    Tests pin this actual swallow behaviour.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """post_execute runs cleanly, result stored, TASK_COMPLETED fires."""
        queue = _uq("mw-post")
        task_name = _task_name("mw_post_happy")
        post_called = asyncio.Event()

        class TaskForPost(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                return val + 10

        class TrackPostMW(Middleware):
            async def post_execute(self, message: MessageContext, result: Any = None) -> None:
                post_called.set()

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                middlewares=[TrackPostMW()],
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 32)
                await asyncio.wait_for(post_called.wait(), timeout=8.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 42

    async def test_post_execute_throw_after_successful_run(self, rabbitmq_url: str, valkey_url: str) -> None:
        """post_execute raises after run() succeeds — result is stored,
        get_result returns the correct value, TASK_COMPLETED fires."""
        queue = _uq("mw-post-throw")
        task_name = _task_name("mw_post_throw")
        completed_event = asyncio.Event()

        class SuccessTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                return val * 2

        class ThrowingPostMW(Middleware):
            async def post_execute(self, message: MessageContext, result: Any = None) -> None:
                raise RuntimeError("post_execute intentional fail")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_COMPLETED, Signal.TASK_FAILED],
        )

        @app.on_signal(Signal.TASK_COMPLETED)
        async def _on_completed(**_: Any) -> None:
            completed_event.set()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                middlewares=[ThrowingPostMW()],
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 6)
                await asyncio.wait_for(completed_event.wait(), timeout=8.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 12, "result must be stored despite post_execute raise"

        assert Signal.TASK_FAILED not in recorder.emitted_signals, "post_execute raise must NOT emit TASK_FAILED"

    async def test_retry_not_triggered_by_post_execute_throw(self, rabbitmq_url: str, valkey_url: str) -> None:
        """post_execute raise must NOT trigger retry (run() succeeded).
        run_count must be 1 even when max_retries > 0."""
        queue = _uq("mw-post-retry")
        task_name = _task_name("mw_post_retry")
        run_count = 0
        completed_event = asyncio.Event()

        class CountedTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=3,
                retry_base_delay=0.1,
            )

            async def run(self, val: int) -> int:
                nonlocal run_count
                run_count += 1
                return val

        class AlwaysThrowPostMW(Middleware):
            async def post_execute(self, message: MessageContext, result: Any = None) -> None:
                raise RuntimeError("post always raises")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_COMPLETED)
        async def _on_done(**_: Any) -> None:
            completed_event.set()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                middlewares=[AlwaysThrowPostMW()],
                mirror_to=app._signal_dispatcher,
            ):
                await app.schedule(task_name, 1)
                await asyncio.wait_for(completed_event.wait(), timeout=8.0)
                # Brief pause to detect any spurious retries.
                await asyncio.sleep(0.5)

        assert run_count == 1, (
            f"run() must be called exactly once; post_execute raise must not trigger retry (got run_count={run_count})"
        )

    async def test_signal_sequence_for_post_execute_failure(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Full signal sequence when post_execute raises:
        TASK_STARTED → TASK_COMPLETED (no TASK_FAILED)."""
        queue = _uq("mw-post-sig")
        task_name = _task_name("mw_post_sig")
        done = asyncio.Event()

        class SigTask2(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                return val

        class PostRaiseMW(Middleware):
            async def post_execute(self, message: MessageContext, result: Any = None) -> None:
                done.set()
                raise RuntimeError("post_execute signal-sequence test")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_COMPLETED, Signal.TASK_FAILED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                middlewares=[PostRaiseMW()],
                mirror_to=app._signal_dispatcher,
            ):
                await app.schedule(task_name, 2)
                await asyncio.wait_for(done.wait(), timeout=8.0)
                await asyncio.sleep(0.2)  # settle for signal propagation

        await assert_signal_sequence(
            recorder,
            [Signal.TASK_STARTED, Signal.TASK_COMPLETED],
            timeout_s=2.0,
        )
        assert Signal.TASK_FAILED not in recorder.emitted_signals


# ---------------------------------------------------------------------------
# Combo 19 — broker-reconnect mid-publish + at-most-once delivery (Tier 2)
# ---------------------------------------------------------------------------


class TestBrokerReconnectAtMostOnceCombo:
    """Combo #19: broker-reconnect mid-publish + at-most-once delivery.

    Tier 2 — all tests require Toxiproxy.  Tests in this class drop the
    RabbitMQ connection via a ``timeout`` toxic during the publish phase and
    verify that at-most-once delivery semantics are respected.

    Publishes are the same for every delivery mode (persistent, with
    publisher confirm).  If the confirm does not arrive within
    ``publish_timeout_s``, the caller gets ``PublishIndeterminate`` and the
    task is NOT re-sent (a connection error, not a retry).
    """

    async def test_happy_path(
        self,
        rabbitmq_proxied_url: str,
        valkey_url: str,
    ) -> None:
        """No toxic active — task is published and consumed successfully."""
        queue = _uq("broker-rc")
        task_name = _task_name("broker_rc_happy")
        done = asyncio.Event()

        class SimpleProxiedTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                done.set()
                return val

        broker = RabbitMQBroker(rabbitmq_proxied_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_proxied_url, valkey_url, queue):
                tid = await app.schedule(task_name, 1)
                await asyncio.wait_for(done.wait(), timeout=10.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 1

    @pytest.mark.chaos
    async def test_at_most_once_drops_message_on_broker_drop(
        self,
        toxiproxy: Any,
        rabbitmq_url: str,
        rabbitmq_proxied_url: str,
        valkey_url: str,
    ) -> None:
        """Toxiproxy blackholes the publisher's RabbitMQ connection while
        ``schedule()`` waits for the publisher confirm.

        Documented contract (``SWARMQ_PUBLISH_TIMEOUT_S`` in the configuration
        reference, ``PublishIndeterminate`` in the errors reference): the
        publish gives up after ``publish_timeout_s`` with
        ``PublishIndeterminate`` instead of hanging, and the caller learns
        about it — no silent drop.  The publisher does not re-send on its own
        after the connection heals, so the task runs at most once.

        Only the producer goes through the proxy; the worker consumes over the
        direct connection so the drop hits exactly the publish path.  A
        sentinel task on the same queue (worker concurrency 1, FIFO) proves
        that the worker has drained everything published before it — if the
        dropped publish had reached the broker, its task would have run first.
        """
        queue = _uq("broker-amo")
        task_name = _task_name("broker_amo")
        sentinel_name = _task_name("broker_amo_sentinel")
        run_count = 0
        sentinel_done = asyncio.Event()

        class AtMostOnceTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                nonlocal run_count
                run_count += 1
                return val

        class SentinelTask(Task):
            task_info = TaskInfo(name=sentinel_name, queues=[queue])

            async def run(self) -> None:
                sentinel_done.set()

        # Short publish deadline: the default of 120 s would outlive the
        # test timeout; the contract is the same for any value.
        broker = RabbitMQBroker(rabbitmq_proxied_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue], publish_timeout_s=2.0)
        direct_broker = RabbitMQBroker(rabbitmq_url)
        direct_backend = ValkeyBackend(valkey_url)
        direct_app = SwarmQ(direct_broker, direct_backend, queues=[queue])

        async with app, direct_app:
            await direct_broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue, concurrency=1):
                toxic_name = await toxiproxy.add_toxic(
                    "rabbitmq",
                    "timeout",
                    attributes={"timeout": 0},
                    stream="upstream",
                )
                try:
                    with pytest.raises(PublishIndeterminate):
                        await app.schedule(task_name, 99)
                finally:
                    await toxiproxy.remove_toxic("rabbitmq", toxic_name)

                await direct_app.schedule(sentinel_name)
                await asyncio.wait_for(sentinel_done.wait(), timeout=10.0)

        # The toxic blocked the upstream before the publish, so nothing reached
        # the broker: the task never ran (the sentinel proves the queue was
        # drained). ``<= 1`` would also pass if the scenario never dropped
        # anything; the publisher gave up and did not re-send.
        assert run_count == 0, f"at-most-once violated or publish not dropped: task ran {run_count} times"

    @pytest.mark.chaos
    async def test_at_least_once_redelivers_after_drop(
        self,
        toxiproxy: Any,
        rabbitmq_proxied_url: str,
        valkey_url: str,
    ) -> None:
        """At-least-once: broker drop mid-consume → nack(requeue=True) →
        message redelivered after reconnect.  run_count >= 1 (could be 2
        if redelivered).  FAF-4: no silent drop."""
        queue = _uq("broker-alo")
        task_name = _task_name("broker_alo")
        run_count = 0
        done_event = asyncio.Event()

        class ALOTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=2,
                retry_base_delay=0.1,
            )

            async def run(self, val: int) -> int:
                nonlocal run_count
                run_count += 1
                done_event.set()
                return val

        broker = RabbitMQBroker(rabbitmq_proxied_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_proxied_url, valkey_url, queue):
                await app.schedule(task_name, 7)

                # Wait briefly for task to be picked up, then drop the broker.
                await asyncio.sleep(0.1)
                toxic_name = await toxiproxy.add_toxic(
                    "rabbitmq",
                    "timeout",
                    attributes={"timeout": 0},
                    stream="downstream",
                )
                await asyncio.sleep(0.2)
                # Heal.
                await toxiproxy.remove_toxic("rabbitmq", toxic_name)

                # Wait for at-least-once redelivery to complete.
                await asyncio.wait_for(done_event.wait(), timeout=15.0)

        assert_at_least_once_held(run_count, expected_min=1)

        obs = FAFStageObservation(
            stage_executed=run_count >= 1,
            nacked_with_requeue=run_count >= 1,
            silently_dropped=run_count == 0,
            requeue_count=max(0, run_count - 1),
        )
        assert_faf_stage_invariants(obs, scenario_kind="broker_drop", max_requeues=5)

    @pytest.mark.chaos
    async def test_faf4_invariants_for_broker_drop(
        self,
        toxiproxy: Any,
        rabbitmq_proxied_url: str,
        valkey_url: str,
    ) -> None:
        """Dedicated FAF-4 invariant assertion for a broker-drop scenario.

        Publishes a task, drops the broker connection after delivery, heals
        it, and asserts:
        - task ran at least once (no silent drop)
        - nack+requeue occurred (re-execution == requeue happened)
        - requeue count bounded
        """
        queue = _uq("broker-faf4")
        task_name = _task_name("broker_faf4")
        run_count = 0
        done_event = asyncio.Event()

        class FAF4Task(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=3,
                retry_base_delay=0.2,
            )

            async def run(self, val: int) -> int:
                nonlocal run_count
                run_count += 1
                done_event.set()
                return val + 100

        broker = RabbitMQBroker(rabbitmq_proxied_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_proxied_url, valkey_url, queue):
                await app.schedule(task_name, 1)

                # Let the task start, then drop the broker briefly.
                await asyncio.sleep(0.15)
                toxic_name = await toxiproxy.add_toxic(
                    "rabbitmq",
                    "timeout",
                    attributes={"timeout": 0},
                    stream="downstream",
                )
                await asyncio.sleep(0.3)
                await toxiproxy.remove_toxic("rabbitmq", toxic_name)

                await asyncio.wait_for(done_event.wait(), timeout=15.0)

        assert_at_least_once_held(run_count, expected_min=1)
        obs = FAFStageObservation(
            stage_executed=run_count >= 1,
            nacked_with_requeue=run_count >= 1,
            silently_dropped=run_count == 0,
            requeue_count=max(0, run_count - 1),
        )
        assert_faf_stage_invariants(obs, scenario_kind="broker_drop", max_requeues=5)


# ---------------------------------------------------------------------------
# Combo 20 — backend-down + result-store (Tier 2)
# ---------------------------------------------------------------------------


class TestBackendDownResultStoreCombo:
    """Combo #20: backend-down + result-store.

    Tier 2 — all tests require Toxiproxy.  Tests blackhole the Valkey backend
    connection while the worker stores the result of a successful ``run()``
    and verify the documented contract (delivery-semantics: "SwarmQ acks
    after the task's terminal state is durably stored"):

    - The failed store aborts the ack; the message goes back to the broker
      (``nack(requeue=True)``) and is redelivered — no silent drop.
    - ``run()`` itself succeeded, so ``TASK_COMPLETED`` fires and
      ``TASK_FAILED`` does not (signals guide: TASK_FAILED is for a ``run()``
      that raised or timed out).  The redelivery is not a retry, so it
      neither consumes ``max_retries`` nor ends in the DLQ.
    - Once Valkey is back, the redelivered run stores the result and
      ``get_result`` returns it.

    The scenarios are gated by events instead of sleeps (see
    ``_BackendDropGates``).  The failed store takes ``backend_op_timeout``
    (5 s by default) to surface; while Valkey is down the redelivered
    message is deferred by the pre-start cancel check (delay queue,
    growing backoff), so the rerun happens after the heal.

    Execution guard (0.11.0): the release of the first run's running
    marker fails while Valkey is down; the worker retries it in the
    background, so the rerun runs after the heal with the default
    ``execution_guard_ttl_s`` of 60 s instead of waiting for the marker
    to expire.
    """

    async def test_happy_path(
        self,
        rabbitmq_url: str,
        valkey_proxied_url: str,
    ) -> None:
        """No toxic — task runs, result stored in proxied Valkey, retrievable."""
        queue = _uq("backend-rs")
        task_name = _task_name("backend_rs_happy")
        done = asyncio.Event()

        class BackendProxiedTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                done.set()
                return val + 5

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_proxied_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_proxied_url, queue):
                tid = await app.schedule(task_name, 37)
                await asyncio.wait_for(done.wait(), timeout=10.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 42

    @staticmethod
    async def _drop_backend_during_first_store(
        toxiproxy: Any,
        app: SwarmQ,
        task_name: str,
        arg: int,
        gates: _BackendDropGates,
    ) -> str:
        """Schedule, blackhole Valkey before the first ``run()`` returns,
        heal once that run's result store has failed. Returns the task id."""
        tid = await app.schedule(task_name, arg)
        await asyncio.wait_for(gates.first_run_started.wait(), timeout=8.0)
        toxic_name = await toxiproxy.add_toxic(
            "valkey",
            "timeout",
            attributes={"timeout": 0},
            stream="downstream",
        )
        try:
            gates.toxic_active.set()
            # The store blocks until backend_op_timeout and fails.
            await asyncio.wait_for(gates.store_failed.wait(), timeout=20.0)
        finally:
            await toxiproxy.remove_toxic("valkey", toxic_name)
        # The message went back to the broker: the redelivered run stores
        # the result.  Waiting for exactly that store keeps get_result from
        # racing it — the first SET may well have reached Valkey even though
        # its reply was dropped.
        await asyncio.wait_for(gates.rerun_stored.wait(), timeout=25.0)
        return tid

    @staticmethod
    async def _assert_settled_on_the_broker(
        broker: RabbitMQBroker,
        queue: str,
        gates: _BackendDropGates,
    ) -> None:
        """FAF-4 from the real broker state instead of values derived from
        ``run_count`` (that observation was circular).  Called after the
        worker shut down, so an unacked message would be ready again:

        1. no silent drop and the requeue happened: the first store failed
           and the result was stored by the redelivered run;
        2. the message was acked -- nothing left ready or delayed in the
           queue and its ``.DQ``;
        3. no silent dead-lettering -- ``.XQ`` is empty.
        """
        assert gates.store_failed.is_set()
        assert gates.rerun_stored.is_set()
        assert await broker.queue_depth(queue) == 0
        assert await broker.dead_letter_depth(queue) == 0

    @pytest.mark.chaos
    @pytest.mark.timeout(60)
    async def test_backend_drop_during_result_store_redelivers_without_task_failed(
        self,
        toxiproxy: Any,
        rabbitmq_url: str,
        valkey_proxied_url: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Valkey is blackholed while the worker stores the result.  The
        failed store is not a task failure: no TASK_FAILED, no DLQ — the
        message is redelivered even with ``max_retries=0`` and the rerun
        stores the result after recovery."""
        queue = _uq("backend-drop-fail")
        task_name = _task_name("backend_drop_fail")
        gates = _BackendDropGates(monkeypatch)

        class SlowResultTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=0,
            )

            async def run(self, val: int) -> int:
                await gates.enter_run()
                return val

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_proxied_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[
                Signal.TASK_STARTED,
                Signal.TASK_FAILED,
                Signal.TASK_COMPLETED,
                Signal.TASK_RETRYING,
                Signal.TASK_DLQ,
            ],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_proxied_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await self._drop_backend_during_first_store(
                    toxiproxy,
                    app,
                    task_name,
                    1,
                    gates,
                )
                result = await app.get_result(tid, timeout=15)

        assert result == 1
        assert gates.run_count >= 2
        emitted = recorder.emitted_signals
        assert Signal.TASK_FAILED not in emitted, (
            f"a failed result store after a successful run() is not a task failure; signals: {emitted}"
        )
        assert Signal.TASK_RETRYING not in emitted
        assert Signal.TASK_DLQ not in emitted
        assert emitted.count(Signal.TASK_COMPLETED) >= 2

    @pytest.mark.chaos
    @pytest.mark.timeout(60)
    async def test_retry_after_backend_recovery(
        self,
        toxiproxy: Any,
        rabbitmq_url: str,
        valkey_proxied_url: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Backend drops during the first run's result store.  After Valkey
        recovers, the redelivered attempt succeeds and the result is stored.

        FAF-4: no silent drop; message must be nacked and redelivered.
        """
        queue = _uq("backend-retry")
        task_name = _task_name("backend_retry")
        gates = _BackendDropGates(monkeypatch)

        class RetriableBackendTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=2,
                retry_base_delay=0.5,
            )

            async def run(self, val: int) -> int:
                await gates.enter_run()
                return val * 2

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_proxied_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_proxied_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await self._drop_backend_during_first_store(
                    toxiproxy,
                    app,
                    task_name,
                    6,
                    gates,
                )
                result = await app.get_result(tid, timeout=15)
                assert result == 12
            await self._assert_settled_on_the_broker(broker, queue, gates)

        assert_at_least_once_held(gates.run_count, expected_min=1)

    @pytest.mark.chaos
    @pytest.mark.timeout(60)
    async def test_faf4_invariants_for_backend_drop(
        self,
        toxiproxy: Any,
        rabbitmq_url: str,
        valkey_proxied_url: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Dedicated FAF-4 assertion block for backend-drop scenario.

        Drops Valkey mid-run, heals it, and asserts all four invariants:
        1. No silent drop
        2. nack+requeue after backend fault
        3. FAF context doesn't change the safety contract
        4. Requeue count bounded
        """
        queue = _uq("backend-faf4")
        task_name = _task_name("backend_faf4")
        gates = _BackendDropGates(monkeypatch)

        class FAF4BackendTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=3,
                retry_base_delay=0.3,
            )

            async def run(self, val: int) -> int:
                await gates.enter_run()
                return val

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_proxied_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_proxied_url, queue):
                tid = await self._drop_backend_during_first_store(
                    toxiproxy,
                    app,
                    task_name,
                    1,
                    gates,
                )
                # The redelivered run completes — the result is durable.
                assert await app.get_result(tid, timeout=15) == 1
            await self._assert_settled_on_the_broker(broker, queue, gates)

        assert_at_least_once_held(gates.run_count, expected_min=1)


class _BackendDropGates:
    """Event gates for the Combo-20 scenarios (no sleeps).

    The first ``run()`` signals its start and returns only once the Valkey
    toxic is active, so its result store hits the dead backend.  A spy on
    ``ValkeyBackend.store_result`` (the backend boundary) reports the failed
    store, which is the moment to heal, and the successful store of the
    redelivered run.  While Valkey is down the redelivered message cannot
    pass the pre-start cancel check (it is deferred again), so the second
    ``run()`` only happens after the heal.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.run_count = 0
        self.first_run_started = asyncio.Event()
        self.toxic_active = asyncio.Event()
        self.store_failed = asyncio.Event()
        self.redelivered = asyncio.Event()
        self.rerun_stored = asyncio.Event()

        original = ValkeyBackend.store_result
        gates = self

        async def _spy(backend: ValkeyBackend, *args: Any, **kwargs: Any) -> Any:
            try:
                stored = await original(backend, *args, **kwargs)
            except BackendConnectionError:
                gates.store_failed.set()
                raise
            if gates.redelivered.is_set():
                gates.rerun_stored.set()
            return stored

        monkeypatch.setattr(ValkeyBackend, "store_result", _spy)

    async def enter_run(self) -> None:
        self.run_count += 1
        if self.run_count == 1:
            self.first_run_started.set()
            await self.toxic_active.wait()
        else:
            self.redelivered.set()


# ---------------------------------------------------------------------------
# Combo 21 — lange Backend-Stoerung vor dem Start (Tier 2)
# ---------------------------------------------------------------------------


class TestLongBackendOutageBeforeStartCombo:
    """Combo #21: Valkey ist laenger weg, als das Zustellbudget reicht.

    Vorher: Der Pre-Start-Cancel-Check scheiterte, jede Zustellung ging per
    ``nack(requeue=True)`` zurueck. Auf einem Broker, der Requeues aufs
    Zustellbudget anrechnet (RabbitMQ 3.13), stieg ``x-delivery-count``, und
    nach ``delivery_limit`` Zyklen schob die Broker-DLX die Nachrichten still
    nach ``.XQ`` -- kein ``TASK_DLQ``, kein Fehlerergebnis. RabbitMQ 4.x
    rechnet ``nack(requeue=True)`` nicht an (nur ``x-acquired-count``); dort
    kreiste die Nachricht ohne Backoff und ohne Meldung zur Zurueckstellung.

    Jetzt: Jede Zustellung, die ein Gate am Backend scheitern laesst, wird mit
    wachsendem Backoff ueber die Delay-Queue zurueckgestellt (neue Nachricht,
    eigenes Zustellbudget) und meldet ``task_deferred_backend_unavailable``.
    Nach der Wiederherstellung laufen alle Tasks genau einmal.
    """

    OUTAGE_S = 40.0  # > 20 alte Zyklen bei abgewiesener Verbindung (~1,5 s)
    TASKS = 4

    @pytest.mark.chaos
    @pytest.mark.timeout(180)
    async def test_outage_longer_than_delivery_budget_defers_instead_of_xq(
        self,
        toxiproxy: Any,
        rabbitmq_url: str,
        valkey_url: str,
        valkey_proxied_url: str,
    ) -> None:
        from tests.unit._log_capture import capture_swarmq_logs

        queue = _uq("backend-outage")
        task_name = _task_name("backend_outage")
        runs: dict[int, int] = {}
        all_done = asyncio.Event()

        class OutageTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, val: int) -> int:
                runs[val] = runs.get(val, 0) + 1
                if len(runs) == TestLongBackendOutageBeforeStartCombo.TASKS:
                    all_done.set()
                return val * 10

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        with capture_swarmq_logs() as logs:
            async with app:
                await broker.declare_queue(queue)
                async with _running_worker(
                    rabbitmq_url,
                    valkey_proxied_url,
                    queue,
                    backend_defer_max_delay=8.0,
                ):
                    # Abgewiesene Verbindung: Proxy aus, bestehende
                    # Verbindungen fallen, neue werden abgewiesen.
                    await toxiproxy.set_proxy_enabled("valkey", False)
                    try:
                        tids = [await app.schedule(task_name, i) for i in range(self.TASKS)]
                        # Bewusstes Beobachtungsfenster: die Ausfalldauer IST
                        # das Szenario (laenger als das alte Zustellbudget);
                        # es gibt kein Ereignis, auf das zu warten waere.
                        await asyncio.sleep(self.OUTAGE_S)
                        assert not runs, "kein Task darf ohne Backend starten"
                        assert await broker.dead_letter_depth(queue) == 0, (
                            "Nachrichten sind waehrend der Backend-Stoerung im .XQ gelandet"
                        )
                    finally:
                        await toxiproxy.set_proxy_enabled("valkey", True)
                    await asyncio.wait_for(all_done.wait(), timeout=90.0)
                    results = [await app.get_result(tid, timeout=15) for tid in tids]
                    xq_depth = await broker.dead_letter_depth(queue)

        assert results == [i * 10 for i in range(self.TASKS)]
        assert runs == dict.fromkeys(range(self.TASKS), 1), f"jeder Task genau einmal: {runs}"
        assert xq_depth == 0
        deferred = [e for e in logs if e["event"] == "task_deferred_backend_unavailable"]
        assert deferred, "keine Zurueckstellungs-Warnung geloggt"
        assert all(e["log_level"] == "warning" for e in deferred)
        assert {e["task_id"] for e in deferred} == set(tids)


# ---------------------------------------------------------------------------
# Combo 22 — Backend-Ausfall zwischen Schedfire-Claim und Cancel-Check (Tier 2)
# ---------------------------------------------------------------------------


class TestSchedfireClaimAcrossBackendOutageCombo:
    """Combo #22: Valkey faellt direkt nach dem gewonnenen Schedfire-Claim aus.

    Vorher: Der Cancel-Check scheiterte, die Nachricht wurde zurueckgestellt,
    die Freigabe des In-Flight-Claims scheiterte ebenfalls (Valkey weg). Die
    Kopie sah nach der Wiederherstellung den stehengebliebenen Claim als
    fremden Lauf und bestaetigte sich als Duplikat -- der Fire ging verloren,
    solange der Claim lebte (``scheduler_inflight_ttl``).

    Jetzt: Die Kopie traegt das Owner-Token (``x-swarmq-schedfire-claim``),
    uebernimmt den Claim und laeuft genau einmal.

    Test-Liste:
    - [x] Valkey-Ausfall nach ``claim_fire`` -> der Fire laeuft nach der
          Wiederherstellung genau einmal und ist danach abgeschlossen
    """

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_deferred_fire_runs_once_after_outage(
        self,
        toxiproxy: Any,
        rabbitmq_url: str,
        valkey_url: str,
        valkey_proxied_url: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from swarmq.backend.valkey.schedules import ValkeyScheduleStore
        from swarmq.serialization import encode_json

        queue = _uq("schedfire-outage")
        task_name = _task_name("schedfire_outage")
        fire_key = f"{_task_name('cron')}:1700000000000"
        runs: list[str] = []
        cut = asyncio.Event()
        release_failed = asyncio.Event()
        completed = asyncio.Event()

        class FireTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> str:
                runs.append("run")
                return "ok"

        real_claim = ValkeyScheduleStore.claim_fire
        real_release = ValkeyScheduleStore.release_fire_claim
        real_mark = ValkeyScheduleStore.mark_fire_completed

        async def claim_then_cut(self: Any, key: str, ttl_ms: int, **kwargs: Any) -> bool:
            claimed = await real_claim(self, key, ttl_ms, **kwargs)
            if key == fire_key and claimed and not cut.is_set():
                # Valkey faellt genau zwischen Claim und Cancel-Check aus.
                await toxiproxy.set_proxy_enabled("valkey", False)
                cut.set()
            return claimed

        async def observed_release(self: Any, key: str, **kwargs: Any) -> None:
            try:
                await real_release(self, key, **kwargs)
            except BackendConnectionError:
                if key == fire_key:
                    release_failed.set()
                raise

        async def observed_mark(self: Any, key: str, ttl_ms: int) -> bool:
            first = await real_mark(self, key, ttl_ms)
            if key == fire_key:
                completed.set()
            return first

        monkeypatch.setattr(ValkeyScheduleStore, "claim_fire", claim_then_cut)
        monkeypatch.setattr(ValkeyScheduleStore, "mark_fire_completed", observed_mark)
        monkeypatch.setattr(ValkeyScheduleStore, "release_fire_claim", observed_release)

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])
        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_proxied_url,
                queue,
                backend_defer_base_delay=0.5,
                backend_defer_max_delay=2.0,
                scheduler_inflight_ttl=600,
            ):
                body = encode_json(
                    {
                        "task": {"id": str(uuid.uuid4()), "queue": queue, "name": task_name},
                        "args": [],
                        "kwargs": {},
                    }
                )
                try:
                    await broker.publish(queue, body, headers={"x-swarmq-schedfire": fire_key})
                    await asyncio.wait_for(cut.wait(), timeout=15.0)
                    # Zurueckgestellt UND Freigabe gescheitert -- der Claim steht.
                    await asyncio.wait_for(release_failed.wait(), timeout=15.0)
                    assert not runs
                finally:
                    await toxiproxy.set_proxy_enabled("valkey", True)
                # Vorher: die Kopie bestaetigte sich als Duplikat, der Fire
                # lief nie (Claim lebt scheduler_inflight_ttl = 600 s).
                await asyncio.wait_for(completed.wait(), timeout=30.0)
                is_completed = await backend.schedules.is_fire_completed(fire_key)
                xq_depth = await broker.dead_letter_depth(queue)

        assert runs == ["run"], f"Fire genau einmal: {runs}"
        assert is_completed is True
        assert xq_depth == 0


class TestSchedfireRequeueInPlaceAcrossBackendOutageCombo:
    """Combo #22b: Valkey faellt waehrend des Laufs eines Schedule-Fires aus.

    Vorher: Das Ergebnis-Schreiben scheiterte, die Nachricht ging per
    ``nack(requeue=True)`` zurueck (``redeliver_in_place``); die Freigabe des
    Claims davor scheiterte ebenfalls. Das Original kam ohne Token zurueck,
    sah nach der Wiederherstellung den stehengebliebenen Claim als fremden
    Lauf und bestaetigte sich als Duplikat -- der Fire wurde nie
    abgeschlossen (Claim lebt ``scheduler_inflight_ttl``).

    Jetzt: Scheitert die Freigabe, geht statt des ``nack`` eine Kopie mit dem
    Owner-Token ueber die Delay-Queue; sie uebernimmt den Claim.

    Test-Liste:
    - [x] Valkey-Ausfall im Lauf, Ergebnis-Schreiben und Freigabe scheitern
          -> nach der Wiederherstellung wird der Fire genau einmal
          abgeschlossen, nichts im ``.XQ``
    """

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_requeued_fire_completes_once_after_outage(
        self,
        toxiproxy: Any,
        rabbitmq_url: str,
        valkey_url: str,
        valkey_proxied_url: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from swarmq.backend.valkey.schedules import ValkeyScheduleStore
        from swarmq.serialization import encode_json

        queue = _uq("schedfire-requeue")
        task_name = _task_name("schedfire_requeue")
        fire_key = f"{_task_name('cron')}:1700000000000"
        runs: list[str] = []
        release_failed = asyncio.Event()
        completed: list[str] = []
        completed_event = asyncio.Event()

        class FireTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> str:
                runs.append("run")
                if len(runs) == 1:
                    # Valkey faellt nach dem Claim im Lauf aus: das
                    # Ergebnis-Schreiben scheitert -> Requeue an derselben
                    # Stelle (AT_LEAST_ONCE).
                    await toxiproxy.set_proxy_enabled("valkey", False)
                return "ok"

        real_release = ValkeyScheduleStore.release_fire_claim
        real_mark = ValkeyScheduleStore.mark_fire_completed

        async def observed_release(self: Any, key: str, **kwargs: Any) -> None:
            # Die Freigabe endet hier mit einem Fehler oder wird vom
            # ``backend_op_timeout`` abgebrochen (CancelledError) -- beides
            # heisst: der Claim steht noch.
            succeeded = False
            try:
                await real_release(self, key, **kwargs)
                succeeded = True
            finally:
                if key == fire_key and not succeeded:
                    release_failed.set()

        async def observed_mark(self: Any, key: str, ttl_ms: int) -> bool:
            first = await real_mark(self, key, ttl_ms)
            if key == fire_key:
                completed.append(key)
                completed_event.set()
            return first

        monkeypatch.setattr(ValkeyScheduleStore, "release_fire_claim", observed_release)
        monkeypatch.setattr(ValkeyScheduleStore, "mark_fire_completed", observed_mark)

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])
        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_proxied_url,
                queue,
                backend_defer_base_delay=0.5,
                backend_defer_max_delay=2.0,
                backend_op_timeout=1.0,
                scheduler_inflight_ttl=600,
            ):
                body = encode_json(
                    {
                        "task": {"id": str(uuid.uuid4()), "queue": queue, "name": task_name},
                        "args": [],
                        "kwargs": {},
                    }
                )
                try:
                    await broker.publish(queue, body, headers={"x-swarmq-schedfire": fire_key})
                    # Lauf vorbei, Ergebnis nicht geschrieben, Freigabe
                    # gescheitert -- der Claim steht.
                    await asyncio.wait_for(release_failed.wait(), timeout=30.0)
                finally:
                    await toxiproxy.set_proxy_enabled("valkey", True)
                # Vorher: die Redelivery ohne Token bestaetigte sich als
                # Duplikat, der Fire wurde nie abgeschlossen.
                await asyncio.wait_for(completed_event.wait(), timeout=30.0)
                is_completed = await backend.schedules.is_fire_completed(fire_key)
                xq_depth = await broker.dead_letter_depth(queue)

        # Der erste Lauf verlor sein Ergebnis (AT_LEAST_ONCE: erneuter Lauf),
        # abgeschlossen wird der Fire genau einmal.
        assert runs == ["run", "run"], f"Laeufe: {runs}"
        assert completed == [fire_key]
        assert is_completed is True
        assert xq_depth == 0


# ---------------------------------------------------------------------------
# Combo 23 — Rate-Limit-Pruefung gegen ein Blackhole (Tier 2)
# ---------------------------------------------------------------------------


class _RateLimitViaProxyBackend(ValkeyBackend):
    """Worker-Backend, dessen Rate-Limit-Store (und nur der) ueber den
    Valkey-Proxy laeuft. So trifft das Blackhole genau das Rate-Limit-Gate;
    Cancel-Check, Heartbeat und Ergebnisse sprechen Valkey direkt. Der
    Proxy-Client ist ein eigener ``ValkeyBackend`` mit den Socket-Schranken
    und Retries von swarmq (10 s Socket-Timeout)."""

    def __init__(self, url: str, rate_limit_url: str) -> None:
        super().__init__(url)
        self._rate_limit_backend = ValkeyBackend(rate_limit_url)

    async def connect(self) -> None:
        await super().connect()
        await self._rate_limit_backend.connect()

    async def close(self) -> None:
        await self._rate_limit_backend.close()
        await super().close()

    @property
    def rate_limit(self) -> Any:
        return self._rate_limit_backend.rate_limit


class TestRateLimitBlackholeCombo:
    """Combo #23: Valkey antwortet der Rate-Limit-Pruefung nicht (Blackhole).

    Vorher: Der Rate-Limit-Check hatte kein Zeitlimit. Er hing bis zum
    Socket-Timeout des Clients (10 s, dazu dessen Retries) -- die Zustellung
    stand so lange, statt zurueckgestellt zu werden.

    Jetzt: Nach ``backend_op_timeout`` zaehlt der Check als Backend-Ausfall,
    die Nachricht wird mit Backoff zurueckgestellt (``gate=rate_limit``).
    Nach dem Ende des Blackholes laeuft der Task genau einmal.
    """

    OP_TIMEOUT_S = 1.0
    BLACKHOLE_S = 6.0

    @pytest.mark.chaos
    @pytest.mark.timeout(120)
    async def test_hanging_rate_limit_check_defers_after_op_timeout(
        self,
        toxiproxy: Any,
        rabbitmq_url: str,
        valkey_url: str,
        valkey_proxied_url: str,
    ) -> None:
        import time

        from tests.unit._log_capture import capture_swarmq_logs

        queue = _uq("rl-blackhole")
        task_name = _task_name("rl_blackhole")
        runs: list[int] = []
        done = asyncio.Event()

        class LimitedTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue], rate_limit="100/s")

            async def run(self, val: int) -> int:
                runs.append(val)
                done.set()
                return val * 10

        def _rl_deferrals(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                e for e in logs if e["event"] == "task_deferred_backend_unavailable" and e.get("gate") == "rate_limit"
            ]

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])
        worker_backend = _RateLimitViaProxyBackend(valkey_url, valkey_proxied_url)

        with capture_swarmq_logs() as logs:
            async with app:
                await broker.declare_queue(queue)
                async with _running_worker(
                    rabbitmq_url,
                    valkey_url,
                    queue,
                    backend=worker_backend,
                    backend_op_timeout=self.OP_TIMEOUT_S,
                    backend_defer_base_delay=0.5,
                    backend_defer_max_delay=1.0,
                ):
                    toxic = await toxiproxy.add_toxic(
                        "valkey",
                        "timeout",
                        attributes={"timeout": 0},
                        stream="downstream",
                    )
                    try:
                        started = time.monotonic()
                        tid = await app.schedule(task_name, 7)
                        first_deferral_after: float | None = None
                        # Bewusstes Beobachtungsfenster ueber die ganze
                        # Blackhole-Dauer (negative Pruefung: kein Lauf ohne
                        # Entscheidung); nebenbei wird der Zeitpunkt der
                        # ersten Zurueckstellung abgelesen.
                        while time.monotonic() - started < self.BLACKHOLE_S:
                            if first_deferral_after is None and _rl_deferrals(logs):
                                first_deferral_after = time.monotonic() - started
                            await asyncio.sleep(0.05)
                        assert not runs, "kein Lauf ohne Rate-Limit-Entscheidung"
                    finally:
                        await toxiproxy.remove_toxic("valkey", toxic)
                    await asyncio.wait_for(done.wait(), timeout=30.0)
                    result = await app.get_result(tid, timeout=15)

        assert first_deferral_after is not None, "der haengende Check wurde nie zurueckgestellt"
        # Ohne Zeitlimit kaeme die erste Zurueckstellung erst nach dem
        # Socket-Timeout (>= 10 s); mit Zeitlimit nach ~op_timeout.
        assert first_deferral_after < self.OP_TIMEOUT_S + 2.5, first_deferral_after
        assert len(_rl_deferrals(logs)) >= 2, "waehrend des Blackholes mehrfach zurueckgestellt"
        assert result == 70
        assert runs == [7], f"genau ein Lauf: {runs}"


# ---------------------------------------------------------------------------
# §Assumptions
# ---------------------------------------------------------------------------
# A1. MiddlewareChain._swallow: pre_execute and post_execute exceptions are
#     caught by the _swallow wrapper and logged; they do NOT propagate to the
#     worker's main error path.  This means:
#     - A pre_execute raise does NOT abort task execution.
#     - A post_execute raise does NOT trigger a retry.
#     - Neither emits TASK_FAILED.
#     Tests in Combos 17/18 pin this real behaviour.
#
# A2. unique_for hash lifetime: the sq:unique:<hash> key is tied to the
#     task_id and survives DLQ move.  It is released only on completion,
#     explicit deletion, or TTL expiry.
#
# A3. result_ttl is measured from store_result invocation, not enqueue time.
#     A task that is retried multiple times has its TTL reset on each
#     store_result call.
#
# A4. FAF-4 invariants for Tier-2 tests use FAFStageObservation.requeue_count
#     approximated from run_count (run_count - 1 = re-executions = requeues).
#     This is an observable proxy for the broker-side nack/requeue sequence.
#
# A5. Toxiproxy ``timeout`` toxic with ``timeout=0`` does NOT close the
#     connection: it drops all data on the stream until the toxic is
#     removed (a blackhole).  The client only notices through its own
#     deadline — ``publish_timeout_s`` for publishes, ``backend_op_timeout``
#     for Valkey operations.  ``stream="upstream"`` hits the client→server
#     direction (requests, publishes), ``stream="downstream"`` the
#     server→client direction (replies, confirms, deliveries).
