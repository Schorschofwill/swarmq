"""SwarmQ signals — Signal Enum and async SignalDispatcher.

Signals provide a lightweight pub/sub hook mechanism for SwarmQ lifecycle
events (task transitions, worker lifecycle, scheduler leadership changes).

Handlers registered via ``connect`` or the ``@on_signal`` decorator are
awaited sequentially on ``dispatch``. Exceptions raised by one handler are
logged at WARNING level and never abort the dispatch loop — subsequent
handlers still run.
"""

from __future__ import annotations

import asyncio
import enum
from collections.abc import Awaitable, Callable
from typing import Any

from swarmq.logging import _error_fields, get_logger

logger = get_logger(__name__)


AsyncHandler = Callable[..., Awaitable[Any]]


class Signal(enum.Enum):
    """Lifecycle signals emitted by SwarmQ components.

    Handler payloads (kwargs) per signal:
      * TASK_ENQUEUED:    task_id, task_name, queue
      * TASK_STARTED:     task_id, task_name
      * TASK_COMPLETED:   task_id, task_name, duration_ms
      * TASK_FAILED:      task_id, task_name, exception
      * TASK_RETRYING:    task_id, retry_count, delay
      * TASK_CANCELLED:   task_id, task_name
      * TASK_RATE_LIMITED: task_id, task_name, retry_after_ms,
                          remaining, rate_limit_count
        - retry_after_ms: ms the message will wait in the delay queue
        - remaining: estimated requests still allowed in the window
        - rate_limit_count: number of times this exact message has been
          requeued by the rate limiter (operator-visible bounce counter)
      * TASK_DUPLICATE_DETECTED: existing_task_id, task_name, dedup_hash,
                                 source [, batch_index]
        - existing_task_id: id of the previously-enqueued task that won
          the SETNX race; caller may poll its result with client.get_result
        - dedup_hash: sha256 hex of canonical_encode(args, kwargs)
        - source: one of
          "client.schedule" | "client.schedule_deferred" |
          "client.schedule_many" | "scheduler.fire"
          Operators can filter Scheduler-induced dup noise (leader
          failover) via source="scheduler.fire" and differentiate
          immediate vs deferred client enqueues via the two
          "client.schedule*" variants.
        - batch_index: optional int, ONLY present when
          source="client.schedule_many" — the position in the input
          list at which the duplicate was detected. Handlers MUST use
          kwargs.get("batch_index") (not kwargs["batch_index"]) to avoid
          KeyError on non-batch sources.
      * TASK_REJECTED:     queue, reason, task_id, task_name, error
        - Emitted for every message the worker hands to the broker's DLX
          without diagnostic headers (the ``route_via_dlx`` path): a body
          over the size cap, an undecodable body, a broken middleware-chain
          factory, or a failed dead-letter publish -- and, after the grace
          period ``unknown_task_grace_s`` ran out, for an unregistered task
          (that one is dead-lettered by the worker with an error result,
          so TASK_DLQ fires first; Plan 0.12.0, MR 11, BZ-06).
        - reason is one of "body_too_large" | "undecodable" |
          "unregistered_task" | "middleware_chain_factory" |
          "dlq_publish_failed".
        - task_id / task_name are None when the body never decoded.
        - Distinct from TASK_DLQ, which promises an error result and a task
          identity — on this path both are often absent. TASK_DLQ keeps
          counting only itself in ``swarmq_tasks_total{outcome="dlq"}``.
      * WORKFLOW_STEP_ENQUEUED: task_id, task_name, queue, workflow_id,
                                step_path
        - Emitted after each workflow step the advancer (or ``apply``)
          publishes, in addition to TASK_ENQUEUED. Carries the workflow
          identity that TASK_ENQUEUED cannot grow without breaking
          handlers with a fixed signature (U11, R16).
      * TASK_UNKNOWN_DEFERRED: task_id, task_name, queue, delay,
                               unknown_since
        - The worker does not register the task and defers the message
          (copy via the delay queue) within ``unknown_task_grace_s``,
          typically during a rolling deploy with producers ahead of the
          workers (Plan 0.12.0, MR 11, BZ-06). ``delay`` in seconds,
          ``unknown_since`` the first contact in Unix milliseconds. Once
          per deferral; counted in
          ``swarmq_tasks_unknown_deferred_total{queue}``.
      * TASK_DLQ:          task_id, task_name, queue, error
        - Emitted when a message is terminally dead-lettered (retry
          exhaustion / non-retryable error). Distinct from TASK_FAILED,
          which fires on every failing attempt; TASK_DLQ fires once, at
          the terminal hand-off to the .XQ queue, so operators can alert
          specifically on "work is being dead-lettered".
      * WORKER_ONLINE / WORKER_SHUTDOWN: worker_id
      * WORKER_HEARTBEAT:          worker_id, uptime_seconds (periodic
                                   liveness tick emitted by the worker's
                                   metrics sampler)
      * WORKER_RECYCLING:          worker_id, kind ("compute_child" |
                                   "worker"), slot (int | None), reason
                                   ("rss" | "tasks" | "forced"), value
                                   (measured MB or task count) — one event
                                   per recycling, Plan 2026-09-03-001 R7/R23
      * SCHEDULER_LEADER:          worker_id (this worker just became leader,
                                   either first-time or via clean handover)
      * SCHEDULER_STANDBY:         worker_id (this worker did not acquire
                                   the leader lock)
      * SCHEDULER_HEARTBEAT:       worker_id (leader successfully renewed
                                   its leader-lock)
      * SCHEDULER_LEADER_HANDOVER: worker_id (this leader is about to
                                   release its lock as part of graceful
                                   shutdown)
      * SCHEDULER_LEADER_FAILOVER: worker_id (this worker just acquired
                                   the leader lock after the previous
                                   leader's TTL expired, i.e. crash/GC)
      * SCHEDULER_FIRE_SKIPPED:    worker_id, schedule_name, skipped_count
                                   (catch-up after downtime skipped this
                                   many cron occurrences — exactly one
                                   catch-up fire runs, the rest never
                                   will; alert on important crons)
      * SCHEDULER_CRON_CONFLICT:   worker_id, schedule_name, fingerprints
                                   (several definitions of one static cron
                                   have been live for more than 10 minutes,
                                   e.g. same-named tasks in two apps; the
                                   leader emits it on every selection tick
                                   while the conflict lasts)
      * SCHEDULER_LEADER_OUTDATED: worker_id, leader, leader_version,
                                   outdated (a non-leader saw the scheduler
                                   leader run a swarmq before the minimum
                                   leader version, 0.12 — leader_version
                                   None means before 0.10.0; once per leader
                                   change with outdated=True, and with
                                   outdated=False when a current leader
                                   follows)
      * SCHEDULER_FIRE_UNROUTABLE: worker_id, schedule_name, queue, kind
                                   (the leader's publish was returned: no
                                   queue with this name, BZ-03. kind
                                   "cron_lost": this cron fire is lost and
                                   committed; "oneshot_dropped": a delayed
                                   task stayed unroutable longer than
                                   scheduler_unroutable_oneshot_max_s and
                                   was dropped with an error result)
      * SCHEDULER_CRON_UNOWNED:    worker_id, schedule_name, task_name, held,
                                   held_count (the leader starts holding a
                                   static cron that no live worker reports
                                   and that it does not register itself,
                                   held=True, or releases it again,
                                   held=False; once per transition, BZ-11;
                                   held_count = crons held afterwards)
      * SCHEDULER_VALKEY_CHECKED:  worker_id, status, policy, maxmemory,
                                   used_memory (the leader checked Valkey's
                                   eviction policy and memory again, at most
                                   once per hour, BZ-04; status "safe",
                                   "unsafe" or "unknown"; maxmemory 0 means
                                   no limit, None unknown)
      * SCHEDULER_QUEUE_ORPHANED:  worker_id, queue, orphaned, message_count,
                                   orphaned_count (the leader found a reported
                                   queue with messages, no consumer and no
                                   live worker reporting it, orphaned=True, or
                                   the queue is no longer orphaned,
                                   orphaned=False; once per transition, BZ-14;
                                   orphaned_count = orphaned queues afterwards)

    Handlers are invoked async; exceptions are swallowed and logged.
    """

    # Task lifecycle
    TASK_ENQUEUED = "task_enqueued"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_RETRYING = "task_retrying"
    TASK_CANCELLED = "task_cancelled"
    TASK_EXPIRED = "task_expired"
    TASK_LOCKED = "task_locked"
    TASK_RATE_LIMITED = "task_rate_limited"
    TASK_DUPLICATE_DETECTED = "task_duplicate_detected"
    TASK_DLQ = "task_dlq"
    TASK_REJECTED = "task_rejected"
    TASK_UNKNOWN_DEFERRED = "task_unknown_deferred"

    # Workflow lifecycle
    WORKFLOW_STEP_ENQUEUED = "workflow_step_enqueued"

    # Worker lifecycle
    WORKER_ONLINE = "worker_online"
    WORKER_SHUTDOWN = "worker_shutdown"
    WORKER_HEARTBEAT = "worker_heartbeat"
    WORKER_RECYCLING = "worker_recycling"

    # Scheduler lifecycle
    SCHEDULER_LEADER = "scheduler_leader"
    SCHEDULER_STANDBY = "scheduler_standby"
    SCHEDULER_HEARTBEAT = "scheduler_heartbeat"
    SCHEDULER_LEADER_HANDOVER = "scheduler_leader_handover"
    SCHEDULER_LEADER_FAILOVER = "scheduler_leader_failover"
    SCHEDULER_FIRE_SKIPPED = "scheduler_fire_skipped"
    SCHEDULER_CRON_CONFLICT = "scheduler_cron_conflict"
    SCHEDULER_LEADER_OUTDATED = "scheduler_leader_outdated"
    SCHEDULER_FIRE_UNROUTABLE = "scheduler_fire_unroutable"
    SCHEDULER_CRON_UNOWNED = "scheduler_cron_unowned"
    SCHEDULER_VALKEY_CHECKED = "scheduler_valkey_checked"
    SCHEDULER_QUEUE_ORPHANED = "scheduler_queue_orphaned"


class SignalDispatcher:
    """Async in-process dispatcher for :class:`Signal` events.

    Handlers are stored per-signal in registration order and awaited
    sequentially. A handler raising an exception is logged and isolated
    from other handlers.
    """

    def __init__(self) -> None:
        self._handlers: dict[Signal, list[AsyncHandler]] = {}

    def connect(self, signal: Signal, handler: AsyncHandler) -> None:
        """Register ``handler`` to be called when ``signal`` is dispatched."""
        self._handlers.setdefault(signal, []).append(handler)

    def on_signal(self, signal: Signal) -> Callable[[AsyncHandler], AsyncHandler]:
        """Decorator form of :meth:`connect`.

        Example::

            @dispatcher.on_signal(Signal.TASK_COMPLETED)
            async def handler(task_id, duration_ms):
                ...
        """

        def decorator(handler: AsyncHandler) -> AsyncHandler:
            self.connect(signal, handler)
            return handler

        return decorator

    async def dispatch(
        self,
        signal: Signal,
        *,
        handler_timeout: float | None = None,
        **kwargs: Any,
    ) -> None:
        """Await every handler registered for ``signal``, passing ``kwargs``.

        Handlers run sequentially in registration order. Exceptions are
        caught per-handler, logged at WARNING, and do not abort the loop.

        Isolation boundary is ``Exception``, deliberately NOT
        ``BaseException``: an ``asyncio.CancelledError`` reaching a
        handler propagates out of ``dispatch`` so shutdown cancellation
        is never swallowed.

        ``handler_timeout`` bounds each handler
        individually — a *blocking* (non-raising) handler otherwise
        stalls the whole dispatch. The worker passes this for lifecycle
        signals (WORKER_ONLINE / WORKER_SHUTDOWN / WORKER_HEARTBEAT),
        where a hanging user handler previously wedged startup or the
        entire graceful-shutdown sequence until the orchestrator's
        SIGKILL. ``handler_timeout`` is reserved — it is consumed here
        and never forwarded to handlers.
        """
        handlers = self._handlers.get(signal, ())
        for handler in handlers:
            try:
                if handler_timeout is not None:
                    await asyncio.wait_for(
                        handler(**kwargs),
                        timeout=handler_timeout,
                    )
                else:
                    await handler(**kwargs)
            except TimeoutError:
                logger.warning(
                    "signal_handler_timeout",
                    signal=signal.value,
                    handler=getattr(handler, "__qualname__", repr(handler)),
                    timeout_s=handler_timeout,
                )
            except Exception as exc:
                logger.warning(
                    "signal_handler_error",
                    signal=signal.value,
                    handler=getattr(handler, "__qualname__", repr(handler)),
                    **_error_fields(exc),
                    exc_info=exc,
                )
