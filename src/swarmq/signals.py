"""SwarmQ signals — Signal Enum and async SignalDispatcher.

Signals provide a lightweight pub/sub hook mechanism for SwarmQ lifecycle
events (task transitions, worker lifecycle, scheduler leadership changes).

Handlers registered via ``connect`` or the ``@on_signal`` decorator are
awaited sequentially on ``dispatch``. Exceptions raised by one handler are
logged at WARNING level and never abort the dispatch loop — subsequent
handlers still run.
"""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

logger = structlog.get_logger()


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

    # Worker lifecycle
    WORKER_ONLINE = "worker_online"
    WORKER_SHUTDOWN = "worker_shutdown"
    WORKER_HEARTBEAT = "worker_heartbeat"

    # Scheduler lifecycle
    SCHEDULER_LEADER = "scheduler_leader"
    SCHEDULER_STANDBY = "scheduler_standby"
    SCHEDULER_HEARTBEAT = "scheduler_heartbeat"
    SCHEDULER_LEADER_HANDOVER = "scheduler_leader_handover"
    SCHEDULER_LEADER_FAILOVER = "scheduler_leader_failover"
    SCHEDULER_FIRE_SKIPPED = "scheduler_fire_skipped"


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

    def on_signal(
        self, signal: Signal
    ) -> Callable[[AsyncHandler], AsyncHandler]:
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

    async def dispatch(self, signal: Signal, **kwargs: Any) -> None:
        """Await every handler registered for ``signal``, passing ``kwargs``.

        Handlers run sequentially in registration order. Exceptions are
        caught per-handler, logged at WARNING, and do not abort the loop.
        """
        handlers = self._handlers.get(signal, ())
        for handler in handlers:
            try:
                await handler(**kwargs)
            except Exception as exc:
                logger.warning(
                    "signal_handler_error",
                    signal=signal.value,
                    handler=getattr(handler, "__qualname__", repr(handler)),
                    error=repr(exc),
                    exc_info=exc,
                )
