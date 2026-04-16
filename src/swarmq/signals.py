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
    """Lifecycle signals emitted by SwarmQ components."""

    # Task lifecycle
    TASK_ENQUEUED = "task_enqueued"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_RETRYING = "task_retrying"
    TASK_REVOKED = "task_revoked"
    TASK_EXPIRED = "task_expired"
    TASK_LOCKED = "task_locked"
    TASK_RATE_LIMITED = "task_rate_limited"

    # Worker lifecycle
    WORKER_ONLINE = "worker_online"
    WORKER_SHUTDOWN = "worker_shutdown"
    WORKER_HEARTBEAT = "worker_heartbeat"

    # Scheduler lifecycle
    SCHEDULER_LEADER = "scheduler_leader"
    SCHEDULER_STANDBY = "scheduler_standby"


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
