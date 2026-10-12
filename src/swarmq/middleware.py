"""SwarmQ middleware — hookable Middleware base class and MiddlewareChain."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any, NotRequired, TypedDict

from swarmq.logging import _error_fields, get_logger

logger = get_logger(__name__)


class MessageContext(TypedDict):
    """Canonical message context passed to middleware hooks.

    Client-side hooks (``pre_enqueue`` / ``post_enqueue``) do not populate
    ``retry_count`` — that field is only present in worker-side hooks
    (``pre_execute`` / ``post_execute`` / ``on_error`` / ``on_retry``).

    Note: ``TypedDict`` is a ``dict`` at runtime, so existing middleware
    using ``message['task_id']`` continues to work unchanged.
    """

    task_id: str
    task_name: str
    queue: str
    args: list[Any]
    kwargs: dict[str, Any]
    delivery_mode: str
    retry_count: NotRequired[int]
    # U11: nur bei Workflow-Schritten gesetzt. Zusaetzliche Schluessel statt
    # neuer Pflichtfelder, damit Handler mit fester Signatur weiterlaufen.
    workflow_id: NotRequired[str]
    step_path: NotRequired[str]


def workflow_step_message_context(
    *,
    task_id: str,
    task_name: str,
    queue: str,
    args: list[Any],
    kwargs: dict[str, Any],
    workflow_id: str,
    step_path: str,
) -> MessageContext:
    """``MessageContext`` fuer einen Workflow-Schritt (U11, R15/R16).

    Eine Quelle fuer beide Enqueue-Pfade — ``Client.apply`` und den
    ``WorkflowAdvancer`` im Worker. ``delivery_mode`` ist fest
    ``at_least_once``, unabhaengig von der Deklaration des Tasks: ein
    Workflow-Schritt braucht den Advance-Seam nach dem Ergebnis, den nur
    at-least-once hat.
    """
    return {
        "task_id": task_id,
        "task_name": task_name,
        "queue": queue,
        "args": list(args),
        "kwargs": kwargs,
        "delivery_mode": "at_least_once",
        "workflow_id": workflow_id,
        "step_path": step_path,
    }


async def _swallow(
    hook_name: str,
    middleware: Middleware,
    coro_factory: Callable[[], Awaitable[None]],
) -> None:
    """Run a middleware hook coroutine, logging any exception instead of raising."""
    try:
        await coro_factory()
    except Exception as exc:
        logger.error(
            "middleware_hook_failed",
            hook=hook_name,
            middleware=type(middleware).__name__,
            **_error_fields(exc),
            exc_info=exc,
        )


class Middleware:
    """Base class for SwarmQ middleware. All hooks are async no-ops by default.

    Subclasses override only the hooks they care about.
    """

    async def pre_enqueue(self, message: MessageContext) -> None:
        """Called before a message is enqueued. Raising aborts the enqueue."""

    async def post_enqueue(self, message: MessageContext) -> None:
        """Called after a message was enqueued. Errors are logged, not raised."""

    async def pre_execute(self, message: MessageContext) -> None:
        """Called before task execution. Errors are logged, not raised."""

    async def post_execute(self, message: MessageContext, result: Any = None) -> None:
        """Called after successful task execution. Errors are logged, not raised."""

    async def on_error(self, message: MessageContext, exception: BaseException) -> None:
        """Called when task execution raised. Errors are logged, not raised."""

    async def on_retry(
        self,
        message: MessageContext,
        exception: BaseException,
        retry_count: int,
        delay: float,
    ) -> None:
        """Called when a task is scheduled for retry. Errors are logged, not raised."""


class MiddlewareChain:
    """Runs a list of middlewares in order, with swallow-or-propagate semantics."""

    def __init__(self, middlewares: list[Middleware]) -> None:
        self._middlewares = list(middlewares)

    async def run_pre_enqueue(self, message: MessageContext) -> None:
        """Forward order. Exceptions propagate (prevent enqueue)."""
        for mw in self._middlewares:
            await mw.pre_enqueue(message)

    async def run_post_enqueue(self, message: MessageContext) -> None:
        """Reverse order. Errors logged, not propagated."""
        for mw in reversed(self._middlewares):
            await _swallow("post_enqueue", mw, partial(mw.post_enqueue, message))

    async def run_pre_execute(self, message: MessageContext) -> None:
        """Forward order. Errors logged, not propagated."""
        for mw in self._middlewares:
            await _swallow("pre_execute", mw, partial(mw.pre_execute, message))

    async def run_post_execute(self, message: MessageContext, result: Any = None) -> None:
        """Reverse order. Errors logged, not propagated."""
        for mw in reversed(self._middlewares):
            await _swallow("post_execute", mw, partial(mw.post_execute, message, result))

    async def run_on_error(self, message: MessageContext, exception: BaseException) -> None:
        """Reverse order. Errors logged, not propagated."""
        for mw in reversed(self._middlewares):
            await _swallow("on_error", mw, partial(mw.on_error, message, exception))

    async def run_on_retry(
        self,
        message: MessageContext,
        exception: BaseException,
        retry_count: int,
        delay: float,
    ) -> None:
        """Reverse order. Errors logged, not propagated."""
        for mw in reversed(self._middlewares):
            await _swallow(
                "on_retry",
                mw,
                partial(mw.on_retry, message, exception, retry_count, delay),
            )


def coerce_middleware_chain_factory(
    middleware_chain: MiddlewareChain | Callable[[], MiddlewareChain] | None,
) -> Callable[[], MiddlewareChain]:
    """Normalize a Client/Worker ``middleware_chain`` argument to a zero-arg factory.

    Accepts three shapes:
    - ``None`` -> returns a factory yielding an empty chain.
    - ``MiddlewareChain`` instance -> wraps it in a factory that always returns it
      (backward-compat; the chain is static).
    - zero-arg callable -> returned as-is; callers resolve the chain on every use
      so mutations to the underlying middleware list are visible.

    ``isinstance`` is deliberate over duck-typing: a callable object that
    happens to expose ``run_pre_enqueue`` would otherwise collide with the
    factory shape. If you need to plug in a custom chain implementation,
    subclass ``MiddlewareChain`` so it passes ``isinstance``.

    Raises ``TypeError`` with the received type name for anything else.
    """
    if middleware_chain is None:
        empty = MiddlewareChain([])
        return lambda: empty
    if isinstance(middleware_chain, MiddlewareChain):
        static_chain = middleware_chain
        return lambda: static_chain
    if callable(middleware_chain):
        return middleware_chain
    raise TypeError(
        f"middleware_chain must be a MiddlewareChain, a zero-arg "
        f"callable returning a MiddlewareChain, or None "
        f"(got {type(middleware_chain).__name__})"
    )
