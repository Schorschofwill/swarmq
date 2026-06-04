"""SwarmQ App — main application class."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING, Any, Literal

from swarmq.client import _MISSING, Client, _MissingType

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker
    from swarmq.workflow import Chain, Chord, Group, Signature
    from swarmq.workflow_orchestration import WorkflowHandle
from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError
from swarmq.logging import setup_logging
from swarmq.middleware import Middleware, MiddlewareChain
from swarmq.progress import ProgressInfo
from swarmq.registry import TaskRegistry
from swarmq.signals import AsyncHandler, Signal, SignalDispatcher
from swarmq.task import Task


def __getattr__(name: str) -> object:
    """PEP-562 lazy resolver — keeps ``Worker`` patchable as
    ``swarmq.app.Worker`` without eagerly importing it (and the heavy
    ``aio_pika`` chain it pulls in). Tests that patch
    ``swarmq.app.Worker`` continue to work; the first access from
    ``start_worker`` resolves the canonical class.
    """
    if name == "Worker":
        import importlib

        return importlib.import_module("swarmq.worker").Worker
    raise AttributeError(f"module 'swarmq.app' has no attribute {name!r}")



class SwarmQ:
    """Main SwarmQ application entry point."""

    def __init__(
        self,
        broker: RabbitMQBroker,
        backend: ValkeyBackend,
        **config_kwargs: Any,
    ) -> None:
        self.broker = broker
        self.backend = backend
        self.config = SwarmQConfig(
            broker_url=broker._url,
            backend_url=backend._url,
            **config_kwargs,
        )
        self.broker._queue_type = self.config.queue_type
        self.registry = TaskRegistry()
        for task_class in Task._registered_tasks:
            self.registry.register(task_class)
        self._log_listener = setup_logging(self.config.log_level, self.config.log_json)
        self._middlewares: list[Middleware] = []
        self._signal_dispatcher = SignalDispatcher()
        self.client = Client(
            broker,
            backend,
            registry=self.registry,
            # Pass a factory so add_middleware() calls made AFTER SwarmQ
            # construction are visible on the next schedule(). Otherwise we
            # would capture a stale snapshot of self._middlewares here.
            middleware_chain=lambda: self.middleware_chain,
            signal_dispatcher=self._signal_dispatcher,
            # R3.5 REL-001: bound every setnx_unique on the enqueue path.
            backend_op_timeout=self.config.backend_op_timeout,
        )

    def add_middleware(self, middleware: Middleware) -> None:
        """Register a middleware to run for every task lifecycle event.

        Safe to call at any time, including AFTER ``SwarmQ()`` construction
        and AFTER ``start_worker()``. Both the Client and the Worker resolve
        the middleware chain per operation (schedule / message), so newly
        registered middleware is visible from the next call onward.
        """
        self._middlewares.append(middleware)

    def on_signal(
        self, signal: Signal
    ) -> Callable[[AsyncHandler], AsyncHandler]:
        """Return a decorator that registers a handler for ``signal``."""
        return self._signal_dispatcher.on_signal(signal)

    @property
    def middleware_chain(self) -> MiddlewareChain:
        """Build a fresh :class:`MiddlewareChain` from currently registered middlewares."""
        return MiddlewareChain(self._middlewares)

    async def __aenter__(self) -> SwarmQ:
        await self.broker.connect()
        try:
            await self.backend.connect()
        except BaseException:
            await self.broker.close()
            raise
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # Härtung R2 P1-4: staged try/finally. broker.close() may raise
        # by design on the injected path (channel.close() errors are NOT
        # swallowed there); without the finally-chain that left the
        # Valkey client open and the log-listener thread running. Every
        # teardown step always runs; when several raise, Python's
        # finally semantics propagate the LAST one (earlier exceptions
        # are visible via __context__).
        try:
            await self.broker.close()
        finally:
            try:
                await self.backend.close()
            finally:
                self._log_listener.stop()

    async def schedule_many(
        self,
        tasks: list[tuple[str, tuple, dict]],
        queue: str | None = None,
        *,
        unique_for: int | None | _MissingType = _MISSING,
        unique_until: Literal["start", "completion"] | _MissingType = _MISSING,
    ) -> list[str]:
        """Forward to :meth:`Client.schedule_many` including R3.5 dedup kwargs."""
        return await self.client.schedule_many(
            tasks, queue=queue,
            unique_for=unique_for,
            unique_until=unique_until,
        )

    async def get_results(
        self,
        task_ids: list[str],
        timeout: float = 10,
        poll_interval: float = 0.005,
    ) -> list[Any]:
        return await self.client.get_results(
            task_ids, timeout=timeout, poll_interval=poll_interval,
        )

    async def schedule(self, task_name: str, *args: Any, **kwargs: Any) -> str:
        return await self.client.schedule(task_name, *args, **kwargs)

    async def get_result(
        self, task_id: str, timeout: float = 10, polling: float = 0.2
    ) -> Any:
        return await self.client.get_result(task_id, timeout=timeout, polling=polling)

    async def run(
        self,
        task_name: str,
        *args: Any,
        timeout: float = 10,
        polling: float = 0.2,
        **kwargs: Any,
    ) -> Any:
        return await self.client.run(
            task_name, *args, timeout=timeout, polling=polling, **kwargs
        )

    async def get_progress(self, task_id: str) -> ProgressInfo | None:
        """Forward to :meth:`Client.get_progress`."""
        return await self.client.get_progress(task_id)

    def watch_progress(self, task_id: str) -> AsyncIterator[ProgressInfo]:
        """Forward to :meth:`Client.watch_progress`.

        Not ``async def`` — returns the async generator directly so
        callers can ``async for info in app.watch_progress(tid): ...``
        without double-await.
        """
        return self.client.watch_progress(task_id)

    async def cancel(self, task_id: str) -> bool:
        """Request cancellation of *task_id*.

        Fire-and-forget delegation to :meth:`Client.cancel`. Computes the
        flag's TTL from ``config.result_ttl`` so the cancel flag outlives
        a typical retry window (default: 24 hours).

        Returns True when the flag was newly set, False when it already
        existed.
        """
        ttl_ms = self.config.result_ttl * 1000
        return await self.client.cancel(task_id, ttl_ms=ttl_ms)

    async def apply(
        self, workflow: Signature | Chain | Group | Chord
    ) -> WorkflowHandle:
        """Start a workflow (chain/group/chord) fire-and-forget; return a
        reattachable :class:`WorkflowHandle` (R10). Workflow state TTL is
        derived from ``config.result_ttl`` so it outlives the retry horizon."""
        return await self.client.apply(
            workflow, state_ttl_ms=self.config.result_ttl * 1000
        )

    async def workflow(self, workflow_id: str) -> WorkflowHandle:
        """Reattach to a workflow by id (R14); raises WorkflowNotFound when
        absent."""
        return await self.client.workflow(workflow_id)

    async def start_worker(self) -> None:
        # Injected connection objects are producer-only. Worker subprocesses
        # are spawned (process_manager) and reconstruct the broker/backend
        # from SWARMQ_BROKER_URL / SWARMQ_BACKEND_URL — a borrowed connection
        # cannot cross the spawn boundary (it is not picklable). Reject it
        # loudly here, before any subprocess is created. getattr defaults to
        # True so URL-constructed brokers/backends (and spec'd test mocks
        # without the flag) are unaffected.
        injected = []
        if not getattr(self.broker, "_owns_connection", True):
            injected.append("broker")
        if not getattr(self.backend, "_owns_client", True):
            injected.append("backend")
        if injected:
            raise ConfigurationError(
                f"start_worker() cannot use an injected {' and '.join(injected)}: "
                "injected connection objects are for the producer/client path "
                "only. Worker subprocesses are spawned and reconstruct the "
                "broker/backend from SWARMQ_BROKER_URL / SWARMQ_BACKEND_URL, so "
                "a borrowed connection cannot cross the spawn boundary. "
                "Construct the worker's broker/backend from a URL instead."
            )

        # Resolve Worker through the module-level __getattr__ so test
        # patches like ``mock.patch("swarmq.app.Worker")`` continue to
        # intercept the construction. A direct lazy-local import would
        # bind to swarmq.worker.Worker and bypass the facade patch.
        from swarmq import app as _app_module

        worker = _app_module.Worker(
            self.broker,
            self.backend,
            self.registry,
            self.config,
            # Pass a factory so add_middleware() calls made AFTER SwarmQ
            # construction are visible on every Worker message. Mirrors the
            # Client construction path in __init__.
            middleware_chain=lambda: self.middleware_chain,
            signal_dispatcher=self._signal_dispatcher,
        )
        await worker.start()
