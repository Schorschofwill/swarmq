"""SwarmQ App — main application class."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import _MISSING, Client
from swarmq.config import SwarmQConfig
from swarmq.logging import setup_logging
from swarmq.middleware import Middleware, MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.signals import AsyncHandler, Signal, SignalDispatcher
from swarmq.task import Task
from swarmq.worker import Worker


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
        await self.broker.close()
        await self.backend.close()
        self._log_listener.stop()

    async def schedule_many(
        self,
        tasks: list[tuple[str, tuple, dict]],
        queue: str | None = None,
        *,
        unique_for: int | None | Any = _MISSING,
        unique_until: Literal["start", "completion"] | Any = _MISSING,
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

    async def start_worker(self) -> None:
        worker = Worker(
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
