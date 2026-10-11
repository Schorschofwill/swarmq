"""SwarmQ adapter for the benchmark comparison suite.

Implements the ``TaskQueueSystem`` protocol defined in ``base.py``.

Architecture
------------
Two separate ``SwarmQ`` application instances are used:

* **client_app** — owns a ``RabbitMQBroker`` + ``ValkeyBackend`` pair used
  exclusively for ``schedule()`` and ``get_result()`` calls.
* **worker_app** — owns its own broker/backend pair and runs the consumer
  loop as an ``asyncio.Task`` via ``SwarmQ.start_worker()``.

This mirrors the pattern established in ``tests/e2e/test_roundtrip.py`` and
avoids sharing an aio-pika channel between producer and consumer, which can
cause deadlocks under load.

Signal handler isolation
~~~~~~~~~~~~~~~~~~~~~~~~
``Worker.start()`` installs SIGTERM/SIGINT handlers on the running event
loop.  To avoid clobbering the benchmark harness's own handlers we
save/restore the previous handlers around the worker's lifetime.

Task registry isolation
~~~~~~~~~~~~~~~~~~~~~~~
``Task.__init_subclass__`` appends every new subclass to the global
``Task._registered_tasks`` list.  We snapshot the registry before creating
the benchmark task classes and restore it in ``teardown()``.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import uuid
from typing import Any

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task

from workers.swarmq_worker import create_task_classes

# Default connection URLs for the benchmark environment.
_DEFAULT_BROKER_URL = "amqp://guest:guest@localhost:5682/"
_DEFAULT_BACKEND_URL = "valkey://localhost:6389/0"


class SwarmQSystem:
    """``TaskQueueSystem`` adapter for SwarmQ."""

    name: str = "swarmq"

    def __init__(
        self,
        broker_url: str = _DEFAULT_BROKER_URL,
        backend_url: str = _DEFAULT_BACKEND_URL,
        queue_type: str = "quorum",
    ) -> None:
        self._broker_url = broker_url
        self._backend_url = backend_url
        self._queue_type = queue_type

        # Unique queue name per adapter instance avoids RabbitMQ collisions.
        self._queue = f"bench-{uuid.uuid4().hex[:8]}"

        # Populated during setup().
        self._client_broker: RabbitMQBroker | None = None
        self._client_backend: ValkeyBackend | None = None
        self._client_app: SwarmQ | None = None

        self._worker_broker: RabbitMQBroker | None = None
        self._worker_backend: ValkeyBackend | None = None
        self._worker_app: SwarmQ | None = None
        self._worker_task: asyncio.Task[None] | None = None

        # Registry snapshot taken before task classes are created.
        self._registry_snapshot: list[type] | None = None

        # Signal handler snapshots (SIGTERM, SIGINT) saved before worker start.
        self._prev_sigterm: signal.Handlers | None = None
        self._prev_sigint: signal.Handlers | None = None

        # Task class mapping returned by the factory.
        self._task_classes: dict[str, type[Task]] | None = None

    # ------------------------------------------------------------------
    # TaskQueueSystem protocol
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Connect to broker/backend, register task classes, declare queues."""

        # 1. Snapshot the global task registry so we can restore it later.
        self._registry_snapshot = Task._registered_tasks.copy()

        # 2. Create benchmark task classes bound to our unique queue.
        self._task_classes = create_task_classes(self._queue)

        # 3. Build client-side app (schedule + get_result).
        self._client_broker = RabbitMQBroker(self._broker_url, queue_type=self._queue_type)
        self._client_backend = ValkeyBackend(self._backend_url)
        self._client_app = SwarmQ(
            self._client_broker,
            self._client_backend,
            queues=[self._queue],
        )
        await self._client_broker.connect()
        await self._client_backend.connect()

        # 4. Declare the queue so the worker can immediately consume.
        await self._client_broker.declare_queue(self._queue)

    async def teardown(self) -> None:
        """Stop worker, close connections, restore task registry."""

        # Stop worker first (if running).
        await self.stop_worker()

        # Close client connections.
        if self._client_broker is not None:
            await self._client_broker.close()
            self._client_broker = None
        if self._client_backend is not None:
            await self._client_backend.close()
            self._client_backend = None
        self._client_app = None

        # Restore the global task registry to its pre-setup state.
        if self._registry_snapshot is not None:
            Task._registered_tasks = self._registry_snapshot
            self._registry_snapshot = None

        self._task_classes = None

    async def start_worker(self) -> None:
        """Start the SwarmQ worker as an in-process ``asyncio.Task``."""
        if self._worker_task is not None:
            return  # Already running.

        self._worker_broker = RabbitMQBroker(self._broker_url, queue_type=self._queue_type)
        self._worker_backend = ValkeyBackend(self._backend_url)
        self._worker_app = SwarmQ(
            self._worker_broker,
            self._worker_backend,
            queues=[self._queue],
        )

        # Save current signal handlers — Worker.start() will overwrite them.
        loop = asyncio.get_running_loop()
        self._prev_sigterm = signal.getsignal(signal.SIGTERM)
        self._prev_sigint = signal.getsignal(signal.SIGINT)

        self._worker_task = asyncio.create_task(self._worker_app.start_worker())

        # Give the worker time to connect and start consuming.
        await asyncio.sleep(0.5)

        # Restore the previous signal handlers so the benchmark harness keeps
        # control of SIGTERM/SIGINT.
        _restore_signal_handler(loop, signal.SIGTERM, self._prev_sigterm)
        _restore_signal_handler(loop, signal.SIGINT, self._prev_sigint)

    async def stop_worker(self) -> None:
        """Cancel the worker task and close its connections."""
        if self._worker_task is None:
            return

        self._worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._worker_task
        self._worker_task = None

        if self._worker_broker is not None:
            await self._worker_broker.close()
            self._worker_broker = None
        if self._worker_backend is not None:
            await self._worker_backend.close()
            self._worker_backend = None
        self._worker_app = None

    async def enqueue(self, task_name: str, payload: dict) -> str:
        """Schedule a task and return its id.

        The benchmark scenarios pass a flat ``payload`` dict.  SwarmQ's
        ``schedule()`` accepts ``*args, **kwargs``, so we forward the
        payload as keyword arguments.
        """
        if self._client_app is None:
            raise RuntimeError("SwarmQSystem.setup() has not been called")
        return await self._client_app.schedule(task_name, **payload)

    async def get_result(
        self,
        task_id: str,
        timeout: float = 30.0,
        poll_interval: float = 0.01,
    ) -> Any:
        """Poll for a task result."""
        if self._client_app is None:
            raise RuntimeError("SwarmQSystem.setup() has not been called")
        return await self._client_app.get_result(task_id, timeout=timeout, polling=poll_interval)


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _restore_signal_handler(
    loop: asyncio.AbstractEventLoop,
    sig: signal.Signals,
    prev_handler: signal.Handlers | None,
) -> None:
    """Restore a signal handler that was saved before ``Worker.start()``.

    ``loop.add_signal_handler`` only works for callables — if the previous
    handler was ``SIG_DFL`` or ``SIG_IGN`` we fall back to
    ``signal.signal()`` which accepts those sentinels.
    """
    if prev_handler is None:
        return
    if callable(prev_handler) and prev_handler not in (
        signal.SIG_DFL,
        signal.SIG_IGN,
    ):
        loop.add_signal_handler(sig, prev_handler)
    else:
        # SIG_DFL / SIG_IGN — remove the loop handler and set via signal module.
        loop.remove_signal_handler(sig)
        signal.signal(sig, prev_handler)
