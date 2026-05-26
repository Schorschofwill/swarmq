"""``swarmq worker`` async entry point.

Kept as a standalone submodule because ``swarmq.process_manager``
imports it by name (``from swarmq.cli import run_worker_async``) and
tests monkeypatch ``swarmq.cli.run_worker_async``. The facade re-
exports this symbol so both import paths stay valid.
"""

from __future__ import annotations

import argparse
import importlib
import os

from swarmq.exceptions import ConfigurationError
from swarmq.task import Task


async def run_worker_async(args: argparse.Namespace) -> None:
    """Import the user's module, build SwarmQ, run start_worker.

    Errors propagate to the synchronous ``main()`` for uniform
    rendering. The only thing this does itself is the ``no tasks
    registered`` check, which surfaces a config error rather than
    starting an idle worker that will silently never execute anything.
    """
    importlib.import_module(args.module)
    if not Task._registered_tasks:
        raise ConfigurationError(
            f"no tasks registered after importing {args.module!r}; "
            "ensure your tasks subclass swarmq.Task and the module's "
            "top-level imports execute the class definitions"
        )

    # Pre-check the env vars before SwarmQConfig so we can give a CLI-aware
    # error message that names the variable, instead of SwarmQConfig's
    # generic 'broker_url must not be empty'.
    broker_url = os.environ.get("SWARMQ_BROKER_URL", "")
    backend_url = os.environ.get("SWARMQ_BACKEND_URL", "")
    if not broker_url:
        raise ConfigurationError(
            "SWARMQ_BROKER_URL is not set; export it or pass it via env, "
            "e.g. SWARMQ_BROKER_URL=amqp://guest:guest@localhost:5672/"
        )
    if not backend_url:
        raise ConfigurationError(
            "SWARMQ_BACKEND_URL is not set; export it or pass it via env, "
            "e.g. SWARMQ_BACKEND_URL=valkey://localhost:6379/0"
        )

    # Lazy-local imports: keep `import swarmq.cli` from pulling the heavy
    # valkey + aio_pika + swarmq.app chains via this module. Pattern
    # mirrors ``cli/_shared.py::_backend_from_env``.
    from swarmq.app import SwarmQ
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

    broker = RabbitMQBroker(broker_url)
    backend = ValkeyBackend(backend_url)
    app = SwarmQ(
        broker,
        backend,
        queues=args.queues,
        concurrency=args.concurrency,
        log_level=args.log_level,
        log_json=(args.log_format == "json"),
        burst_mode=args.burst,
        burst_idle_timeout=args.burst_idle_timeout,
        priorities=args.priorities,
        metrics_port=args.metrics_port,
        health_port=args.health_port,
    )
    async with app:
        await app.start_worker()
