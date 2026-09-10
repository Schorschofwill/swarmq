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
from typing import Any

from swarmq.exceptions import ConfigurationError
from swarmq.task import Task


async def run_worker_async(args: argparse.Namespace) -> None:
    """Import the user's module, build SwarmQ, run start_worker.

    Errors propagate to the synchronous ``main()`` for uniform
    rendering. The only thing this does itself is the ``no tasks
    registered`` check, which surfaces a config error rather than
    starting an idle worker that will silently never execute anything.
    """
    try:
        importlib.import_module(args.module)
    except ImportError as exc:
        # R3a: a task module that uses FromDishka triggers `from dishka import
        # FromDishka` at import; without the extra installed that raises a raw
        # ImportError here, before any SwarmQ validation. Re-raise with an
        # actionable hint instead of a bare traceback.
        from swarmq.dishka import _HAVE_DISHKA

        # Only rephrase when the failure is specifically the missing dishka
        # package (ModuleNotFoundError.name == "dishka" or a "dishka.*"
        # submodule) — a substring match on the message would misclassify an
        # unrelated ImportError that merely mentions "dishka".
        missing = getattr(exc, "name", None) or ""
        if not _HAVE_DISHKA and (missing == "dishka" or missing.startswith("dishka.")):
            raise ConfigurationError(
                f"importing {args.module!r} failed because the 'dishka' extra "
                f"is not installed but a task uses FromDishka: {exc}. "
                f"Install it with: pip install 'swarmq[dishka]'"
            ) from exc
        raise

    if not Task._registered_tasks:
        raise ConfigurationError(
            f"no tasks registered after importing {args.module!r}; "
            "ensure your tasks subclass swarmq.Task and the module's "
            "top-level imports execute the class definitions"
        )

    # R3b: fail fast at worker start if a task uses FromDishka but the
    # container is missing or cannot resolve an injected type — instead of a
    # NoFactoryError mid-task on first execution.
    from swarmq.dishka import validate_registered_tasks

    validate_registered_tasks(Task._registered_tasks)

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
    pool_kwargs = {
        name: value
        for name in (
            "pool",
            "recycle_rss_mb",
            "recycle_max_tasks",
            "heartbeat_interval_s",
            "heartbeat_timeout_s",
            "consumer_timeout_s",
            "malloc_trim_threshold",
            "malloc_arena_max",
            "compute_thread_limit",
        )
        if (value := getattr(args, name, None)) is not None
    }
    app = SwarmQ(
        broker,
        backend,
        queues=args.queues,
        concurrency=args.concurrency,
        **pool_kwargs,
        log_level=args.log_level,
        log_json=(args.log_format == "json"),
        burst_mode=args.burst,
        burst_idle_timeout=args.burst_idle_timeout,
        priorities=args.priorities,
        metrics_port=args.metrics_port,
        health_port=args.health_port,
    )
    configure = getattr(args, "configure", None)
    if configure is not None:
        await _run_configure_hook(app, configure)

    async with app:
        await app.start_worker()


async def _run_configure_hook(app: Any, spec: str) -> None:
    """``--configure MODULE:CALLABLE`` nach der App-Konstruktion aufrufen
    (R9/R10, KTD5).

    Der Import passiert hier, im Worker-Prozess -- nicht im Supervisor und
    nicht im Forkserver-Preload: der Hook ist Anwendungscode und soll nur
    dort laufen, wo Tasks laufen. Ein Awaitable wird abgewartet, damit ein
    ``async def setup(app)`` nicht stillschweigend nie laeuft.

    Jeder Fehler wird in einen ``ConfigurationError`` gehuellt und nennt
    Modul und Callable: ein Neustart wuerde ihn reproduzieren, also endet der
    Worker damit mit Exit 78 statt in fuenf Neustarts.
    """
    import inspect

    module_name, _, attr_name = spec.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ConfigurationError(
            f"--configure {spec!r}: importing {module_name!r} failed: {exc}"
        ) from exc

    hook = getattr(module, attr_name, None)
    if hook is None:
        raise ConfigurationError(
            f"--configure {spec!r}: {module_name!r} has no attribute {attr_name!r}"
        )
    if not callable(hook):
        raise ConfigurationError(
            f"--configure {spec!r}: {module_name}.{attr_name} is not callable"
        )

    try:
        result = hook(app)
        if inspect.isawaitable(result):
            await result
    except Exception as exc:
        raise ConfigurationError(
            f"--configure {spec!r}: {module_name}.{attr_name} raised "
            f"{type(exc).__name__}: {exc}"
        ) from exc
