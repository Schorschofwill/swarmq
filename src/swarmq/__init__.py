"""SwarmQ public package surface.

``ValkeyBackend`` and ``RabbitMQBroker`` are resolved LAZILY via
PEP-562 ``__getattr__`` so ``import swarmq`` (and transitively
``import swarmq.cli``) does not pull the heavy ``valkey`` / ``aio_pika``
import chains into ``sys.modules``. Identity is preserved on first
access — ``from swarmq import ValkeyBackend`` returns the same class
object as ``from swarmq.backend.valkey import ValkeyBackend``.

See plan ``docs/plans/2026-04-23-002-fix-import-laziness-plan.md``
Unit 1 for the rationale; Plan 2026-04-23-001 Unit 1 established the
same pattern in ``swarmq/cli/__init__.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from swarmq.exceptions import (
    BackendConnectionError,
    BackendResponseError,
    BrokerConnectionError,
    ConfigurationError,
    LoggingConfigError,
    NoRetry,
    PublishIndeterminate,
    ResultExpiredError,
    ResultNotReadyError,
    ResultSerializationError,
    Retry,
    SoftTimeoutError,
    SwarmQError,
    TaskCancelledError,
    TaskError,
    TaskNotFoundError,
    WorkflowNotFound,
    WorkflowResultExpired,
    WorkflowValidationError,
)
from swarmq.progress import ProgressInfo
from swarmq.workflow import Signature, chain, chord, group, sig

if TYPE_CHECKING:
    # Type-checker only — at runtime these resolve through __getattr__.
    from swarmq.app import SwarmQ
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker
    from swarmq.health import HealthChecker, HealthServer, serve_health
    from swarmq.metrics import (
        Metrics,
        MetricsServer,
        install_signal_collectors,
        serve_metrics,
    )
    from swarmq.middleware import MessageContext, Middleware
    from swarmq.signals import Signal, SignalDispatcher
    from swarmq.task import DeliveryMode, Task, TaskInfo
    from swarmq.workflow_orchestration import WorkflowHandle


_LAZY_SURFACE: dict[str, tuple[str, str]] = {
    "SwarmQ": ("swarmq.app", "SwarmQ"),
    "ValkeyBackend": ("swarmq.backend.valkey", "ValkeyBackend"),
    "RabbitMQBroker": ("swarmq.broker.rabbitmq", "RabbitMQBroker"),
    # Optional-feature surfaces; lazy keeps prometheus_client / asyncio.base_events
    # off the `import swarmq.cli` hot path.
    "HealthChecker": ("swarmq.health", "HealthChecker"),
    "HealthServer": ("swarmq.health", "HealthServer"),
    "serve_health": ("swarmq.health", "serve_health"),
    "Metrics": ("swarmq.metrics", "Metrics"),
    "MetricsServer": ("swarmq.metrics", "MetricsServer"),
    "install_signal_collectors": ("swarmq.metrics", "install_signal_collectors"),
    "serve_metrics": ("swarmq.metrics", "serve_metrics"),
    "MessageContext": ("swarmq.middleware", "MessageContext"),
    "Middleware": ("swarmq.middleware", "Middleware"),
    # Signals + task pull structlog / asyncio chains; lazy gates them too.
    "Signal": ("swarmq.signals", "Signal"),
    "SignalDispatcher": ("swarmq.signals", "SignalDispatcher"),
    "DeliveryMode": ("swarmq.task", "DeliveryMode"),
    "Task": ("swarmq.task", "Task"),
    "TaskInfo": ("swarmq.task", "TaskInfo"),
    "WorkflowHandle": ("swarmq.workflow_orchestration", "WorkflowHandle"),
    # Optional Dishka DI surface. swarmq.dishka imports without dishka
    # installed; SwarmQProvider access raises a clear error when it is absent.
    "setup_dishka": ("swarmq.dishka", "setup_dishka"),
    "TaskContext": ("swarmq.dishka", "TaskContext"),
    "SwarmQProvider": ("swarmq.dishka", "SwarmQProvider"),
}


def __getattr__(name: str) -> object:
    """PEP-562 lazy resolver for the heavy public surface.

    Defers loading ``valkey`` / ``aio_pika`` until first attribute
    access. Returns the canonical class object via
    ``importlib.import_module`` so ``is`` comparisons hold against
    direct imports from the source module.
    """
    if name in _LAZY_SURFACE:
        import importlib

        module_name, attr = _LAZY_SURFACE[name]
        return getattr(importlib.import_module(module_name), attr)
    raise AttributeError(f"module 'swarmq' has no attribute {name!r}")


def __dir__() -> list[str]:
    """Advertise lazy names so ``dir(swarmq)`` and IDE completion see them."""
    return sorted(set(globals()) | set(_LAZY_SURFACE))


__all__ = [
    # Exceptions
    "BackendConnectionError",
    "BackendResponseError",
    "BrokerConnectionError",
    "ConfigurationError",
    # Task
    "DeliveryMode",
    # Health
    "HealthChecker",
    "HealthServer",
    "LoggingConfigError",
    # Middleware
    "MessageContext",
    # Metrics
    "Metrics",
    "MetricsServer",
    "Middleware",
    "NoRetry",
    # Progress
    "ProgressInfo",
    "PublishIndeterminate",
    # Heavy lazy surface (resolved via __getattr__)
    "RabbitMQBroker",
    "ResultExpiredError",
    "ResultNotReadyError",
    "ResultSerializationError",
    "Retry",
    # Signals
    "Signal",
    "SignalDispatcher",
    # Workflow primitives
    "Signature",
    "SoftTimeoutError",
    # Core API
    "SwarmQ",
    "SwarmQError",
    "SwarmQProvider",
    "Task",
    "TaskCancelledError",
    "TaskContext",
    "TaskError",
    "TaskInfo",
    "TaskNotFoundError",
    "ValkeyBackend",
    "WorkflowHandle",
    "WorkflowNotFound",
    "WorkflowResultExpired",
    "WorkflowValidationError",
    "chain",
    "chord",
    "group",
    "install_signal_collectors",
    "serve_health",
    "serve_metrics",
    # Optional Dishka DI
    "setup_dishka",
    "sig",
]
