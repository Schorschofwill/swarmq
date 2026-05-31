"""Optional Dishka dependency-injection integration for SwarmQ tasks.

Importing this module never requires ``dishka`` to be installed — the
core symbols (:class:`TaskContext`, :func:`setup_dishka`,
:func:`get_registered_container`) are pure Python. The DI machinery
(:class:`SwarmQProvider`, auto-wrapping of task lifecycle methods)
activates only when ``dishka`` is installed *and* a container has been
registered via :func:`setup_dishka`.

Design (see ``docs/plans/2026-05-31-001-feat-dishka-di-tasks-plan.md``):

- ``setup_dishka(container)`` stores the user-built ``AsyncContainer`` in a
  module-level slot, mirroring ``Task._registered_tasks``. Call it at module
  level so each spawned worker subprocess rebuilds it fresh on re-import.
- The worker reads the slot, opens a REQUEST scope per task, and exposes the
  per-execution :class:`TaskContext` to providers via ``from_context``.
- ``request_container`` carries the per-task REQUEST container to the
  ``@inject``-wrapped lifecycle methods without changing their call sites.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

try:  # pragma: no cover - branch exercised via patch.dict in tests
    import dishka as _dishka  # noqa: F401

    _HAVE_DISHKA = True
except ImportError:  # pragma: no cover - exercised via patch.dict in tests
    _HAVE_DISHKA = False

if TYPE_CHECKING:
    from dishka import AsyncContainer


@dataclass(frozen=True, slots=True)
class TaskContext:
    """Per-execution task metadata injectable into REQUEST-scoped providers.

    Populated by the worker from the message before the task lifecycle runs
    and made available via ``from_context(TaskContext, scope=Scope.REQUEST)``
    (see :class:`SwarmQProvider`). These four fields are the stable public
    contract — they mirror the worker's ``MessageContext``.
    """

    task_id: str
    task_name: str
    queue: str
    retry_count: int


class _Missing:
    """Singleton sentinel distinguishing 'never registered' from an explicit
    ``None`` container."""

    _instance: _Missing | None = None

    def __new__(cls) -> _Missing:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "<swarmq.dishka.MISSING>"


_MISSING: Final = _Missing()

# Module-level container slot. Mirrors ``Task._registered_tasks``: set at
# user-module import time via setup_dishka, read by the worker after import.
_container: AsyncContainer | _Missing = _MISSING

# Per-task REQUEST container. The worker sets this inside ``_handle_message``
# (before the run coroutine's ``asyncio.create_task``, so it is copied into the
# run task's context) and resets it afterwards. The ``@inject``-wrapped task
# methods read it via their ``container_getter`` — this keeps the call site
# ``task_instance.run(*args, **kwargs)`` unchanged.
request_container: contextvars.ContextVar = contextvars.ContextVar(
    "swarmq_request_container"
)


def setup_dishka(container: AsyncContainer) -> None:
    """Register the application's Dishka ``AsyncContainer`` (APP scope).

    Call at module level so each spawned worker subprocess rebuilds the
    container fresh on re-import (spawn-safe). The worker reads it after
    importing the task module.
    """
    global _container
    _container = container


def get_registered_container() -> AsyncContainer | _Missing:
    """Return the registered container, or :data:`_MISSING` if
    :func:`setup_dishka` was never called."""
    return _container


def _reset_container() -> None:
    """Test-only: clear the registered container slot."""
    global _container
    _container = _MISSING


if _HAVE_DISHKA:
    from dishka import Provider, Scope, from_context

    class SwarmQProvider(Provider):
        """Built-in provider exposing the per-task :class:`TaskContext` at
        REQUEST scope. Add it to your container when you want providers to
        depend on ``TaskContext``."""

        task_context = from_context(TaskContext, scope=Scope.REQUEST)

else:
    # Drop any stale binding (e.g. left by importlib.reload after dishka was
    # removed) so the __getattr__ fallback below is the only path to the name.
    globals().pop("SwarmQProvider", None)

    def __getattr__(name: str) -> object:
        # Surface a clear error if SwarmQProvider is accessed without dishka,
        # rather than returning a confusing None.
        if name == "SwarmQProvider":
            raise ImportError(
                "SwarmQProvider requires the 'dishka' extra: "
                "pip install 'swarmq[dishka]'"
            )
        raise AttributeError(f"module 'swarmq.dishka' has no attribute {name!r}")
