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
import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from swarmq.logging import get_logger

try:  # pragma: no cover - branch exercised via patch.dict in tests
    import dishka as _dishka  # noqa: F401

    _HAVE_DISHKA = True
except ImportError:  # pragma: no cover - exercised via patch.dict in tests
    _HAVE_DISHKA = False

if TYPE_CHECKING:
    from dishka import AsyncContainer

logger = get_logger(__name__)


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
request_container: contextvars.ContextVar[AsyncContainer] = contextvars.ContextVar("swarmq_request_container")


def setup_dishka(container: AsyncContainer) -> None:
    """Register the application's Dishka ``AsyncContainer`` (APP scope).

    Call at module level so each spawned worker subprocess rebuilds the
    container fresh on re-import (spawn-safe). The worker reads it after
    importing the task module.

    Calling it twice replaces the previous container (logged at
    WARNING — two modules both calling setup_dishka is almost
    always an accident, and the silent last-writer-wins made the losing
    module's providers vanish without a trace).
    """
    global _container
    if _container is not _MISSING and _container is not container:
        logger.warning(
            "dishka_container_replaced",
            hint=(
                "setup_dishka() was already called with a different container; the previous registration is discarded"
            ),
        )
    _container = container


def get_registered_container() -> AsyncContainer | _Missing:
    """Return the registered container, or :data:`_MISSING` if
    :func:`setup_dishka` was never called."""
    return _container


def _reset_container() -> None:
    """Test-only: clear the registered container slot."""
    global _container
    _container = _MISSING


_LIFECYCLE_METHODS: Final = ("run", "pre_execution", "post_execution")


def _key_resolvable(container: object, key: object) -> bool:
    """Best-effort check: does ``container``'s provider graph carry a factory
    for ``key``? Walks the APP registry and its child (SESSION/REQUEST/…)
    registries via the (private) introspection surface verified on the pinned
    dishka version. Returns True if the introspection API is not shaped as
    expected, degrading R3b to container-presence-only rather than blocking a
    valid worker on an upstream API change."""
    try:
        reg = container.registry  # type: ignore[attr-defined]
        while reg is not None:
            if reg.get_factory(key) is not None:
                return True
            reg = reg.child_registry
        return False
    except AttributeError:
        # Introspection surface changed across dishka versions — degrade to
        # presence-only (the container-registered check already passed). Log
        # the degradation rather than silently skipping resolvability.
        logger.warning(
            "dishka_resolvability_introspection_unavailable",
            detail=(
                "dishka registry introspection API not as expected; R3b validation degraded to container-presence-only"
            ),
        )
        return True


def _reject_ineffective_fromdishka(registered_tasks: list[type], error_cls: type[Exception]) -> None:
    """R18: eine ``FromDishka``-Annotation ohne DependencyKey ist ein Startfehler.

    ``FromDishka[X] | None = None`` ergibt keinen Key; die Injektion faellt
    lautlos aus und der Parameter bleibt ``None``. Gilt in allen Worker-Modi.
    """
    offenders = [
        f"{t.task_info.name}: {', '.join(params)}"  # type: ignore[attr-defined]
        for t in registered_tasks
        if (params := getattr(t, "_dishka_ineffective_fromdishka", None))
    ]
    if offenders:
        raise error_cls(
            "FromDishka annotation(s) that never inject: "
            + "; ".join(offenders)
            + ". Declare the parameter as `name: FromDishka[Type]` without "
            "`| None` / `Optional` and without a default — otherwise dishka "
            "derives no dependency key and silently passes None."
        )


def validate_registered_tasks(registered_tasks: list[type]) -> None:
    """Worker-start validation for DI tasks (plan R3b).

    For every registered task that declares ``FromDishka`` parameters, ensure a
    container is registered and each injected type resolves against the
    container graph. Catches Dishka's otherwise runtime-deferred
    ``NoFactoryError`` at startup instead of mid-task. No-op when dishka is
    absent or no task uses injection.
    """
    if not _HAVE_DISHKA:
        return

    from swarmq.exceptions import ConfigurationError

    _reject_ineffective_fromdishka(registered_tasks, ConfigurationError)
    di_tasks = [t for t in registered_tasks if getattr(t, "_dishka_injected_keys", None)]
    if not di_tasks:
        return

    container = get_registered_container()
    if container is _MISSING:
        names = ", ".join(t.task_info.name for t in di_tasks)  # type: ignore[attr-defined]
        raise ConfigurationError(
            f"task(s) [{names}] use FromDishka but no Dishka container was "
            f"registered; call swarmq.setup_dishka(container) at module level "
            f"(see 'swarmq[dishka]')"
        )

    missing: list[str] = []
    for task in di_tasks:
        for key in task._dishka_injected_keys:  # type: ignore[attr-defined]
            if not _key_resolvable(container, key):
                type_hint = getattr(key, "type_hint", key)
                name = getattr(type_hint, "__name__", repr(type_hint))
                missing.append(f"{task.task_info.name} -> {name}")  # type: ignore[attr-defined]
    if missing:
        detail = "; ".join(missing)
        raise ConfigurationError(
            f"Dishka container has no provider for injected type(s): {detail}. "
            f"Register the missing provider(s) (and SwarmQProvider for "
            f"TaskContext) before starting the worker."
        )


def _get_request_container(args: tuple[object, ...], kwargs: dict[str, object]) -> AsyncContainer:
    """container_getter for wrap_injection: returns the per-task REQUEST
    container set by the worker. Raises a clear error if called outside an
    active request scope (e.g. a FromDishka task invoked directly in a test
    without a worker)."""
    try:
        return request_container.get()
    except LookupError:
        raise RuntimeError(
            "No active SwarmQ request scope; FromDishka injection only works when the task runs through a worker."
        ) from None


def _collect_method_keys(
    name: str,
    sig: inspect.Signature,
    hints: dict[str, object],
    raw_annotations: dict[str, object],
) -> tuple[list[object], list[str]]:
    """DependencyKeys einer Lifecycle-Methode plus wirkungslose Annotationen.

    R18 (Plan 2026-09-03-001): nennt eine Annotation FromDishka, ergibt aber
    keinen Key — typisch ``FromDishka[X] | None = None`` — fiele die Injektion
    lautlos aus. Solche Parameter landen als ``"<methode>.<param>"`` in der
    zweiten Liste; ``validate_registered_tasks`` meldet sie beim Start.
    """
    from dishka.integrations.base import default_parse_dependency

    keys: list[object] = []
    silent: list[str] = []
    for pname, param in sig.parameters.items():
        hint = hints.get(pname)
        dep = default_parse_dependency(param, hint) if hint is not None else None
        if dep is not None:
            keys.append(dep)
        elif _annotation_mentions_fromdishka(raw_annotations.get(pname)) or (
            isinstance(raw_annotations.get(pname), str) and "FromDishka" in str(raw_annotations.get(pname))
        ):
            silent.append(f"{name}.{pname}")
    return keys, silent


def _annotation_mentions_fromdishka(raw: object) -> bool:
    """Strukturelle Suche nach dem FromDishka-Marker in ausgewerteten Annotationen.

    Steigt durch ``Optional``/``Union``/Generics ab und prueft ``Annotated``-
    Metadaten auf dishkas ``FromComponent``-Marker.
    """
    from typing import Annotated, get_args, get_origin

    if get_origin(raw) is Annotated:
        metadata = getattr(raw, "__metadata__", ())
        if any("FromComponent" in type(marker).__name__ for marker in metadata):
            return True
    return any(_annotation_mentions_fromdishka(arg) for arg in get_args(raw))


def maybe_wrap_task(cls: type) -> None:
    """Auto-wrap a Task subclass's lifecycle methods for FromDishka injection.

    Called from ``Task.__init_subclass__`` *after* the reserved-param check.
    No-op when dishka is not installed, or when no lifecycle method declares a
    ``FromDishka`` parameter (``wrap_injection`` would return the function
    unchanged anyway). Records the injected dependency keys on
    ``cls._dishka_injected_keys`` so the worker-start validation (U5) can check
    resolvability against the container.
    """
    if not _HAVE_DISHKA:
        return

    import inspect
    from typing import get_type_hints

    from dishka.integrations.base import (
        default_parse_dependency,
        is_dishka_injected,
        wrap_injection,
    )

    def _raw_mentions_fromdishka(param: inspect.Parameter, raw: object) -> bool:
        # Cheap, get_type_hints-free pre-check covering both annotation forms:
        # string annotations (PEP 563 / from __future__ import annotations) and
        # already-evaluated Annotated objects. Lets non-DI tasks skip the
        # resolution below entirely, so existing tasks with forward-reference
        # or TYPE_CHECKING-only annotations are never broken — only genuine DI
        # users pay the cost and get the strict error.
        if isinstance(raw, str):
            return "FromDishka" in raw
        if default_parse_dependency(param, raw) is not None:
            return True
        # R18: ``FromDishka[X] | None`` ergibt keinen Key, nennt FromDishka
        # aber strukturell — nur so faellt die wirkungslose Annotation auf.
        return _annotation_mentions_fromdishka(raw)

    collected: list[object] = []
    ineffective: list[str] = []
    for name in _LIFECYCLE_METHODS:
        func = cls.__dict__.get(name)
        if func is None or is_dishka_injected(func):
            continue

        sig = inspect.signature(func)
        raw_annotations = getattr(func, "__annotations__", {})
        if not any(
            _raw_mentions_fromdishka(param, raw_annotations.get(pname)) for pname, param in sig.parameters.items()
        ):
            continue

        # This method declares FromDishka params. Resolve hints (strictly) and
        # derive the injected DependencyKeys — wrap_injection does not expose
        # them on its result.
        try:
            hints = get_type_hints(func, include_extras=True)
        except NameError as exc:
            raise NameError(
                f"Cannot resolve type hints on {cls.__name__}.{name} for "
                f"dishka injection: {exc}. Annotations on FromDishka "
                f"parameters must be importable at class-definition time "
                f"(no TYPE_CHECKING-only or forward-reference types)."
            ) from exc

        method_keys, silent = _collect_method_keys(name, sig, hints, raw_annotations)
        ineffective.extend(silent)
        if not method_keys:
            continue

        collected.extend(method_keys)
        setattr(
            cls,
            name,
            wrap_injection(
                func=func,
                is_async=True,
                manage_scope=False,
                container_getter=_get_request_container,
            ),
        )

    if collected:
        cls._dishka_injected_keys = collected  # type: ignore[attr-defined]
    if ineffective:
        cls._dishka_ineffective_fromdishka = ineffective  # type: ignore[attr-defined]


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
            raise ImportError("SwarmQProvider requires the 'dishka' extra: pip install 'swarmq[dishka]'")
        raise AttributeError(f"module 'swarmq.dishka' has no attribute {name!r}")
