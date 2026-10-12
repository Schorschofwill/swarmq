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

import annotationlib
import contextvars
import inspect
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, ForwardRef, Protocol, cast, override

from swarmq.logging import get_logger

try:  # pragma: no cover - branch exercised via patch.dict in tests
    import dishka as _dishka  # noqa: F401  # pyright: ignore[reportUnusedImport]  # nur Präsenzprüfung des optionalen Pakets

    _HAVE_DISHKA = True
except ImportError:  # pragma: no cover - exercised via patch.dict in tests
    _HAVE_DISHKA = False  # pyright: ignore[reportConstantRedefinition]  # Fallback-Zweig des Präsenz-Flags

if TYPE_CHECKING:
    from dishka import AsyncContainer


class _NamedTaskInfo(Protocol):
    name: str


class _TaskWithInfo(Protocol):
    """Strukturelle Sicht auf eine Task-Klasse; ein Import von ``swarmq.task``
    schlösse einen Zyklus (task → dishka → task)."""

    task_info: _NamedTaskInfo


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

    def __new__(cls) -> _Missing:  # noqa: PYI034 - Singleton liefert die gespeicherte Instanz
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @override
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
    global _container  # noqa: PLW0603 - Modul-Singleton
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
    global _container  # noqa: PLW0603 - Modul-Singleton
    _container = _MISSING


_LIFECYCLE_METHODS: Final = ("run", "pre_execution", "post_execution")
_FORWARDREF: Final = annotationlib.Format.FORWARDREF


def _key_resolvable(container: object, key: object) -> bool:
    """Best-effort check: does ``container``'s provider graph carry a factory
    for ``key``? Walks the APP registry and its child (SESSION/REQUEST/…)
    registries via the (private) introspection surface verified on the pinned
    dishka version. Returns True if the introspection API is not shaped as
    expected, degrading R3b to container-presence-only rather than blocking a
    valid worker on an upstream API change."""
    try:
        reg: Any = cast("Any", container).registry  # private dishka-Introspektion, absichtlich ungetypt
        while reg is not None:
            if reg.get_factory(key) is not None:
                return True
            reg = reg.child_registry
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
    else:
        return False


def _reject_ineffective_fromdishka(registered_tasks: list[type], error_cls: type[Exception]) -> None:
    """R18: eine ``FromDishka``-Annotation ohne DependencyKey ist ein Startfehler.

    ``FromDishka[X] | None = None`` ergibt keinen Key; die Injektion faellt
    lautlos aus und der Parameter bleibt ``None``. Gilt in allen Worker-Modi.
    """
    offenders = [
        f"{cast('_TaskWithInfo', t).task_info.name}: {', '.join(params)}"
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


def _unwrapped_di_params(func: object) -> tuple[list[str], list[str]]:
    """DI-Parameter einer nicht gewrappten Methode, jetzt tolerant ausgewertet,
    und die Parameter, die auch jetzt noch unaufloesbar sind."""
    from typing import get_type_hints

    from dishka.integrations.base import default_parse_dependency

    try:
        hints = get_type_hints(func, include_extras=True, format=_FORWARDREF)
    except SyntaxError:
        return [], []  # T5 Regel 2: nicht auswertbar ist kein DI
    sig = inspect.signature(cast("Any", func), annotation_format=_FORWARDREF)
    di_params = [
        pname
        for pname, param in sig.parameters.items()
        if pname in hints and default_parse_dependency(param, hints[pname]) is not None
    ]
    unresolved = [pname for pname, hint in hints.items() if _contains_forward_ref(hint)]
    return di_params, unresolved


def _reject_unwrapped_di_methods(registered_tasks: list[type], error_cls: type[Exception]) -> None:
    """T6 (Plan 2026-10-12-1105): eine Methode, die bei der Klassendefinition
    nicht gewrappt wurde, nun aber einen DI-Key ergibt, injiziert nie.

    Unter PEP 563 ist das ein Alias oder ``FromComponent`` mit einer
    Annotation, die bei der Definition noch nicht aufloesbar war. Ein spaetes
    Wrappen griffe in Kindprozessen nicht, also scheitert der Start.
    """
    from dishka.integrations.base import is_dishka_injected

    offenders: list[str] = []
    for task in registered_tasks:
        for name in _LIFECYCLE_METHODS:
            func = task.__dict__.get(name)
            if func is None or is_dishka_injected(func):
                continue
            params, unresolved = _unwrapped_di_params(func)
            if params:
                task_name = cast("_TaskWithInfo", task).task_info.name
                offender = f"{task_name}: " + ", ".join(f"{name}.{p}" for p in params)
                if unresolved:
                    offender += " (" + ", ".join(f"{name}.{p} is unresolvable" for p in unresolved) + ")"
                offenders.append(offender)
    if offenders:
        raise error_cls(
            "dependency injection annotation(s) that were not resolvable at class definition, "
            "so nothing would be injected: "
            + "; ".join(offenders)
            + ". Every annotation of the method must be resolvable when the task class is defined: "
            "define or import the types before the class (no forward references, no TYPE_CHECKING-only imports)."
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
    _reject_unwrapped_di_methods(registered_tasks, ConfigurationError)
    di_tasks = [t for t in registered_tasks if getattr(t, "_dishka_injected_keys", None)]
    if not di_tasks:
        return

    container = get_registered_container()
    if container is _MISSING:
        names = ", ".join(cast("_TaskWithInfo", t).task_info.name for t in di_tasks)
        raise ConfigurationError(
            f"task(s) [{names}] use FromDishka but no Dishka container was "
            f"registered; call swarmq.setup_dishka(container) at module level "
            f"(see 'swarmq[dishka]')"
        )

    missing: list[str] = []
    for task in di_tasks:
        injected_keys: list[object] = cast("Any", task)._dishka_injected_keys  # dynamisch gesetztes Klassenattribut
        for key in injected_keys:
            if not _key_resolvable(container, key):
                type_hint = getattr(key, "type_hint", key)
                name = getattr(type_hint, "__name__", repr(type_hint))
                missing.append(f"{cast('_TaskWithInfo', task).task_info.name} -> {name}")
    if missing:
        detail = "; ".join(missing)
        raise ConfigurationError(
            f"Dishka container has no provider for injected type(s): {detail}. "
            f"Register the missing provider(s) (and SwarmQProvider for "
            f"TaskContext) before starting the worker."
        )


def _get_request_container(args: tuple[object, ...], kwargs: dict[str, object]) -> AsyncContainer:  # noqa: ARG001 - Signatur von dishka container_getter vorgegeben
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
    raw_annotations: Mapping[str, object],
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
        elif _annotation_mentions_fromdishka(hint) or _raw_text_mentions_fromdishka(raw_annotations.get(pname)):
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


def _raw_text_mentions_fromdishka(raw: object) -> bool:
    """Nennt eine Roh-Annotation ``FromDishka`` im Text? Das ist ein
    PEP-563-String oder unter PEP 649 der Text eines ganzen ForwardRef, z. B.
    ``ForwardRef('FromDishka[Later] | None')`` (T5 Regel 1)."""
    if isinstance(raw, ForwardRef):
        raw = raw.__forward_arg__
    return isinstance(raw, str) and "FromDishka" in raw


def _contains_forward_ref(hint: object) -> bool:
    """Steckt in einem ausgewerteten Hint (auch verschachtelt) ein ForwardRef?"""
    from typing import get_args

    return isinstance(hint, ForwardRef) or any(_contains_forward_ref(arg) for arg in get_args(hint))


def _strict_hints(cls: type, name: str, func: object) -> dict[str, Any]:
    """Strikte Auswertung; ein unaufloesbarer Name wird ein NameError mit Hinweis."""
    from typing import get_type_hints

    try:
        return get_type_hints(func, include_extras=True)
    except NameError as exc:
        raise NameError(
            f"Cannot resolve type hints on {cls.__name__}.{name} for "
            f"dishka injection: {exc}. Annotations on FromDishka "
            f"parameters must be importable at class-definition time "
            f"(no TYPE_CHECKING-only or forward-reference types)."
        ) from exc


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

    from typing import get_type_hints

    from dishka.integrations.base import is_dishka_injected, wrap_injection

    collected: list[object] = []
    ineffective: list[str] = []
    for name in _LIFECYCLE_METHODS:
        func = cls.__dict__.get(name)
        if func is None or is_dishka_injected(func):
            continue

        # E9: unter PEP 649 wirft schon der Zugriff auf ``__annotations__``
        # bei einem unaufloesbaren Namen; FORWARDREF liest tolerant.
        sig = inspect.signature(func, annotation_format=_FORWARDREF)
        raw_annotations = annotationlib.get_annotations(func, format=_FORWARDREF)
        if any(_raw_text_mentions_fromdishka(raw) for raw in raw_annotations.values()):
            # T5 Regel 1: ``FromDishka`` im Text -- strikt auswerten wie
            # bisher, ein unaufloesbarer Name ist ein NameError mit Hinweis.
            hints = _strict_hints(cls, name, func)
        else:
            # Tolerant: ein nicht aufloesbarer Name bleibt ein ForwardRef.
            # Erst die ausgewerteten Hints zeigen Aliase und FromComponent.
            # T5 Regel 2: was sich so nicht auswerten laesst, ist kein DI.
            try:
                hints = get_type_hints(func, include_extras=True, format=_FORWARDREF)
            except SyntaxError:
                continue

        method_keys, silent = _collect_method_keys(name, sig, hints, raw_annotations)
        ineffective.extend(silent)
        if not method_keys:
            continue
        unresolved = [pname for pname, hint in hints.items() if _contains_forward_ref(hint)]
        if unresolved:
            # T5 Regel 4: ``wrap_injection`` wertet strikt aus und wuerfe einen
            # rohen NameError. Unter PEP 563 (Strings) bleibt die Methode
            # ungewrappt, der Start meldet sie (T6); unter PEP 649 bleibt es
            # beim NameError mit Hinweis.
            if all(isinstance(raw_annotations.get(pname), str) for pname in unresolved):
                continue
            _strict_hints(cls, name, func)

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
        cls._dishka_injected_keys = collected  # type: ignore[attr-defined]  # dynamisch gesetztes Klassenattribut
    if ineffective:
        cls._dishka_ineffective_fromdishka = ineffective  # type: ignore[attr-defined]  # dynamisch gesetztes Klassenattribut


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
