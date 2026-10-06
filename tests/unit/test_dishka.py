"""Unit tests for the optional Dishka DI foundation (plan U1).

Covers the dishka-free core (TaskContext, container slot, sentinel) and the
guarded SwarmQProvider surface. The dishka-absent path is exercised by loading
a fresh throwaway copy of the module from source with ``dishka`` blocked via
``patch.dict(sys.modules, {"dishka": None})`` — never reloading the live
module, which would rebind _MISSING/TaskContext and break identity checks in
importers like worker.py.

Note: this module intentionally does NOT use ``from __future__ import
annotations``. The DI tests define Task subclasses inside test functions, and
``get_type_hints`` must resolve their ``FromDishka`` parameter annotations —
eager (non-string) annotations resolve against the local scope, string
annotations would not. Real user task modules define tasks at module level, so
the future import is fine there.
"""

import dataclasses
import sys
from unittest.mock import patch

import pytest

from swarmq import dishka as di


def test_setup_and_get_round_trip():
    """Happy path: a registered container is returned by the getter."""
    sentinel = object()
    di.setup_dishka(sentinel)  # type: ignore[arg-type]
    assert di.get_registered_container() is sentinel


def test_missing_when_never_registered():
    """Edge case: the slot reads _MISSING, not None, before any setup."""
    # The autouse _reset_dishka_container fixture restores the slot; assert the
    # pristine state by resetting explicitly within the test.
    di._reset_container()
    assert di.get_registered_container() is di._MISSING
    assert di.get_registered_container() is not None


def test_missing_is_singleton():
    assert di._Missing() is di._MISSING
    assert repr(di._MISSING) == "<swarmq.dishka.MISSING>"


def test_task_context_is_frozen_with_exact_fields():
    """R10/R14: TaskContext is a frozen dataclass with exactly four fields."""
    ctx = di.TaskContext(
        task_id="t1", task_name="job", queue="default", retry_count=2
    )
    assert ctx.task_id == "t1"
    assert ctx.task_name == "job"
    assert ctx.queue == "default"
    assert ctx.retry_count == 2
    field_names = {f.name for f in dataclasses.fields(ctx)}
    assert field_names == {"task_id", "task_name", "queue", "retry_count"}
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.task_id = "mutated"  # type: ignore[misc]


def test_swarmqprovider_declares_request_task_context():
    """R10: SwarmQProvider exposes TaskContext via from_context at REQUEST scope."""
    pytest.importorskip("dishka")
    from dishka import make_async_container

    provider = di.SwarmQProvider()
    assert provider is not None
    # The from_context dependency source is registered on the provider; the
    # exact attribute shape is dishka-internal, so assert a container built
    # with this provider constructs without error.
    container = make_async_container(provider)
    assert container is not None


def test_request_container_contextvar_exists():
    import contextvars

    assert isinstance(di.request_container, contextvars.ContextVar)


def _make_container_with_svc():
    """Build a container whose REQUEST scope provides a Svc bound to TaskContext."""
    from collections.abc import AsyncIterable

    from dishka import Provider, Scope, make_async_container, provide

    class Svc:
        def __init__(self, ctx: di.TaskContext) -> None:
            self.tid = ctx.task_id

    class Prov(Provider):
        from dishka import from_context

        tc = from_context(di.TaskContext, scope=Scope.REQUEST)

        @provide(scope=Scope.REQUEST)
        async def svc(self, ctx: di.TaskContext) -> AsyncIterable[Svc]:
            yield Svc(ctx)

    return make_async_container(Prov()), Svc


@pytest.mark.timeout(5)
async def test_lifecycle_injection_through_task_subclass():
    """AE1/R4/R5: a Task subclass's run gets FromDishka params filled from the
    per-task REQUEST container; plain args still come from the call."""
    pytest.importorskip("dishka")
    from dishka import FromDishka

    from swarmq.task import Task, TaskInfo

    container, Svc = _make_container_with_svc()

    class InjectTask(Task):
        task_info = TaskInfo(name="inject_task", queues=["q"])

        async def run(self, x: int, svc: FromDishka[Svc]) -> str:  # type: ignore[valid-type]
            return f"{x}:{svc.tid}"

    # run was wrapped (FromDishka present).
    from dishka.integrations.base import is_dishka_injected

    assert is_dishka_injected(InjectTask.run)

    ctx = di.TaskContext(
        task_id="abc", task_name="inject_task", queue="q", retry_count=0
    )
    req = await container(context={di.TaskContext: ctx}).__aenter__()
    token = di.request_container.set(req)
    try:
        result = await InjectTask().run(5)  # svc injected, x positional
    finally:
        await req.__aexit__(None, None, None)
        di.request_container.reset(token)
        await container.close()

    assert result == "5:abc"


def test_no_fromdishka_means_no_wrapping():
    """R6/AE2: a task without FromDishka params is left untouched."""
    pytest.importorskip("dishka")
    from dishka.integrations.base import is_dishka_injected

    from swarmq.task import Task, TaskInfo

    class PlainTask(Task):
        task_info = TaskInfo(name="plain_task", queues=["q"])

        async def run(self, x: int) -> int:
            return x * 2

    assert not is_dishka_injected(PlainTask.run)
    assert not hasattr(PlainTask, "_dishka_injected_keys")


def test_injected_keys_recorded_for_resolvability_check():
    """U2->U5 contract: injected DependencyKeys are recorded on the class."""
    pytest.importorskip("dishka")
    from dishka import FromDishka

    from swarmq.task import Task, TaskInfo

    class Dep:
        pass

    class KeyTask(Task):
        task_info = TaskInfo(name="key_task", queues=["q"])

        async def run(self, dep: FromDishka[Dep]) -> None:  # type: ignore[valid-type]
            pass

    keys = KeyTask._dishka_injected_keys  # type: ignore[attr-defined]
    assert any(getattr(k, "type_hint", None) is Dep for k in keys)


def test_reserved_param_still_fails_with_fromdishka_annotation():
    """R7/AE9: a reserved param name fails the class-def check even when
    annotated with FromDishka — the reserved check runs on the raw signature
    before wrapping."""
    pytest.importorskip("dishka")
    from dishka import FromDishka

    from swarmq.task import Task, TaskInfo

    class Dep:
        pass

    with pytest.raises(ValueError, match="reserved"):

        class BadTask(Task):
            task_info = TaskInfo(name="bad_task", queues=["q"])

            async def run(self, delay: FromDishka[Dep]) -> None:  # type: ignore[valid-type]
                pass


def test_wrapped_run_preserves_return_annotation_for_registry():
    """Registry return-type detection survives wrapping."""
    pytest.importorskip("dishka")
    import inspect

    from dishka import FromDishka

    from swarmq.task import Task, TaskInfo

    class Dep:
        pass

    class RetTask(Task):
        task_info = TaskInfo(name="ret_task", queues=["q"])

        async def run(self, dep: FromDishka[Dep]) -> str:  # type: ignore[valid-type]
            return "ok"

    ann = inspect.get_annotations(RetTask.run, eval_str=True)
    assert ann.get("return") is str


def test_validate_no_di_tasks_is_noop():
    """R3b: validation is a no-op when no registered task uses FromDishka."""
    from swarmq.dishka import validate_registered_tasks

    validate_registered_tasks([])  # must not raise


def test_validate_di_task_without_container_raises():
    """AE3/R3b: a FromDishka task with no registered container fails at
    validation time with an actionable message."""
    pytest.importorskip("dishka")
    from dishka import FromDishka

    from swarmq.dishka import validate_registered_tasks
    from swarmq.exceptions import ConfigurationError
    from swarmq.task import Task, TaskInfo

    class Dep:
        pass

    class VT1(Task):
        task_info = TaskInfo(name="vt1", queues=["q"])

        async def run(self, d: FromDishka[Dep]) -> None:  # type: ignore[valid-type]
            pass

    di._reset_container()
    with pytest.raises(ConfigurationError, match="setup_dishka"):
        validate_registered_tasks([VT1])


def test_validate_di_task_missing_provider_raises():
    """R3b: a registered container lacking a provider for an injected type
    fails at validation, not at first execution (no deferred NoFactoryError)."""
    pytest.importorskip("dishka")
    from dishka import FromDishka, make_async_container

    from swarmq.dishka import validate_registered_tasks
    from swarmq.exceptions import ConfigurationError
    from swarmq.task import Task, TaskInfo

    class Dep:
        pass

    class VT2(Task):
        task_info = TaskInfo(name="vt2", queues=["q"])

        async def run(self, d: FromDishka[Dep]) -> None:  # type: ignore[valid-type]
            pass

    di.setup_dishka(make_async_container())  # no provider for Dep
    with pytest.raises(ConfigurationError, match="no provider"):
        validate_registered_tasks([VT2])


def test_validate_di_task_resolvable_passes():
    """R3b: when every injected type resolves, validation passes silently."""
    pytest.importorskip("dishka")
    from dishka import FromDishka, Provider, Scope, make_async_container, provide

    from swarmq.dishka import validate_registered_tasks
    from swarmq.task import Task, TaskInfo

    class Dep:
        pass

    class DepProvider(Provider):
        dep = provide(Dep, scope=Scope.APP)

    class VT3(Task):
        task_info = TaskInfo(name="vt3", queues=["q"])

        async def run(self, d: FromDishka[Dep]) -> None:  # type: ignore[valid-type]
            pass

    di.setup_dishka(make_async_container(DepProvider()))
    validate_registered_tasks([VT3])  # must not raise


def test_module_imports_without_dishka_and_core_symbols_usable():
    """R2/AE6: swarmq.dishka imports with dishka absent; core symbols work,
    SwarmQProvider access raises a clear error.

    Loads a FRESH, throwaway copy of the module from its source file with
    dishka blocked — never reloads the live swarmq.dishka, which would rebind
    _MISSING/TaskContext and break the identity checks in modules (e.g.
    worker.py) that imported those objects by reference.
    """
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "swarmq._dishka_noimport_probe", Path(di.__file__)
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # Block dishka AND register the probe module under its own name so the
    # frozen+slots dataclass can resolve cls.__module__ during creation
    # (CPython dataclasses does sys.modules[cls.__module__].__dict__).
    with patch.dict(
        sys.modules, {"dishka": None, "swarmq._dishka_noimport_probe": mod}
    ):
        spec.loader.exec_module(mod)

    assert mod._HAVE_DISHKA is False
    # Core symbols stay usable without dishka.
    ctx = mod.TaskContext(task_id="x", task_name="y", queue="q", retry_count=0)
    assert ctx.task_id == "x"
    sentinel = object()
    mod.setup_dishka(sentinel)
    assert mod.get_registered_container() is sentinel
    # SwarmQProvider access raises a clear, actionable error.
    with pytest.raises(ImportError, match=r"swarmq\[dishka\]"):
        _ = mod.SwarmQProvider


class TestDoubleSetupWarning:
    """Härtung R2 P2: zwei Module, die beide setup_dishka() aufrufen,
    sind fast immer ein Versehen — stilles last-writer-wins ließ die
    Provider des Verlierers spurlos verschwinden."""

    def test_second_setup_warns(self):
        from unittest.mock import MagicMock, patch

        from swarmq.dishka import _reset_container, setup_dishka

        _reset_container()
        c1, c2 = MagicMock(), MagicMock()
        try:
            with patch("swarmq.dishka.logger") as logger:
                setup_dishka(c1)
                logger.warning.assert_not_called()
                setup_dishka(c2)
                logger.warning.assert_called_once()
                assert logger.warning.call_args.args == ("dishka_container_replaced",)
        finally:
            _reset_container()

    def test_same_container_does_not_warn(self):
        from unittest.mock import MagicMock, patch

        from swarmq.dishka import _reset_container, setup_dishka

        _reset_container()
        c1 = MagicMock()
        try:
            with patch("swarmq.dishka.logger") as logger:
                setup_dishka(c1)
                setup_dishka(c1)  # idempotent re-registration
                logger.warning.assert_not_called()
        finally:
            _reset_container()


# --- R18 (Plan 2026-09-03-001): wirkungslose FromDishka-Annotationen ---


def test_optional_fromdishka_annotation_is_recorded_and_rejected_at_start():
    """``FromDishka[X] | None = None`` ergibt keinen Key: heute lautlos ``None``,
    ab 0.5.0 ein ConfigurationError beim Worker-Start (AE8)."""
    pytest.importorskip("dishka")
    from dishka import FromDishka

    from swarmq.exceptions import ConfigurationError
    from swarmq.task import Task, TaskInfo

    class Db:
        pass

    class SilentTask(Task):
        task_info = TaskInfo(name="silent_task", queues=["q"])

        async def run(self, db: FromDishka[Db] | None = None) -> None:  # type: ignore[valid-type]
            pass

    assert SilentTask._dishka_ineffective_fromdishka == ["run.db"]  # type: ignore[attr-defined]
    assert not getattr(SilentTask, "_dishka_injected_keys", None)
    with pytest.raises(ConfigurationError, match="silent_task: run.db"):
        di.validate_registered_tasks([SilentTask])


def test_mixed_effective_and_ineffective_params_still_rejected():
    pytest.importorskip("dishka")
    from dishka import FromDishka

    from swarmq.exceptions import ConfigurationError
    from swarmq.task import Task, TaskInfo

    class Dep:
        pass

    class MixedTask(Task):
        task_info = TaskInfo(name="mixed_task", queues=["q"])

        async def pre_execution(self, dep: FromDishka[Dep], other: FromDishka[Dep] | None = None) -> None:  # type: ignore[valid-type]
            pass

    assert MixedTask._dishka_ineffective_fromdishka == ["pre_execution.other"]  # type: ignore[attr-defined]
    assert MixedTask._dishka_injected_keys  # type: ignore[attr-defined]
    with pytest.raises(ConfigurationError, match="mixed_task: pre_execution.other"):
        di.validate_registered_tasks([MixedTask])


def test_correct_fromdishka_annotation_records_nothing_ineffective():
    pytest.importorskip("dishka")
    from dishka import FromDishka

    from swarmq.task import Task, TaskInfo

    class Dep:
        pass

    class GoodTask(Task):
        task_info = TaskInfo(name="good_task", queues=["q"])

        async def run(self, dep: FromDishka[Dep]) -> None:  # type: ignore[valid-type]
            pass

    assert not hasattr(GoodTask, "_dishka_ineffective_fromdishka")


def test_ineffective_check_is_noop_without_dishka():
    class Marked:
        _dishka_ineffective_fromdishka = ("run.db",)

    with patch.object(di, "_HAVE_DISHKA", False):
        di.validate_registered_tasks([Marked])  # kein Fehler
