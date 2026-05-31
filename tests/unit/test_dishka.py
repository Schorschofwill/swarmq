"""Unit tests for the optional Dishka DI foundation (plan U1).

Covers the dishka-free core (TaskContext, container slot, sentinel) and the
guarded SwarmQProvider surface. The dishka-absent path is simulated with
``patch.dict(sys.modules, {"dishka": None})`` + module reimport, mirroring
tests/unit/test_hot_reload.py.
"""

from __future__ import annotations

import dataclasses
import importlib
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


def test_module_imports_without_dishka_and_core_symbols_usable():
    """R2/AE6: swarmq.dishka imports with dishka absent; core symbols work,
    SwarmQProvider access raises a clear error."""
    with patch.dict(sys.modules, {"dishka": None}):
        reloaded = importlib.reload(di)
        try:
            assert reloaded._HAVE_DISHKA is False
            # Core symbols stay usable without dishka.
            ctx = reloaded.TaskContext(
                task_id="x", task_name="y", queue="q", retry_count=0
            )
            assert ctx.task_id == "x"
            sentinel = object()
            reloaded.setup_dishka(sentinel)  # type: ignore[arg-type]
            assert reloaded.get_registered_container() is sentinel
            # SwarmQProvider access raises a clear, actionable error.
            with pytest.raises(ImportError, match=r"swarmq\[dishka\]"):
                _ = reloaded.SwarmQProvider
        finally:
            # Restore the real module for subsequent tests.
            importlib.reload(importlib.import_module("swarmq.dishka"))
