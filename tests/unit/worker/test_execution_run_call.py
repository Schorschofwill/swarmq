"""``TaskExecutor``: Fehler beim AUFRUF von ``run()`` (falsche Argumente, kein Coroutine)
duerfen die Pipeline nicht verlassen — sonst bleibt die Nachricht unbestaetigt haengen.

Altbefund aus dem ce-code-review 2026-09-04: ``task_instance.run(*ctx.args, **ctx.kwargs)``
stand ausserhalb des try, ein TypeError entkam ``_handle_message``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.middleware import MiddlewareChain
from swarmq.task import Task, TaskInfo
from swarmq.worker.execution import TaskExecutor
from swarmq.worker.gates import GateContext, GateDeps


class NeedsValue(Task):
    task_info = TaskInfo(name="needs_value_exec", queues=["q"])

    async def run(self, value: str) -> str:
        return value


class SyncRun(Task):
    task_info = TaskInfo(name="sync_run_exec", queues=["q"])

    def run(self) -> int:  # type: ignore[override]  # absichtlich kein Coroutine
        return 1


def _deps() -> GateDeps:
    config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", cancel_poll_interval=0.02)
    backend = MagicMock()
    backend.is_cancelled = AsyncMock(return_value=False)
    signals = MagicMock()
    signals.dispatch = AsyncMock()
    return GateDeps(
        broker=MagicMock(), backend=backend, config=config, signals=signals, registry=MagicMock(),
        rate_limiter=lambda: None, lock_manager=lambda: None, finalize_schedfire=AsyncMock(),
        maybe_release_unique=AsyncMock(), extract_valid_workflow_id=lambda _m: None,
        register_lock=lambda _h, _e: None,
    )


def _ctx(task_class: type, *args: Any, **kwargs: Any) -> GateContext:
    return GateContext(
        task_id="t1", task_name=task_class.task_info.name, queue="q", task_info=task_class.task_info,
        task_class=task_class, args=list(args), kwargs=kwargs,
    )


@pytest.mark.timeout(5)
async def test_missing_run_argument_is_an_error_result_not_an_escaping_exception():
    ex = TaskExecutor(_deps())
    result = await ex.execute(_ctx(NeedsValue), {"retry_count": 0}, MiddlewareChain([]))
    assert result.is_error and not result.is_cancelled
    assert isinstance(result.task_exception, TypeError)
    assert "value" in result.error_message


@pytest.mark.timeout(5)
async def test_unexpected_keyword_is_an_error_result():
    ex = TaskExecutor(_deps())
    result = await ex.execute(_ctx(NeedsValue, "ok", extra=1), {"retry_count": 0}, MiddlewareChain([]))
    assert result.is_error and isinstance(result.task_exception, TypeError)


@pytest.mark.timeout(5)
async def test_non_coroutine_run_is_an_error_result():
    ex = TaskExecutor(_deps())
    result = await ex.execute(_ctx(SyncRun), {"retry_count": 0}, MiddlewareChain([]))
    assert result.is_error and isinstance(result.task_exception, TypeError)
