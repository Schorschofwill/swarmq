"""Scheduler je Worker abschaltbar (Plan 2026-10-12-1105, MR 5, P1, T2/T3).

Test-Liste:
8. ``scheduler="off"`` -> ``SchedulerLoop`` mit ``candidate=False`` und
   WARNING ``scheduler_disabled`` genau einmal mit ``hint``.
9. ``scheduler="on"`` -> ``candidate=True``, kein ``scheduler_disabled``.
9a. Eingebettet: ``SwarmQ(..., scheduler="off").start_worker()`` ->
    ``SchedulerLoop`` mit ``candidate=False``.

Grenze: ``SchedulerLoop`` ist durch einen Mock ersetzt; ``start()`` bricht
nach dem Verdrahten an ``declare_queue`` ab (wie in ``test_lifecycle.py``).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.worker import Worker

pytestmark = [pytest.mark.timeout(5)]


@pytest.fixture
def scheduler_cls(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    from swarmq.worker import lifecycle

    cls = MagicMock()
    instance = cls.return_value
    instance.start = AsyncMock()
    instance.stop = AsyncMock()
    instance.abort = AsyncMock()
    monkeypatch.setattr(lifecycle, "SchedulerLoop", cls)
    return cls


def _config(scheduler: str) -> SwarmQConfig:
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        queues=["default"],
        scheduler=scheduler,
    )


async def _start_until_wired(worker: Worker, broker: Any) -> None:
    broker.declare_queue = AsyncMock(side_effect=RuntimeError("stop after wiring"))
    with pytest.raises(RuntimeError, match="stop after wiring"):
        await worker.start()


async def test_scheduler_off_builds_an_observer_and_warns_once(broker, backend, registry, scheduler_cls, swarmq_logs):
    worker = Worker(broker=broker, backend=backend, registry=registry, config=_config("off"))

    await _start_until_wired(worker, broker)

    assert scheduler_cls.call_args.kwargs["candidate"] is False
    disabled = [e for e in swarmq_logs if e["event"] == "scheduler_disabled"]
    assert len(disabled) == 1
    assert disabled[0]["log_level"] == "warning"
    assert disabled[0]["logger"] == "swarmq.worker.lifecycle"
    assert "leader" in disabled[0]["hint"]


async def test_scheduler_on_builds_a_candidate_without_warning(broker, backend, registry, scheduler_cls, swarmq_logs):
    worker = Worker(broker=broker, backend=backend, registry=registry, config=_config("on"))

    await _start_until_wired(worker, broker)

    assert scheduler_cls.call_args.kwargs["candidate"] is True
    assert [e for e in swarmq_logs if e["event"] == "scheduler_disabled"] == []


async def test_embedded_app_with_scheduler_off_builds_an_observer(broker, backend, scheduler_cls):
    from swarmq.app import SwarmQ

    broker._url = "amqp://localhost"
    backend._url = "valkey://localhost"
    app = SwarmQ(broker, backend, queues=["default"], scheduler="off")
    broker.declare_queue = AsyncMock(side_effect=RuntimeError("stop after wiring"))

    with pytest.raises(RuntimeError, match="stop after wiring"):
        await app.start_worker()

    assert scheduler_cls.call_args.kwargs["candidate"] is False
