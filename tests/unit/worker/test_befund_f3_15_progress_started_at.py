"""Nachweis F3-15: ``ProgressInfo.started_at`` stammt aus ``time.monotonic()``
und erscheint als Datum um 1970.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-15, R3D-10).

Der Worker (``execution.py``) und das Rechenkind (``compute/child.py``) setzen
``_task_start_time = time.monotonic()``; ``Task.update_progress`` macht daraus
mit ``datetime.fromtimestamp(..., tz=UTC)`` das ``started_at`` -- die
Sekunden seit Systemstart als Unix-Zeit, also ein Datum kurz nach 1970. Der
Progress-Plan (2026-04-17-006, :188) sieht ``time.time()`` vor.

Erwartet (Empfehlung Register, Punkt 12): zwei Zeitwerte -- die Wanduhr fuer
``started_at``, die monotone Uhr fuer ``elapsed``/``eta_seconds``. Gilt in
beiden Pfaden (IO-Worker und CPU-Kind). Der Doku-Teil (``api.md`` nennt
``datetime`` statt ``str``) ist nicht Gegenstand dieses Tests.

Testliste:
# [x] IO-Pfad: started_at liegt nahe now(UTC), eta_seconds klein und >= 0
# [x] CPU-Kind: dasselbe ueber den Rueckkanal-Shim
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.compute import child
from swarmq.worker.compute.envelope import ComputeJob
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

# Grosszuegig: der Test laeuft in Millisekunden, die Uhrzeit von 1970 liegt
# Jahrzehnte daneben.
_TOLERANCE = timedelta(minutes=5)


def _assert_progress_fields(fields: dict[str, str], before: datetime) -> None:
    started_at = datetime.fromisoformat(fields["started_at"])
    after = datetime.now(UTC)
    assert before - _TOLERANCE <= started_at <= after + _TOLERANCE, (
        f"started_at={fields['started_at']} liegt nicht beim Start ({before.isoformat()})"
    )
    eta = int(fields["eta_seconds"])
    assert 0 <= eta <= _TOLERANCE.total_seconds(), f"eta_seconds={eta} nicht aus der monotonen Laufzeit"


class F315ProgressTask(Task):
    task_info = TaskInfo(name="f315_progress_io", queues=["default"])

    @override
    async def run(self) -> None:
        await self.update_progress(1, 2, "half")


@pytest.mark.befund("F3-15")
@pytest.mark.entscheidung("E-044")
async def test_io_progress_started_at_is_wall_clock(
    broker: MagicMock, backend: MagicMock, config: SwarmQConfig
) -> None:
    set_progress = AsyncMock()
    backend.set_progress = set_progress
    backend.publish_progress = AsyncMock()
    registry = TaskRegistry()
    registry.register(F315ProgressTask)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    message = make_message(_default_payload(task_id="f315-io", name="f315_progress_io", args=[], kwargs={}))
    before = datetime.now(UTC)

    await worker._handle_message(message)

    set_progress.assert_awaited_once()
    assert set_progress.await_args is not None
    fields: dict[str, str] = set_progress.await_args.args[1]
    _assert_progress_fields(fields, before)


@pytest.fixture
def _clean_task_registry() -> Iterator[None]:
    saved = list(Task._registered_tasks)
    Task._clear_registered_tasks()
    yield
    Task._clear_registered_tasks()
    Task._registered_tasks.extend(saved)


class _Channel:
    def __init__(self) -> None:
        self.sent: list[tuple[Any, ...]] = []

    def send(self, msg: tuple[Any, ...]) -> None:
        self.sent.append(msg)


@pytest.mark.befund("F3-15")
@pytest.mark.entscheidung("E-044")
@pytest.mark.usefixtures("_clean_task_registry")
async def test_cpu_child_progress_started_at_is_wall_clock() -> None:
    class F315ProgressCpu(Task):
        task_info = TaskInfo(name="f315_progress_cpu", queues=["q"])

        @override
        async def run(self) -> None:
            await self.update_progress(1, 2, "half")

    channel = _Channel()
    before = datetime.now(UTC)

    envelope = await child.run_job(
        ComputeJob(task_id="f315-cpu", task_name=F315ProgressCpu.task_info.name, queue="q", args=(), kwargs={}), channel
    )

    assert envelope.ok
    progress = [msg for msg in channel.sent if msg[0] == "progress"]
    assert len(progress) == 1
    _assert_progress_fields(progress[0][2], before)
