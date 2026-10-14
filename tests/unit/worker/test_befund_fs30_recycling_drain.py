"""Nachweis FS-30: Ein Recycling-Drain ohne registrierten
``hard_timeout`` faellt auf ``shutdown_timeout`` zurueck und bricht laufende
Langlaeufer ab.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-30, abgetrennt von FS-01; Quellen MB-2, N-5).

``recycle_drain_timeout`` (``worker/recycling.py``) liefert ohne einen Task mit
``hard_timeout`` (Default ``None``) nur ``shutdown_timeout``. ``_drain_in_flight``
bricht danach jeden noch laufenden Handler ab – nach E-001 (KTD2) wird daraus
ein Fehler mit Retry-Verbrauch bzw. DLQ. R22/KTD11 (Plan 2026-09-03-001) und
``website/explanation/delivery-semantics.md`` versprechen dagegen: Das Recycling
drainiert "ohne erzwungenen Abbruch", "verbraucht keinen Retry eines laufenden
Tasks"; der erzwungene Abbruch bleibt SIGTERM vorbehalten.

Gepruefte Erwartung (Verhalten, keine Fristformel): Ein Task ohne
``hard_timeout``, der laenger laeuft als ``shutdown_timeout``, wird beim
Recycling nicht abgebrochen, sondern darf zu Ende laufen und wird bestaetigt.

Laufzeit: Der Nachweis "wird NICHT abgebrochen" braucht ein Zeitfenster
deutlich ueber ``shutdown_timeout`` (hier 0 s): der Test wartet bis zu 0,2 s auf
einen Abbruch. Ein solches Fenster laesst sich nicht durch ein Event ersetzen,
weil das korrekte Verhalten gerade das Ausbleiben eines Ereignisses ist. Heute
(Fehlerfall) endet der Test nach rund 0,03 s.

Hinweis fuer den Fix: ``test_recycling.py::test_task_count_threshold_recycles_with_ack``
schreibt den Rueckfall ``_drain_timeout_override == shutdown_timeout`` fest und
muss dann angepasst werden.

Testliste:
# [x] Registry nur mit Task ohne hard_timeout: Recycling-Drain bricht einen
#     Langlaeufer nach shutdown_timeout nicht ab; er endet regulaer (ack)
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, override

import pytest

from swarmq import supervision as sv
from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker, WorkerState, recycling
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_SHUTDOWN_TIMEOUT_S = 0
_ABORT_WINDOW_S = 0.2


class _Fs01RecyclingProbe:
    """Ereignisse je Testlauf (im laufenden Loop angelegt)."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()


_PROBES: list[_Fs01RecyclingProbe] = []  # je Test genau eine; Task-Klassen sind modulweit


class Fs01RecyclingLongRunner(Task):
    """Ohne ``hard_timeout`` (Default); laeuft, bis der Test ihn freigibt."""

    task_info = TaskInfo(name="fs01_recycling_long_runner", queues=["default"])

    @override
    async def run(self) -> str:
        probe = _PROBES[-1]
        probe.started.set()
        try:
            await probe.release.wait()
        except asyncio.CancelledError:
            probe.cancelled.set()
            raise
        return "fertig"


@pytest.mark.befund("FS-30")
@pytest.mark.xfail(
    strict=True, reason="FS-30: offen - Recycling-Drain ohne hard_timeout bricht nach shutdown_timeout ab"
)
async def test_recycling_drain_does_not_abort_a_long_runner_without_hard_timeout(broker: Any, backend: Any) -> None:
    probe = _Fs01RecyclingProbe()
    _PROBES[:] = [probe]
    sv.set_current_channel(None)  # eingebettet: Recycling ohne Supervisor-Bestaetigung
    registry = TaskRegistry()
    registry.register(Fs01RecyclingLongRunner)
    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=2,
        queues=["default"],
        shutdown_timeout=_SHUTDOWN_TIMEOUT_S,
        recycle_max_tasks=1,
    )
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._state = WorkerState.RUNNING
    worker._tasks_done = 1  # Recycling-Schwelle erreicht (sonst setzt der Monitor das)
    message = make_message(
        _default_payload(task_id="fs01-rec-t1", name="fs01_recycling_long_runner", args=[], kwargs={})
    )

    await worker._on_message(message)
    await probe.started.wait()
    await recycling.request_recycle(worker, "tasks", 1.0)
    shutdown = asyncio.create_task(worker._execute_shutdown())
    try:
        # Wartet auf das Ausbleiben eines Abbruchs: ein Event allein reicht nicht.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(probe.cancelled.wait(), timeout=_ABORT_WINDOW_S)
        aborted = probe.cancelled.is_set()
    finally:
        probe.release.set()
        await shutdown

    assert not aborted, "Recycling-Drain bricht den Langlaeufer ohne hard_timeout nach shutdown_timeout ab"
    broker.ack.assert_awaited_once()
    assert broker.retry_later.await_count == 0
    assert broker.dead_letter.await_count == 0
