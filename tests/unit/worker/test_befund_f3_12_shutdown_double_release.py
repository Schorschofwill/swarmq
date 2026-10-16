"""Nachweis F3-12: Doppelte Lock-Freigabe mit Fehlalarm ``task_lock_lost``,
wenn der Shutdown zuerst abraeumt und der Handler danach seinen ``finally``
erreicht.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-12, R3A-01).

Schritt 3 des Drains bricht die Handler ab und wartet hoechstens
``min(1.0, step_timeout)``. Braucht ein Handler mit ``lock=`` danach laenger
(Aufraeumen im Task, langsamer Fehlerpfad, Kill eines Rechenkinds), nimmt
``_release_leftover_locks`` seinen Registry-Eintrag und gibt den Lock frei.
Erreicht der Handler spaeter seinen ``finally``, gibt
``release_acquired_locks`` die lokale Liste trotzdem frei: derselbe Token
wird ein zweites Mal freigegeben, ``release`` liefert ``False`` und es folgt
WARNING ``task_lock_lost`` ("a second holder may have run in parallel"). Der
Nachbarpfad ``_release_after_teardown`` ist seit f21459f behoben (KTD6 Plan
Runde 2: "kein ``task_lock_lost``").

Ablauf per Event gesteuert, kein ``sleep``: Der Task faengt den Abbruch ab
und raeumt auf, bis der Test ``cleanup_may_finish`` setzt -- also erst nach
dem kompletten Shutdown. Das Mindestbudget je Schritt ist wie in
``test_pipeline_finally.py`` auf 0,01 s gesetzt; am Ablauf aendert das
nichts (Gegenpruefung G1: mit der Default-Untergrenze 1,0 s und > 1 s
Aufraeumzeit dasselbe Ergebnis), es erspart dem Unit-Test nur die Sekunde.

Erwartet: genau eine Freigabe je Token und kein ``task_lock_lost``. Behoben
in 0.12.3: ``release_acquired_locks`` gibt nur frei, wenn es den
Registry-Eintrag selbst entnommen hat. Ob der
Shutdown den Lock eines noch laufenden Handlers ueberhaupt freigeben soll,
ist die offene Produktfrage F3-32 und wird hier bewusst NICHT geprueft.

Testliste:
# [x] Handler raeumt nach dem Abbruch laenger auf als der Drain wartet ->
#     je Token genau eine release, kein task_lock_lost
"""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any, override
from unittest.mock import MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import LockToken
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker, lifecycle
from tests.support.inmemory.backend import InMemoryLockStore
from tests.support.inmemory.clock import FakeClock
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_LOCK = "f312-res"


class _Gates:
    """Events fuer den Ablauf; je Test neu gesetzt."""

    started: asyncio.Event
    cleanup_started: asyncio.Event
    cleanup_may_finish: asyncio.Event


class F312SlowCleanupTask(Task):
    task_info = TaskInfo(name="f312_slow_cleanup", queues=["default"], lock=_LOCK, hard_timeout=60)

    @override
    async def run(self) -> None:
        _Gates.started.set()
        try:
            await asyncio.Event().wait()  # laeuft, bis der Drain abbricht
        except asyncio.CancelledError:
            # Aufraeumen im Task (Rollback o. ae.), laenger als der Drain wartet.
            _Gates.cleanup_started.set()
            await _Gates.cleanup_may_finish.wait()
            raise


class _CountingLockStore(InMemoryLockStore):
    def __init__(self) -> None:
        super().__init__(FakeClock())
        self.releases: list[str] = []

    @override
    async def release(self, token: LockToken) -> bool:
        self.releases.append(token.owner)
        return await super().release(token)


@pytest.mark.befund("F3-12")
async def test_shutdown_then_handler_finally_releases_each_token_once(
    broker: MagicMock, backend: MagicMock, swarmq_logs: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(lifecycle, "_SHUTDOWN_STEP_MIN_S", 0.01)
    _Gates.started = asyncio.Event()
    _Gates.cleanup_started = asyncio.Event()
    _Gates.cleanup_may_finish = asyncio.Event()
    registry = TaskRegistry()
    registry.register(F312SlowCleanupTask)
    config = SwarmQConfig(
        broker_url="amqp://localhost", backend_url="valkey://localhost", concurrency=1, queues=["default"]
    )
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    store = _CountingLockStore()
    worker._lock_manager = store
    worker._config.shutdown_timeout = 0
    message = make_message(_default_payload(task_id="f312-task", name="f312_slow_cleanup", args=[], kwargs={}))

    await worker._on_message(message)
    await _Gates.started.wait()
    worker._state = lifecycle.WorkerState.RUNNING
    worker._request_shutdown()
    await worker._execute_shutdown()
    assert _Gates.cleanup_started.is_set(), "Voraussetzung: der Drain hat den Handler abgebrochen"
    _Gates.cleanup_may_finish.set()
    await asyncio.gather(*list(worker._active_tasks), return_exceptions=True)
    while worker._detached_releases:
        await asyncio.gather(*worker._detached_releases, return_exceptions=True)

    per_token = Counter(store.releases)
    assert per_token, "Voraussetzung: der Lock wurde freigegeben"
    assert all(n == 1 for n in per_token.values()), f"Token mehrfach freigegeben: {dict(per_token)}"
    lost = [e for e in swarmq_logs if e["event"] == "task_lock_lost"]
    assert not lost, f"Fehlalarm task_lock_lost: {lost}"
