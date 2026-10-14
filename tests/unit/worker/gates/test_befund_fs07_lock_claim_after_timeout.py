"""Nachweis FS-07 (Lock-Gate): Ein Timeout nach erfolgreichem Lock-Acquire gibt
den Lock nicht frei.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-07; Quelle K-4).

``LockGate._acquire_and_register`` (``worker/gates/lock.py``) begrenzt
``acquire`` mit ``bounded_backend_call`` auf ``backend_op_timeout``. Stockt
Valkey ueber diese Frist und fuehrt das Skript trotzdem aus, haelt
``owner_token`` den Lock; das Gate stellt die Zustellung zurueck, gibt aber
nichts frei. Mutex bzw. Semaphor-Slot bleiben bis ``hard_timeout + 30 s``
verwaist, die eigene Kopie trifft auf den eigenen Lock. Erwartet (wie beim
Ausfuehrungsschutz): nach der Zeitueberschreitung wird der Token (im
Hintergrund, owner-geprueft) freigegeben.

Der Scheduler-Teil (Unique-Claim des Leaders) steht in
``tests/unit/scheduler/test_befund_fs07_unique_claim_after_timeout.py``.

Testliste:
# [x] Acquire schreibt den Lock, antwortet aber nach ``backend_op_timeout``
#     -> zurueckgestellt und der geschriebene Token wird freigegeben
# [x] Shutdown wartet auf die Hintergrund-Freigabe, bevor er das Backend schliesst
# [x] Freigabe trifft nie einen fremden Lock (nur der Token dieses Versuchs)
# [x] Freigabe scheitert -> ``lock_release_failed`` (stage ``acquire_timeout``),
#     die Zurueckstellung bleibt
# [x] Fehlerantwort (kein Ausfall, z. B. WRONGTYPE) -> keine Freigabe
# [x] ohne ``GateDeps.detach`` (eigene Verdrahtung) -> Freigabe inline
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import uuid
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.contracts.coordination import LockToken
from swarmq.exceptions import BackendConnectionError, BackendResponseError
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker import lifecycle as _lc
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.lock import LockGate
from swarmq.worker.lifecycle import WorkerState
from swarmq.worker.outcome import Redelivered
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

# Ein Freigabe-Task im Hintergrund braucht mit gemocktem Store nur wenige
# Schleifendurchlaeufe; 50 ms sind grosszuegig und halten den Test unter 100 ms.
_RELEASE_GRACE_S = 0.05


class Fs07LockedTask(Task):
    task_info = TaskInfo(name="fs07_locked", queues=["default"], lock="fs07-resource", hard_timeout=60)

    @override
    async def run(self) -> None:
        return None


def _ctx() -> GateContext:
    return GateContext(
        task_id="fs07-t1",
        task_name=Fs07LockedTask.task_info.name,
        queue="default",
        task_info=Fs07LockedTask.task_info,
        task_class=Fs07LockedTask,
        args=[],
        kwargs={},
        handler_id=uuid.uuid4(),
    )


@pytest.mark.befund("FS-07")
async def test_lock_written_before_acquire_timeout_is_released(worker: Worker, broker: MagicMock) -> None:
    worker._gate_deps.config.backend_op_timeout = 0.01
    held: dict[str, LockToken] = {}
    released = asyncio.Event()
    never = asyncio.Event()

    async def _acquire(name: str, *, owner: str, ttl_ms: int, max_concurrency: int) -> LockToken:
        # Valkey fuehrt das Skript aus, die Antwort kommt zu spaet.
        held[owner] = LockToken(name=name, owner=owner, max_concurrency=max_concurrency)
        await never.wait()
        raise AssertionError("unerreichbar")

    async def _release(token: LockToken) -> bool:
        held.pop(token.owner, None)
        released.set()
        return True

    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = _acquire
    worker._lock_manager.release = AsyncMock(side_effect=_release)

    outcome = await worker._lock_gate.apply(make_message(_default_payload()), _ctx())
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(released.wait(), timeout=_RELEASE_GRACE_S)

    assert outcome is Redelivered, "Voraussetzung: Timeout stellt die Zustellung zurueck"
    assert held == {}, f"Lock nach Acquire-Timeout nicht freigegeben, verwaist bis hard_timeout + 30 s: {list(held)}"


class _LockStore:
    """Owner-gepruefter Lock-Store; ``acquire`` schreibt den Lock und meldet
    danach *acquire_error* (verlorene Antwort), ``release`` laesst sich per
    *release_gate* aufhalten."""

    def __init__(
        self,
        acquire_error: Exception,
        release_gate: asyncio.Event | None = None,
        release_error: Exception | None = None,
    ) -> None:
        self.held: dict[str, str] = {}
        self.events: list[str] = []
        self._acquire_error = acquire_error
        self._release_gate = release_gate
        self._release_error = release_error

    async def acquire(self, name: str, *, owner: str, ttl_ms: int, max_concurrency: int) -> LockToken:
        self.held.setdefault(name, owner)
        raise self._acquire_error

    async def release(self, token: LockToken) -> bool:
        if self._release_gate is not None:
            await self._release_gate.wait()
        self.events.append("release")
        if self._release_error is not None:
            raise self._release_error
        if self.held.get(token.name) != token.owner:
            return False
        del self.held[token.name]
        return True


def _install(worker: Worker, store: _LockStore) -> None:
    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = store.acquire
    worker._lock_manager.release = store.release


@pytest.mark.befund("FS-07")
async def test_shutdown_waits_for_the_background_release_before_closing_the_backend(
    worker: Worker, backend: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_lc, "_SHUTDOWN_STEP_MIN_S", 0.01)
    gate = asyncio.Event()
    store = _LockStore(BackendConnectionError("lock acquire timed out"), release_gate=gate)
    _install(worker, store)
    backend.close.side_effect = lambda: store.events.append("backend_close")

    @worker._signal_dispatcher.on_signal(Signal.WORKER_SHUTDOWN)
    async def _open_gate(**_kwargs: Any) -> None:
        gate.set()  # die Freigabe laeuft erst, wenn der Shutdown begonnen hat

    outcome = await worker._lock_gate.apply(make_message(_default_payload()), _ctx())
    pending = set(worker._detached_releases)  # die Freigabe haengt am Gate
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()
    await asyncio.wait_for(worker._execute_shutdown(), 1.0)

    assert outcome is Redelivered
    assert len(pending) == 1, "Hintergrund-Freigabe nicht fuer den Shutdown getrackt"
    assert store.events == ["release", "backend_close"]
    assert store.held == {}


@pytest.mark.befund("FS-07")
async def test_release_after_timeout_never_touches_a_foreign_lock(worker: Worker) -> None:
    """Haelt ein anderer Worker den Lock schon, schrieb das Acquire nichts; die
    Freigabe mit dem Token dieses Versuchs laesst den fremden Lock stehen."""
    store = _LockStore(BackendConnectionError("lock acquire timed out"))
    store.held["fs07-resource"] = "foreign-owner"
    _install(worker, store)

    outcome = await worker._lock_gate.apply(make_message(_default_payload()), _ctx())
    await asyncio.gather(*worker._detached_releases)

    assert outcome is Redelivered
    assert store.events == ["release"]
    assert store.held == {"fs07-resource": "foreign-owner"}


@pytest.mark.befund("FS-07")
async def test_failing_release_after_timeout_is_logged_and_still_defers(
    worker: Worker, swarmq_logs: list[dict[str, Any]]
) -> None:
    store = _LockStore(BackendConnectionError("lock acquire timed out"), release_error=BackendConnectionError("down"))
    _install(worker, store)

    outcome = await worker._lock_gate.apply(make_message(_default_payload()), _ctx())
    await asyncio.gather(*worker._detached_releases)

    assert outcome is Redelivered
    [entry] = [e for e in swarmq_logs if e["event"] == "lock_release_failed"]
    assert entry["lock_name"] == "fs07-resource"
    assert entry["stage"] == "acquire_timeout"
    assert entry["error_type"] == "BackendConnectionError"


@pytest.mark.befund("FS-07")
async def test_error_reply_of_the_acquire_releases_nothing(worker: Worker) -> None:
    """Eine Fehlerantwort heisst: das Skript lief nicht; es gibt nichts
    freizugeben (der Fall geht wie bisher per Requeue zurueck)."""
    store = _LockStore(BackendResponseError("WRONGTYPE Operation against a key"))
    _install(worker, store)

    outcome = await worker._lock_gate.apply(make_message(_default_payload()), _ctx())
    await asyncio.gather(*worker._detached_releases)

    assert outcome is Redelivered
    assert store.events == []


@pytest.mark.befund("FS-07")
async def test_without_detach_the_release_runs_inline(worker: Worker) -> None:
    store = _LockStore(BackendConnectionError("lock acquire timed out"))
    _install(worker, store)
    gate = LockGate(dataclasses.replace(worker._gate_deps, detach=None), dead_letter=AsyncMock())

    outcome = await gate.apply(make_message(_default_payload()), _ctx())

    assert outcome is Redelivered
    assert store.events == ["release"]
    assert store.held == {}
