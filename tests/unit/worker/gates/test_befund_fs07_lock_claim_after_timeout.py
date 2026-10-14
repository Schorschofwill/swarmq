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
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.contracts.coordination import LockToken
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
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
@pytest.mark.xfail(strict=True, reason="FS-07: offen - Lock nach Acquire-Timeout bleibt verwaist")
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
