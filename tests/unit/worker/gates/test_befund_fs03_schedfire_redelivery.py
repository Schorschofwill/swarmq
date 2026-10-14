"""Nachweis FS-03: Das Schedfire-Gate verwirft die Neuzustellung eines Fires,
dessen In-Flight-Claim einem toten Vorgaenger gehoert; ein am Semaphor
wartendes Fire hinterlaesst beim Abbruch seinen Claim.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-03; Quellen K-2, K-5).

K-2: ``SchedfireGate`` (``worker/gates/schedfire.py``) bestaetigt jede
Zustellung, deren ``claim_fire`` verliert, als Duplikat (Ack, kein Lauf). Stirbt
der Worker, der den Claim haelt (SIGKILL, OOM), stellt der Broker die einzige
Kopie neu zu (``x-delivery-count`` > 0); der Claim gehoert noch dem Toten, das
Gate bestaetigt sie -- das Fire ist still verloren. Erwartet: eine Broker-
Neuzustellung wird nicht als Duplikat bestaetigt, sondern zurueckgestellt (bis
der Claim ablaeuft) oder ausgefuehrt.

K-5: ``_process_message`` (``worker/pipeline.py``) nimmt den Semaphor nach den
Prae-Pipeline-Gates und ausserhalb des ``try/finally``. Bricht der Shutdown
einen Handler ab, der dort wartet, wird der Schedfire-Claim weder freigegeben
noch finalisiert; die Neuzustellung trifft dann auf K-2. Erwartet: der Claim
wird (owner-geprueft) freigegeben.

Testliste:
# [x] Broker-Neuzustellung, Claim gehoert dem toten Vorgaenger -> nicht als
#     Duplikat bestaetigt (ausgefuehrt oder zurueckgestellt)
# [x] Shutdown bricht ein am Semaphor wartendes Fire ab -> Claim freigegeben
"""

from __future__ import annotations

import asyncio
from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker, WorkerState
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_SLOW_STARTED = asyncio.Event()
_NEVER = asyncio.Event()


class Fs03CronJob(Task):
    task_info = TaskInfo(name="fs03_cron_job", queues=["default"])

    @override
    async def run(self) -> str:
        return "ok"


class Fs03SlowJob(Task):
    task_info = TaskInfo(name="fs03_slow_job", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        _SLOW_STARTED.set()
        await _NEVER.wait()


@pytest.mark.befund("FS-03")
@pytest.mark.xfail(strict=True, reason="FS-03: offen - Neuzustellung mit fremdem Claim wird als Duplikat bestaetigt")
async def test_redelivery_after_worker_death_is_not_acked_as_duplicate(
    broker: MagicMock, backend: MagicMock, config: SwarmQConfig
) -> None:
    registry = TaskRegistry()
    registry.register(Fs03CronJob)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    # Der Claim gehoert dem gestorbenen Vorgaenger (nie finalisiert).
    backend.schedules.claim_fire = AsyncMock(return_value=False)
    message = make_message(
        _default_payload(task_id="fs03-t1", name="fs03_cron_job", args=[], kwargs={}),
        headers={"x-swarmq-schedfire": "cron:fs03:1", "x-delivery-count": 1},
    )
    message.redelivered = True

    await worker._handle_message(message)

    ran = backend.store_result.await_count > 0
    put_back = any(verb.await_count > 0 for verb in (broker.redeliver, broker.retry_later, broker.nack))
    assert ran or put_back, (
        "Broker-Neuzustellung nach Worker-Tod als Duplikat bestaetigt "
        f"(ack={broker.ack.await_count}), Fire ohne Lauf verloren"
    )


@pytest.mark.befund("FS-03")
async def test_shutdown_cancel_while_waiting_for_slot_releases_the_fire_claim(
    broker: MagicMock, backend: MagicMock
) -> None:
    _SLOW_STARTED.clear()
    registry = TaskRegistry()
    registry.register(Fs03SlowJob)
    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=1,
        queues=["default"],
        shutdown_timeout=0,
    )
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._state = WorkerState.RUNNING
    fire_waits_for_slot = asyncio.Event()

    async def _is_cancelled(task_id: str, **_kwargs: object) -> bool:
        # Letzter Backend-Aufruf vor dem Semaphor: danach wartet das Fire am Slot.
        if task_id == "fs03-fire":
            fire_waits_for_slot.set()
        return False

    backend.is_cancelled = AsyncMock(side_effect=_is_cancelled)
    blocker = make_message(_default_payload(task_id="fs03-blocker", name="fs03_slow_job", args=[], kwargs={}))
    fire = make_message(
        _default_payload(task_id="fs03-fire", name="fs03_slow_job", args=[], kwargs={}),
        headers={"x-swarmq-schedfire": "cron:fs03:2"},
    )

    await worker._on_message(blocker)
    await _SLOW_STARTED.wait()
    await worker._on_message(fire)
    await fire_waits_for_slot.wait()
    fire_key = backend.schedules.claim_fire.await_args.args[0]

    worker._request_shutdown()
    await worker._execute_shutdown()

    released = [c.args[0] for c in backend.schedules.release_fire_claim.await_args_list]
    assert fire_key in released, f"Claim {fire_key!r} des abgebrochenen Fires bleibt stehen (freigegeben: {released})"
