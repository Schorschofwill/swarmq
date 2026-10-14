"""Schedfire-Neuzustellung mit fremdem Claim gegen echtes Valkey (FS-03, E-046, Plan U5).

Stirbt der Worker mit gewonnenem In-Flight-Claim, stellt der Broker die
Nachricht neu zu (``x-delivery-count`` > 0). Das Gate stellt sie zurueck,
statt sie als Duplikat zu bestaetigen; nach Ablauf des Claims laeuft sie genau
einmal, und weitere Kopien treffen den Completed-Marker. Laeuft das Original
noch (nur sein Kanal ist weg), laeuft die Neuzustellung nicht parallel: solange
der Claim lebt, stellt das Schedfire-Gate zurueck; ist er abgelaufen, haelt
der Ausfuehrungsschutz die Kopie zurueck (zweite Linie).

Broker ist ein Mock (Kopien werden aus den ``retry_later``-Headern gebaut),
Backend ist das echte ``ValkeyBackend``. Den Ablauf der Claim-TTL ersetzt das
Loeschen des In-Flight-Keys (keine Wartezeit im Test).

Test-Liste:
- [x] Claim mit Owner "W1", Neuzustellung mit Zaehler -> zurueckgestellt,
      kein Lauf; nach Claim-Ablauf laeuft die Kopie genau einmal, eine
      zweite Kopie (Doppelkopie nach ``PublishIndeterminate``) trifft den
      Completed-Marker und wird bestaetigt
- [x] Original laeuft, sein Claim lebt; Neuzustellung mit Zaehler -> das
      Schedfire-Gate stellt zurueck, kein Parallellauf; nach Abschluss des
      Originals wird die zurueckgestellte Kopie bestaetigt (der
      Ausfuehrungsschutz wird hier nicht erreicht)
- [x] Original laeuft, sein Claim ist abgelaufen (geloescht); Kopie mit
      Defer-Header gewinnt den Claim -> der Ausfuehrungsschutz stellt sie
      zurueck (``x-swarmq-guard-defer-count``), kein Parallellauf; nach
      Abschluss des Originals wird sie bestaetigt, ohne zu laufen
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_GUARD_DEFER_COUNT, HEADER_SCHEDFIRE_DEFER_COUNT
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]

RUNS: list[str] = []
# Optional: der Lauf meldet seinen Start und wartet auf die Freigabe.
HOLD: dict[str, asyncio.Event] = {}


class DeferFireTask(Task):
    task_info = TaskInfo(name="defer-fire-task", queues=["default"])

    @override
    async def run(self) -> str:
        RUNS.append("run")
        if HOLD:
            HOLD["started"].set()
            await HOLD["release"].wait()
        return "ok"


@pytest.fixture
async def backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    b = ValkeyBackend(valkey_url)
    await b.connect()
    try:
        yield b
    finally:
        await b.close()


@pytest.fixture
def broker() -> MagicMock:
    b = MagicMock(spec=RabbitMQBroker)
    for verb in ("ack", "nack", "redeliver", "retry_later", "dead_letter", "route_via_dlx"):
        setattr(b, verb, AsyncMock())
    return b


@pytest.fixture
def worker(broker: MagicMock, backend: ValkeyBackend) -> Worker:
    RUNS.clear()
    HOLD.clear()
    registry = TaskRegistry()
    registry.register(DeferFireTask)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", queues=["default"])
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


def _fire_key() -> str:
    return f"cron-{uuid.uuid4().hex[:8]}:1700000000000"


def _message(fire_key: str, task_id: str, extra: dict[str, Any] | None = None) -> MagicMock:
    payload = {"task": {"id": task_id, "queue": "default", "name": "defer-fire-task"}, "args": [], "kwargs": {}}
    return make_message(payload, headers={"x-swarmq-schedfire": fire_key, **(extra or {})})


def _take_copy_headers(broker: MagicMock) -> dict[str, Any]:
    broker.retry_later.assert_awaited_once()
    headers: dict[str, Any] = broker.retry_later.await_args.kwargs["headers"]
    broker.retry_later.reset_mock()
    return headers


async def test_redelivery_after_worker_death_runs_once_after_the_claim_expired(
    worker: Worker, broker: MagicMock, backend: ValkeyBackend
) -> None:
    fire_key = _fire_key()
    task_id = uuid.uuid4().hex
    # W1 hat den Claim gewonnen und ist gestorben (nie finalisiert).
    assert await backend.schedules.claim_fire(fire_key, 60_000, owner="W1") is True

    redelivered = _message(fire_key, task_id, {"x-delivery-count": 1})
    await worker._handle_message(redelivered)
    assert not RUNS
    broker.ack.assert_not_awaited()
    headers = _take_copy_headers(broker)
    assert headers[HEADER_SCHEDFIRE_DEFER_COUNT] == 1

    # Solange der Claim lebt, wird auch die Kopie zurueckgestellt.
    await worker._handle_message(make_message(body=redelivered.body, headers=headers))
    assert not RUNS
    headers = _take_copy_headers(broker)
    assert headers[HEADER_SCHEDFIRE_DEFER_COUNT] == 2

    await backend.schedules.clear_schedfire_inflight(fire_key)  # Claim-TTL abgelaufen
    await worker._handle_message(make_message(body=redelivered.body, headers=headers))
    assert RUNS == ["run"]
    assert await backend.schedules.is_fire_completed(fire_key) is True

    twin = make_message(body=redelivered.body, headers=headers)
    await worker._handle_message(twin)
    assert RUNS == ["run"]
    broker.ack.assert_any_await(twin)


async def test_redelivery_while_the_original_runs_does_not_run_in_parallel(
    worker: Worker, broker: MagicMock, backend: ValkeyBackend
) -> None:
    fire_key = _fire_key()
    task_id = uuid.uuid4().hex
    started, release = asyncio.Event(), asyncio.Event()
    HOLD.update(started=started, release=release)
    original = asyncio.create_task(worker._handle_message(_message(fire_key, task_id)))
    await asyncio.wait_for(started.wait(), timeout=5)

    # Kanal des Originals gerissen: der Broker stellt dieselbe Nachricht neu zu.
    redelivered = _message(fire_key, task_id, {"x-delivery-count": 1})
    await worker._handle_message(redelivered)
    assert RUNS == ["run"]
    headers = _take_copy_headers(broker)

    release.set()
    await original
    copy = make_message(body=redelivered.body, headers=headers)
    await worker._handle_message(copy)
    assert RUNS == ["run"]
    broker.ack.assert_any_await(copy)


async def test_copy_winning_an_expired_claim_is_held_back_by_the_execution_guard(
    worker: Worker, broker: MagicMock, backend: ValkeyBackend
) -> None:
    """Task ohne ``hard_timeout``, Lauf laenger als ``scheduler_inflight_ttl``:
    die zurueckgestellte Kopie gewinnt den abgelaufenen Claim. Den zweiten
    Lauf verhindert dann nur der Ausfuehrungsschutz (Default an)."""
    # Den Store setzt sonst ``start()``; ohne ihn ist der Schutz aus.
    worker._guard_store = backend.execution_guard
    fire_key = _fire_key()
    task_id = uuid.uuid4().hex
    started, release = asyncio.Event(), asyncio.Event()
    HOLD.update(started=started, release=release)
    original = asyncio.create_task(worker._handle_message(_message(fire_key, task_id)))
    await asyncio.wait_for(started.wait(), timeout=5)

    await backend.schedules.clear_schedfire_inflight(fire_key)  # Claim-TTL abgelaufen, Original laeuft
    deferred_copy = _message(fire_key, task_id, {HEADER_SCHEDFIRE_DEFER_COUNT: 3})
    await worker._handle_message(deferred_copy)
    assert RUNS == ["run"]
    broker.ack.assert_not_awaited()
    headers = _take_copy_headers(broker)
    assert headers[HEADER_GUARD_DEFER_COUNT] == 1

    release.set()
    await original
    copy = make_message(body=deferred_copy.body, headers=headers)
    await worker._handle_message(copy)
    assert RUNS == ["run"]
    broker.ack.assert_any_await(copy)
