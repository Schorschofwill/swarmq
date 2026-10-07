"""Schedfire-Claim gegen echtes Valkey: die eigene Kopie uebernimmt ihn.

Faellt Valkey zwischen gewonnenem ``claim_fire`` und einem spaeteren Gate aus,
stellt der Worker die Nachricht zurueck -- und auch die Freigabe des Claims
scheitert. Frueher sah die Kopie den stehengebliebenen Claim als fremden Lauf
und bestaetigte sich als Duplikat: der Fire ging verloren (bis
``scheduler_inflight_ttl``). Jetzt traegt die Kopie das Owner-Token
(``x-swarmq-schedfire-claim``) und uebernimmt den Claim; echte Duplikate
(ohne bzw. mit altem Token) werden weiter verworfen.

Broker ist ein Mock (die Kopie wird aus den ``retry_later``-Headern gebaut),
Backend ist das echte ``ValkeyBackend``; den Ausfall simulieren gepatchte
Methoden.

Test-Liste:
- [x] Cancel-Check und Freigabe scheitern -> die zurueckgestellte Kopie laeuft
      genau einmal, der Fire ist danach abgeschlossen
- [x] waehrend der Claim steht, bleibt eine Zustellung ohne Token (Stale-
      Leader-Kopie, Broker-Redelivery des Originals) ein Duplikat
- [x] nach der Uebernahme bleibt eine Zustellung mit dem alten Token
      (Broker-Redelivery der Kopie) ein Duplikat
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.headers import HEADER_SCHEDFIRE_CLAIM
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]

RUNS: list[str] = []
# Optional: der Lauf meldet seinen Start und wartet auf die Freigabe.
HOLD: dict[str, asyncio.Event] = {}


class FireTask(Task):
    task_info = TaskInfo(name="fire-task", queues=["default"])

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
    registry.register(FireTask)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", queues=["default"])
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


def _message(fire_key: str, extra: dict[str, Any] | None = None) -> MagicMock:
    payload = {"task": {"id": uuid.uuid4().hex, "queue": "default", "name": "fire-task"}, "args": [], "kwargs": {}}
    return make_message(payload, headers={"x-swarmq-schedfire": fire_key, **(extra or {})})


async def _defer_with_failed_release(worker: Worker, broker: MagicMock, backend: ValkeyBackend, msg: Any) -> dict:
    """Cancel-Check und Claim-Freigabe scheitern wie bei einem Valkey-Ausfall."""
    store = backend.schedules
    real_is_cancelled, real_release = backend.is_cancelled, store.release_fire_claim
    backend.is_cancelled = AsyncMock(side_effect=BackendConnectionError("valkey down"))  # type: ignore[method-assign]
    store.release_fire_claim = AsyncMock(side_effect=BackendConnectionError("valkey down"))  # type: ignore[method-assign]
    try:
        await worker._handle_message(msg)
    finally:
        backend.is_cancelled = real_is_cancelled  # type: ignore[method-assign]
        store.release_fire_claim = real_release  # type: ignore[method-assign]
    broker.retry_later.assert_awaited_once()
    headers: dict = broker.retry_later.await_args.kwargs["headers"]
    broker.retry_later.reset_mock()
    return headers


def _copy_of(original: Any, headers: dict) -> MagicMock:
    return make_message(body=original.body, headers=headers)


async def test_deferred_copy_takes_over_its_own_claim(
    worker: Worker, broker: MagicMock, backend: ValkeyBackend,
) -> None:
    fire_key = f"cron-{uuid.uuid4().hex[:8]}:1700000000000"
    original = _message(fire_key)
    headers = await _defer_with_failed_release(worker, broker, backend, original)
    assert not RUNS

    copy = _copy_of(original, headers)
    await worker._handle_message(copy)

    assert RUNS == ["run"], "die eigene Kopie wurde als Duplikat verworfen -- Fire verloren"
    assert await backend.schedules.is_fire_completed(fire_key) is True


async def test_delivery_without_token_stays_a_duplicate(
    worker: Worker, broker: MagicMock, backend: ValkeyBackend,
) -> None:
    fire_key = f"cron-{uuid.uuid4().hex[:8]}:1700000000000"
    original = _message(fire_key)
    headers = await _defer_with_failed_release(worker, broker, backend, original)

    stale = _copy_of(original, {"x-swarmq-schedfire": fire_key})
    await worker._handle_message(stale)
    assert not RUNS
    broker.ack.assert_awaited_once_with(stale)

    await worker._handle_message(_copy_of(original, headers))
    assert RUNS == ["run"]


async def test_redelivery_of_the_copy_after_takeover_stays_a_duplicate(
    worker: Worker, broker: MagicMock, backend: ValkeyBackend,
) -> None:
    fire_key = f"cron-{uuid.uuid4().hex[:8]}:1700000000000"
    original = _message(fire_key)
    headers = await _defer_with_failed_release(worker, broker, backend, original)

    started, release = asyncio.Event(), asyncio.Event()
    HOLD.update(started=started, release=release)
    first = asyncio.create_task(worker._handle_message(_copy_of(original, headers)))
    await asyncio.wait_for(started.wait(), timeout=5)

    redelivered = _copy_of(original, headers)  # gleiche Header, altes Token
    assert redelivered.headers[HEADER_SCHEDFIRE_CLAIM] == headers[HEADER_SCHEDFIRE_CLAIM]
    await worker._handle_message(redelivered)
    broker.ack.assert_awaited_once_with(redelivered)

    release.set()
    await first
    assert RUNS == ["run"]
