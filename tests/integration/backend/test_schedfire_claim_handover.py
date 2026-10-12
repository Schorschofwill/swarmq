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
- [x] Claim geschrieben, Antwort verloren (unklarer Ausgang), Freigabe
      scheitert -> die Kopie uebernimmt den Claim und laeuft genau einmal
- [x] die Kopie einer Kopie: deren Claim kam nicht an (das alte Token steht
      noch) -> die naechste Kopie uebernimmt trotzdem

Test-Liste (viertes Review):
- [x] Requeue an derselben Stelle (Fehlerantwort beim Cancel-Check), die
      Freigabe scheitert: statt des tokenlosen ``nack`` geht eine Kopie mit
      Token; nach der Erholung von Valkey laeuft der Fire genau einmal
- [x] ``sched_fire_claim.lua`` direkt: ein leeres erstes Uebernahme-Token
      wird uebersprungen, ein spaeteres passt
- [x] ``sched_fire_claim.lua`` direkt: mehr als 16 Uebernahme-Tokens --
      das Skript prueft alle (die Deckelung liegt beim Worker), ohne
      passendes Token bleibt der Claim fremd
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
from swarmq.exceptions import BackendConnectionError, BackendResponseError
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
    registry.register(FireTask)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", queues=["default"])
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


def _message(fire_key: str, extra: dict[str, Any] | None = None) -> MagicMock:
    payload = {"task": {"id": uuid.uuid4().hex, "queue": "default", "name": "fire-task"}, "args": [], "kwargs": {}}
    return make_message(payload, headers={"x-swarmq-schedfire": fire_key, **(extra or {})})


async def _defer_with_failed_release(
    worker: Worker, broker: MagicMock, backend: ValkeyBackend, msg: Any
) -> dict[str, Any]:
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
    headers: dict[str, Any] = broker.retry_later.await_args.kwargs["headers"]
    broker.retry_later.reset_mock()
    return headers


async def _defer_with_lost_claim_reply(
    worker: Worker,
    broker: MagicMock,
    backend: ValkeyBackend,
    msg: Any,
    *,
    written: bool,
) -> dict[str, Any]:
    """Der Claim-Aufruf endet mit einem Transportfehler -- nachdem Valkey den
    Claim geschrieben hat (``written``) oder bevor er ankam; die Freigabe
    scheitert ebenfalls."""
    store = backend.schedules
    real_claim, real_release = store.claim_fire, store.release_fire_claim

    async def _claim(*args: Any, **kwargs: Any) -> bool:
        if written:
            await real_claim(*args, **kwargs)
        raise BackendConnectionError("reply lost")

    store.claim_fire = _claim  # type: ignore[method-assign]
    store.release_fire_claim = AsyncMock(side_effect=BackendConnectionError("valkey down"))  # type: ignore[method-assign]
    try:
        await worker._handle_message(msg)
    finally:
        store.claim_fire = real_claim  # type: ignore[method-assign]
        store.release_fire_claim = real_release  # type: ignore[method-assign]
    broker.retry_later.assert_awaited_once()
    headers: dict[str, Any] = broker.retry_later.await_args.kwargs["headers"]
    broker.retry_later.reset_mock()
    return headers


def _copy_of(original: Any, headers: dict[str, Any]) -> MagicMock:
    return make_message(body=original.body, headers=headers)


async def test_deferred_copy_takes_over_its_own_claim(
    worker: Worker,
    broker: MagicMock,
    backend: ValkeyBackend,
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
    worker: Worker,
    broker: MagicMock,
    backend: ValkeyBackend,
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
    worker: Worker,
    broker: MagicMock,
    backend: ValkeyBackend,
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


async def test_copy_takes_over_a_claim_whose_reply_was_lost(
    worker: Worker,
    broker: MagicMock,
    backend: ValkeyBackend,
) -> None:
    fire_key = f"cron-{uuid.uuid4().hex[:8]}:1700000000000"
    original = _message(fire_key)
    headers = await _defer_with_lost_claim_reply(worker, broker, backend, original, written=True)
    assert not RUNS

    await worker._handle_message(_copy_of(original, headers))

    assert RUNS == ["run"], "die Kopie hielt den eigenen, geschriebenen Claim fuer fremd -- Fire verloren"
    assert await backend.schedules.is_fire_completed(fire_key) is True


async def test_copy_of_a_copy_takes_over_when_the_lost_claim_never_arrived(
    worker: Worker,
    broker: MagicMock,
    backend: ValkeyBackend,
) -> None:
    fire_key = f"cron-{uuid.uuid4().hex[:8]}:1700000000000"
    original = _message(fire_key)
    first_headers = await _defer_with_failed_release(worker, broker, backend, original)
    second_headers = await _defer_with_lost_claim_reply(
        worker,
        broker,
        backend,
        _copy_of(original, first_headers),
        written=False,
    )
    assert not RUNS

    await worker._handle_message(_copy_of(original, second_headers))

    assert RUNS == ["run"], "das alte Token fehlte in der Kopie -- Fire verloren"
    assert await backend.schedules.is_fire_completed(fire_key) is True


async def test_failed_release_before_requeue_in_place_sends_a_copy_that_runs_once(
    worker: Worker,
    broker: MagicMock,
    backend: ValkeyBackend,
) -> None:
    """Vorher: das Original ging per ``nack`` ohne Token zurueck, der Claim
    blieb stehen -- nach der Erholung bestaetigte sich die Redelivery als
    Duplikat, der Fire war verloren."""
    fire_key = f"cron-{uuid.uuid4().hex[:8]}:1700000000000"
    original = _message(fire_key)
    store = backend.schedules
    real_is_cancelled, real_release = backend.is_cancelled, store.release_fire_claim
    backend.is_cancelled = AsyncMock(side_effect=BackendResponseError("WRONGTYPE"))  # type: ignore[method-assign]
    store.release_fire_claim = AsyncMock(side_effect=BackendConnectionError("valkey down"))  # type: ignore[method-assign]
    try:
        await worker._handle_message(original)
    finally:
        backend.is_cancelled = real_is_cancelled  # type: ignore[method-assign]
        store.release_fire_claim = real_release  # type: ignore[method-assign]
    broker.redeliver.assert_not_awaited()
    broker.retry_later.assert_awaited_once()
    headers: dict[str, Any] = broker.retry_later.await_args.kwargs["headers"]
    assert not RUNS

    await worker._handle_message(_copy_of(original, headers))

    assert RUNS == ["run"], "die Kopie hielt den eigenen Claim fuer fremd -- Fire verloren"
    assert await backend.schedules.is_fire_completed(fire_key) is True


async def test_lua_claim_skips_an_empty_first_takeover_token(backend: ValkeyBackend) -> None:
    fire_key = f"cron-{uuid.uuid4().hex[:8]}:1700000000000"
    store = backend.schedules
    assert await store.claim_fire(fire_key, 60_000, owner="tok-held") is True

    assert await store.claim_fire(fire_key, 60_000, owner="tok-new", takeover=["", "tok-held"]) is True
    assert await store.claim_fire(fire_key, 60_000, owner="tok-other", takeover=["", "tok-held"]) is False


async def test_lua_claim_checks_more_than_sixteen_takeover_tokens(backend: ValkeyBackend) -> None:
    fire_key = f"cron-{uuid.uuid4().hex[:8]}:1700000000000"
    store = backend.schedules
    assert await store.claim_fire(fire_key, 60_000, owner="tok-held") is True
    foreign = [f"tok-{n}" for n in range(20)]

    assert await store.claim_fire(fire_key, 60_000, owner="tok-x", takeover=foreign) is False
    assert await store.claim_fire(fire_key, 60_000, owner="tok-new", takeover=[*foreign, "tok-held"]) is True
