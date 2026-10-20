"""Neue Ausfuehrungsgeneration beim Dead-Lettern durch den Worker und
Weiterleitung durch ``dlq retry`` (Plan 0.12.0, MR 12, BZ-08).

Der Stempel des Verbs ``dead_letter`` setzt ``x-swarmq-replay-id`` auf
``"g" + sha1(aktuelle Identitaet)[:39]`` und ``x-swarmq-dlq-gen`` auf den Wert
von ``x-swarmq-dead-lettered-at`` -- beide im selben Aufruf (Rev. 3 F2). Die
CLI behaelt die Replay-ID nur, wenn beide Werte gleich sind (frisch von einem
0.12-Worker dead-gelettert); sonst (0.11-Worker, Bestand) vergibt sie wie
bisher eine neue ``uuid4``.

Test-Liste:
1. [x] ``dead_letter`` stempelt Generations-ID und ``x-swarmq-dlq-gen`` =
       ``x-swarmq-dead-lettered-at``; ``x-death`` wird zur Base.
2. [x] Ohne Task-ID im Body (kaputte Nachricht) bleibt die Replay-ID, wie sie
       war; kein ``x-swarmq-dlq-gen``.
3. [x] Zwei DLQ-Kopien derselben Ausfuehrung (``PublishIndeterminate``)
       tragen dieselbe Generations-ID, eine neue Generation eine andere.
4. [x] ``dlq retry`` behaelt die Replay-ID bei ``dlq-gen == dead-lettered-at``
       und entfernt ``dlq-gen``.
5. [x] ``dlq retry`` vergibt eine ``uuid4``, wenn ``dlq-gen`` fehlt oder vom
       Zeitstempel abweicht (0.11-Worker hat neu gestempelt).
6. [x] ``dlq retry`` setzt ``x-swarmq-enqueued-at`` neu (A6) und nimmt
       ``x-death`` als Base mit.

Review MR 12 (Hinweise, P2-3):
7. [x] Eine ungueltige Replay-ID (> 64 Zeichen, fremder Producer) ist keine
       frische Generation, auch wenn ``dlq-gen`` passt: neue ID.
8. [x] Hat der Broker die Nachricht nach dem Stempel erneut dead-gelettert
       (``x-death`` nach UI-Move und ``route_via_dlx``), ist die Generation
       verbraucht: neue ID.
9. [x] ``RetryHandler.dead_letter`` (Worker-Pfad) stempelt die Generation
       genau dann, wenn der Body eine Task-ID hat.

Review MR 12 (P2-2):
10. [x] Ohne frische Generation, mit Task-ID im Body: die neue Replay-ID ist
        deterministisch aus der Identitaet der XQ-Nachricht (Broker-DLX-
        Eintrag zweimal wiederholt -> dieselbe ID), verschieden fuer eine
        andere Identitaet; ``uuid4`` nur ohne Task-ID.

Testlücken Runde 4 (Plan 2026-10-20-1500, U3, Lücke E-6):
11. [x] ``x-swarmq-dlq-gen`` als ``float`` (fremder Producer) ist keine frische
        Generation, auch wenn der Wert dem Zeitstempel gleicht: neue Replay-ID
        (Mutant ``admin.py`` ``_fresh_generation``: ``or`` -> ``and``).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts import Delivery
from swarmq.identity import generation_id
from swarmq.registry import TaskRegistry
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.retry import RetryHandler

_NOW = 1_760_000_000.0
_AT = "x-swarmq-dead-lettered-at"
_GEN = "x-swarmq-dlq-gen"
_REPLAY = "x-swarmq-replay-id"


def _broker() -> Any:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    broker._connection = AsyncMock()
    broker.clock = lambda: _NOW
    return broker


def _published(broker: Any) -> Any:
    return broker._publish_channel.default_exchange.publish.call_args.args[0]


def _body(task_id: str = "t-1") -> bytes:
    return json.dumps({"task": {"id": task_id, "name": "n", "queue": "tasks"}}).encode()


def _delivery(headers: dict[str, Any], body: bytes = _body()) -> Delivery:
    handle = Mock(consumer_tag=None)
    handle.ack = AsyncMock()
    return Delivery(body=body, headers=headers, queue="tasks", handle=handle)


async def test_dead_letter_stamps_the_generation_with_its_timestamp() -> None:
    broker = _broker()
    deaths = [{"queue": "tasks", "count": 1, "reason": "rejected"}]
    delivery = _delivery({_REPLAY: "r1", "x-death": deaths, "x-keep": "k"})

    await broker.dead_letter(delivery, headers=dict(delivery.headers))

    headers = _published(broker).headers
    assert headers[_REPLAY] == generation_id("t-1|r1#1")
    assert headers[_GEN] == headers[_AT] == int(_NOW * 1000)
    assert "x-death" not in headers
    assert headers["x-swarmq-xdeath-base"] == 1
    assert headers["x-keep"] == "k"


async def test_without_a_task_id_the_stamp_leaves_the_replay_id_alone() -> None:
    broker = _broker()
    delivery = _delivery({_REPLAY: "r1"}, body=b"not a swarmq message")
    await broker.dead_letter(delivery, headers=dict(delivery.headers))
    headers = _published(broker).headers
    assert headers[_REPLAY] == "r1"
    assert _GEN not in headers
    assert headers[_AT] == int(_NOW * 1000)


async def test_two_copies_of_one_execution_share_the_generation_a_new_execution_does_not() -> None:
    broker = _broker()
    first = _delivery({_REPLAY: "r1"})
    await broker.dead_letter(first, headers=dict(first.headers))
    gen_a = _published(broker).headers[_REPLAY]
    broker.clock = lambda: _NOW + 5  # zweite Kopie derselben Ausfuehrung (PublishIndeterminate), spaeter
    again = _delivery({_REPLAY: "r1"})
    await broker.dead_letter(again, headers=dict(again.headers))
    gen_b = _published(broker).headers[_REPLAY]
    next_generation = _delivery({_REPLAY: gen_a})
    await broker.dead_letter(next_generation, headers=dict(next_generation.headers))
    gen_c = _published(broker).headers[_REPLAY]
    assert gen_a == gen_b != gen_c


def _dlq_broker(*messages: Any) -> Any:
    """Leser mit einer DLQ aus ``messages``; Republishes landen in
    ``broker.published`` als (routing_key, msg)."""
    broker = _broker()
    queue = Mock()
    queue.get = AsyncMock(side_effect=[*messages, None])
    broker._dlq_queue = AsyncMock(return_value=queue)
    broker.published = []

    async def publish(routing_key: str, msg: Any) -> None:
        broker.published.append((routing_key, msg))

    broker._publish_with_retry = publish
    return broker


def _incoming(headers: dict[str, Any], task_id: str = "t-1") -> Mock:
    return Mock(body=_body(task_id), headers=dict(headers))


_AT_MS = int((_NOW - 60) * 1000)


async def test_dlq_retry_keeps_the_replay_id_of_a_fresh_generation() -> None:
    gen = generation_id("t-1")
    broker = _dlq_broker(_incoming({_REPLAY: gen, _AT: _AT_MS, _GEN: _AT_MS, "x-swarmq-dead-letter-ttl-ms": 7_200_000}))
    found, _ = await broker.retry_dead_letter("tasks", "t-1")
    [(routing_key, msg)] = broker.published
    assert found is True
    assert routing_key == "tasks"
    assert msg.headers[_REPLAY] == gen
    assert _GEN not in msg.headers
    assert _AT not in msg.headers


async def test_dlq_retry_assigns_a_uuid_without_a_matching_generation() -> None:
    gen = generation_id("t-1")
    stale = _incoming({_REPLAY: gen, _AT: _AT_MS, _GEN: _AT_MS - 1, "x-swarmq-dead-letter-ttl-ms": 7_200_000})
    legacy = _incoming({_REPLAY: "old", _AT: _AT_MS, "x-swarmq-dead-letter-ttl-ms": 7_200_000}, task_id="t-2")
    broker = _dlq_broker(stale, legacy)
    moved, _ = await broker.retry_all_dead_letters("tasks")
    replay_ids = [msg.headers[_REPLAY] for _, msg in broker.published]
    assert moved == 2
    assert all(len(r) == 32 for r in replay_ids)
    assert gen not in replay_ids
    assert "old" not in replay_ids
    assert all(_GEN not in msg.headers for _, msg in broker.published)


async def test_dlq_retry_restarts_the_age_and_keeps_the_death_count_as_base() -> None:
    deaths = [{"queue": "tasks", "count": 2, "reason": "rejected"}]
    broker = _dlq_broker(_incoming({"x-death": deaths, "x-swarmq-enqueued-at": int(_NOW) - 86_400}))
    await broker.retry_dead_letter("tasks", "t-1")
    [(_, msg)] = broker.published
    assert msg.headers["x-swarmq-enqueued-at"] == int(_NOW)
    assert "x-death" not in msg.headers
    assert msg.headers["x-swarmq-xdeath-base"] == 2


async def test_dlq_retry_does_not_keep_an_invalid_replay_id() -> None:
    too_long = "x" * 65
    ttl = {"x-swarmq-dead-letter-ttl-ms": 7_200_000}
    broker = _dlq_broker(_incoming({_REPLAY: too_long, _AT: _AT_MS, _GEN: _AT_MS, **ttl}))
    await broker.retry_dead_letter("tasks", "t-1")
    [(_, msg)] = broker.published
    assert msg.headers[_REPLAY] != too_long


async def test_dlq_retry_after_a_broker_dead_lettering_gets_a_new_replay_id() -> None:
    """UI-Move einer frisch gestempelten DLQ-Kopie, Lauf, ``route_via_dlx``:
    die Header (``dlq-gen`` gleich dem Zeitstempel) reisen mit, ``x-death``
    zeigt das erneute Dead-Lettern. Die Generation ist verbraucht."""
    gen = generation_id("t-1")
    deaths = [{"queue": "tasks", "count": 1, "reason": "rejected"}]
    ttl = {"x-swarmq-dead-letter-ttl-ms": 7_200_000}
    broker = _dlq_broker(_incoming({_REPLAY: gen, _AT: _AT_MS, _GEN: _AT_MS, "x-death": deaths, **ttl}))
    await broker.retry_dead_letter("tasks", "t-1")
    [(_, msg)] = broker.published
    assert msg.headers[_REPLAY] != gen


async def test_the_worker_dead_letter_path_stamps_a_generation_only_with_a_task_id() -> None:
    """Durch ``RetryHandler.dead_letter`` (der Weg jeder fachlichen DLQ-Kopie)
    statt ueber den In-Memory-Broker, der nicht stempelt."""
    broker = _broker()
    deps = GateDeps(
        broker=broker,
        backend=AsyncMock(),
        config=SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x"),
        signals=Mock(dispatch=AsyncMock()),
        registry=TaskRegistry(),
        rate_limiter=lambda: None,
        lock_manager=lambda: None,
        finalize_schedfire=AsyncMock(),
        maybe_release_unique=AsyncMock(),
        extract_valid_workflow_id=lambda _m: None,
        register_lock=lambda _h, _e: None,
    )
    handler = RetryHandler(deps, workflow_advancer=lambda: None, finish=AsyncMock())
    ctx = GateContext(task_id="t-1", task_name="n", queue="tasks", task_info=TaskInfo(name="n", queues=["tasks"]))

    assert await handler.dead_letter(_delivery({_REPLAY: "r1"}), ctx, "boom", "ValueError") is True
    stamped = _published(broker).headers
    assert stamped[_REPLAY] == generation_id("t-1|r1")
    assert stamped[_GEN] == stamped[_AT]
    assert stamped["x-swarmq-traceback"] == "boom"

    assert await handler.dead_letter(_delivery({_REPLAY: "r1"}, body=b"no task"), ctx, "boom", "ValueError") is True
    unstamped = _published(broker).headers
    assert unstamped[_REPLAY] == "r1"
    assert _GEN not in unstamped


async def test_dlq_retry_without_a_generation_derives_the_replay_id_from_the_identity() -> None:
    deaths = [{"queue": "tasks", "count": 1, "reason": "delivery_limit"}]
    parked = {"x-death": deaths, _AT: _AT_MS, "x-swarmq-dead-letter-ttl-ms": 7_200_000}
    other = {**parked, "x-death": [{**deaths[0], "count": 2}]}
    broker = _dlq_broker(_incoming(parked), _incoming(parked), _incoming(other))
    await broker.retry_all_dead_letters("tasks")
    first, again, different = (msg.headers[_REPLAY] for _, msg in broker.published)
    assert first == again != different


@pytest.mark.timeout(5)
async def test_dlq_retry_does_not_take_a_float_generation_as_fresh() -> None:
    """E-6: Nur ein ``int`` ist eine Generation. ``float(_AT_MS) == _AT_MS``
    gälte sonst als frisch, und die Wiederholung liefe unter der alten
    Replay-ID. Gegenprobe ``admin.py:63`` ``or`` -> ``and``: rot."""
    gen = generation_id("t-1")
    ttl = {"x-swarmq-dead-letter-ttl-ms": 7_200_000}
    broker = _dlq_broker(_incoming({_REPLAY: gen, _AT: _AT_MS, _GEN: float(_AT_MS), **ttl}))

    await broker.retry_dead_letter("tasks", "t-1")

    [(routing_key, msg)] = broker.published
    assert routing_key == "tasks"
    assert msg.headers[_REPLAY] != gen
    assert _GEN not in msg.headers
