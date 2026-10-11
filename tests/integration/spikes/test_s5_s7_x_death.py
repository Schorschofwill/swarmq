"""Spikes S5 und S7 (MR 12): ``x-death`` bei Client-Republish und wiederholtem Broker-DLX.

Beobachtet auf 4.3.6 und 4.2.9 (identisch, 4.3 ergänzt ``x-acquired-count``):

S5
- Ein vom Client gesetztes ``x-death`` bleibt beim Publish unverändert erhalten.
- Ein Broker-DLX baut ``x-death`` **neu** auf: Client-Einträge (auch einer für
  dieselbe Queue mit ``count`` 5 und einer für eine fremde Queue) sind weg,
  es steht nur der neue Eintrag mit ``count`` 1 da.
- Republish nach einem Broker-DLX (Client mit kopierten Headern oder
  Management-API ``publish`` wie die UI): ``x-death`` kommt unverändert an.
  Der nächste Broker-DLX zählt nicht weiter: ``count`` 1, neue ``time``.
- Shovel ("Move messages" der UI): das Shovel-Plugin ist auf den
  Testbrokern nicht aktiviert, daher nicht geprüft.

S7
- Ein Kreislauf nur im Broker (A rejected -> C, C expired -> A) erhöht in
  ``x-death`` je Runde ``count``; ``time`` bleibt die des ersten Todes.

Test-Liste:
- [x] Client-x-death überlebt einen normalen Publish
- [x] Broker-DLX ersetzt Client-x-death
- [x] Republish nach DLX (Client und Management-API) behält x-death; nächster DLX: count 1, neue time
- [x] wiederholter Broker-DLX im Kreislauf: count steigt, time bleibt
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import aio_pika
import httpx
import pytest
from aio_pika.abc import AbstractConnection, AbstractIncomingMessage

from tests.integration.spikes._broker import AUTH, QUORUM, declare, get_message, publish
from tests.support.services import RabbitMQNode
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.spike]

_PAST = datetime(2026, 1, 1, tzinfo=UTC)


def _entry(queue: str, reason: str, count: int, when: datetime) -> dict[str, Any]:
    return {"count": count, "reason": reason, "queue": queue, "time": when, "exchange": "", "routing-keys": [queue]}


def _naive(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """``time`` ohne Zeitzone vergleichbar machen (aiormq liefert sie mit UTC)."""
    return [{**e, "time": e["time"].replace(tzinfo=UTC) if e["time"].tzinfo is None else e["time"]} for e in entries]


async def _reject(connection: AbstractConnection, queue: str) -> None:
    channel = await connection.channel()
    try:
        target = await channel.get_queue(queue, ensure=False)
        got: list[AbstractIncomingMessage] = []

        async def arrived() -> bool:
            message = await target.get(no_ack=False, fail=False)
            if message is not None:
                got.append(message)
            return bool(got)

        assert await wait_until(arrived, timeout=10.0, interval=0.02)
        await got[0].reject(requeue=False)
    finally:
        await channel.close()


async def _next_second() -> None:
    """``x-death.time`` hat Sekundenauflösung: auf die nächste volle Sekunde warten."""
    start = int(time.time())
    assert await wait_until(lambda: time.time() >= start + 1.05, timeout=3.0, interval=0.05)


@pytest.fixture
async def dlx_pair(amqp: AbstractConnection, queue_name: Callable[[str], str]) -> tuple[str, str]:
    """``source`` dead-lettert über den Default-Exchange nach ``parking``."""
    source, parking = queue_name("s5-src"), queue_name("s5-park")
    assert await declare(amqp, parking, QUORUM) is None
    dlx = {**QUORUM, "x-dead-letter-exchange": "", "x-dead-letter-routing-key": parking}
    assert await declare(amqp, source, dlx) is None
    return source, parking


async def test_client_set_x_death_survives_plain_publish(
    amqp: AbstractConnection, queue_name: Callable[[str], str]
) -> None:
    queue = queue_name("s5-plain")
    assert await declare(amqp, queue, QUORUM) is None
    fake = [_entry("elsewhere", "rejected", 3, _PAST)]

    await publish(amqp, queue, aio_pika.Message(b"s5", headers={"x-death": fake, "x-first-death-queue": "elsewhere"}))
    message = await get_message(amqp, queue)

    assert _naive(message.headers["x-death"]) == fake
    assert message.headers["x-first-death-queue"] == "elsewhere"


async def test_broker_dead_lettering_rebuilds_x_death(amqp: AbstractConnection, dlx_pair: tuple[str, str]) -> None:
    source, parking = dlx_pair
    fake = [_entry(source, "rejected", 5, _PAST), _entry("other", "expired", 2, _PAST)]
    await publish(amqp, source, aio_pika.Message(b"s5", headers={"x-death": fake}))

    await _reject(amqp, source)
    message = await get_message(amqp, parking)

    (death,) = message.headers["x-death"]
    assert (death["queue"], death["reason"], death["count"]) == (source, "rejected", 1)
    assert death["time"].replace(tzinfo=UTC) > _PAST


@pytest.mark.parametrize("republish", ["client", "management-api"])
async def test_republish_after_dead_lettering_keeps_x_death_and_next_dlx_restarts_count(
    node: RabbitMQNode, version: str, amqp: AbstractConnection, dlx_pair: tuple[str, str], republish: str
) -> None:
    source, parking = dlx_pair
    await publish(amqp, source, aio_pika.Message(b"s5", headers={"x-custom": "k"}))
    await _reject(amqp, source)
    parked = await get_message(amqp, parking)
    headers = dict(parked.headers)
    (first,) = headers["x-death"]
    if version == "4.3":
        assert headers["x-acquired-count"] == 1
    else:
        assert "x-acquired-count" not in headers

    if republish == "client":
        await publish(amqp, source, aio_pika.Message(parked.body, headers=headers))
    else:
        body = {
            "properties": {"headers": _jsonable(headers)},
            "routing_key": source,
            "payload": parked.body.decode(),
            "payload_encoding": "string",
        }
        async with httpx.AsyncClient() as http:
            response = await http.post(f"{node.management_url}/exchanges/%2F/amq.default/publish", auth=AUTH, json=body)
        assert response.json() == {"routed": True}
    await _next_second()

    channel = await amqp.channel()
    try:
        replayed = await (await channel.get_queue(source, ensure=False)).get(no_ack=False)
        (carried,) = replayed.headers["x-death"]
        assert {k: carried[k] for k in ("queue", "reason", "count")} == {
            "queue": source,
            "reason": "rejected",
            "count": 1,
        }
        if republish == "client":
            assert carried["time"] == first["time"]
        else:
            # Die Management-API kennt keinen Timestamp-Typ: die Sekunden kommen als Zahl an.
            assert carried["time"] == int(first["time"].timestamp())
        assert replayed.headers["x-custom"] == "k"
        await replayed.reject(requeue=False)
    finally:
        await channel.close()

    again = await get_message(amqp, parking)
    (second,) = again.headers["x-death"]
    assert (second["queue"], second["reason"], second["count"]) == (source, "rejected", 1)
    assert second["time"] > first["time"], "the broker starts a new x-death entry with a new time"


async def test_repeated_broker_dead_lettering_counts_up_and_keeps_time(
    amqp: AbstractConnection, queue_name: Callable[[str], str]
) -> None:
    loop_a, loop_c = queue_name("s7-a"), queue_name("s7-c")
    forward = {**QUORUM, "x-dead-letter-exchange": "", "x-dead-letter-routing-key": loop_c}
    assert await declare(amqp, loop_a, forward) is None
    back = {**QUORUM, "x-message-ttl": 100, "x-dead-letter-exchange": "", "x-dead-letter-routing-key": loop_a}
    assert await declare(amqp, loop_c, back) is None
    await publish(amqp, loop_a, aio_pika.Message(b"s7"))

    rounds: list[list[dict[str, Any]]] = []
    for _ in range(3):
        channel = await amqp.channel()
        target = await channel.get_queue(loop_a, ensure=False)
        got: list[AbstractIncomingMessage] = []

        async def arrived(target: Any = target, got: list[AbstractIncomingMessage] = got) -> bool:
            message = await target.get(no_ack=False, fail=False)
            if message is not None:
                got.append(message)
            return bool(got)

        assert await wait_until(arrived, timeout=10.0, interval=0.02)
        rounds.append(list((got[0].headers or {}).get("x-death", [])))
        await _next_second()
        await got[0].reject(requeue=False)
        await channel.close()

    assert rounds[0] == []
    by_queue_1 = {e["queue"]: e for e in rounds[1]}
    by_queue_2 = {e["queue"]: e for e in rounds[2]}
    assert {q: (e["reason"], e["count"]) for q, e in by_queue_1.items()} == {
        loop_c: ("expired", 1),
        loop_a: ("rejected", 1),
    }
    assert {q: (e["reason"], e["count"]) for q, e in by_queue_2.items()} == {
        loop_c: ("expired", 2),
        loop_a: ("rejected", 2),
    }
    assert {q: e["time"] for q, e in by_queue_2.items()} == {q: e["time"] for q, e in by_queue_1.items()}


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, datetime):
        return int(value.timestamp())
    return value
