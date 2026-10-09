"""DLQ-Frist der Nachricht statt der Frist des Lesers (Review Phase 2,
2026-10-09; Plan 2026-10-08-1541, KTD6).

``dlq list``/``inspect``/``retry`` laufen oft mit einer anderen
Konfiguration als die Worker (CLI ohne ``SWARMQ_DEAD_LETTER_TTL_S``). Die
Frist, mit der eine Nachricht abgelegt wurde, reist deshalb als Header
``x-swarmq-dead-letter-ttl-ms`` mit; der Leser rechnet mit ihr.

Test-Liste:
1. DLQ-Publish traegt ``x-swarmq-dead-letter-ttl-ms`` mit der Frist in ms.
2. ``dead_letter_ttl_s=None`` -> Header ``"none"`` (unbegrenzt), keine
   ``expiration``.
3. Leser mit kuerzerer Env-Frist: die laengere Frist des Headers gilt, die
   Nachricht bleibt mit deren Rest-TTL.
4. Header ``"none"``: nie verwerfen, ohne ``expiration`` zuruecklegen.
5. Zeitstempel ohne Frist-Header (Zwischenstand): nicht verwerfen, volle
   Env-Frist ab jetzt, beide Header gesetzt.
6. Altbestand ohne beide Header: volle Env-Frist ab jetzt, beide Header.
7. Env ``None`` beim Leser und Nachricht ohne Header: ``"none"``, keine
   ``expiration``.
8. Abgelaufen nach Header-Frist -> verworfen.
9. Unlesbarer Frist-Header zaehlt wie fehlender.
10. ``dlq retry`` entfernt den Frist-Header.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, Mock

from swarmq.broker.rabbitmq import RabbitMQBroker

_NOW = 1_760_000_000.0
_AT = "x-swarmq-dead-lettered-at"
_TTL = "x-swarmq-dead-letter-ttl-ms"


def _broker() -> Any:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    broker._connection = AsyncMock()
    broker.clock = lambda: _NOW
    return broker


def _published(broker: Any) -> Any:
    return broker._publish_channel.default_exchange.publish.call_args.args[0]


async def test_dead_letter_publish_carries_its_ttl_in_ms() -> None:
    broker = _broker()
    await broker._publish_to_dead_letter_queue("tasks", b"body")
    assert _published(broker).headers[_TTL] == 604_800_000


async def test_dead_letter_publish_without_ttl_marks_it_unlimited() -> None:
    broker = _broker()
    broker.dead_letter_ttl_s = None
    await broker._publish_to_dead_letter_queue("tasks", b"body")
    message = _published(broker)
    assert message.headers[_TTL] == "none"
    assert message.properties.expiration is None


def _dlq_broker(*messages: Any, ttl: float | None = 7200.0) -> Any:
    """Leser mit Env-Frist ``ttl`` und einer DLQ aus ``messages``; die
    Republishes landen in ``broker.published`` als (routing_key, msg)."""
    broker = _broker()
    broker.dead_letter_ttl_s = ttl
    queue = Mock()
    queue.get = AsyncMock(side_effect=[*messages, None])
    broker._dlq_queue = AsyncMock(return_value=queue)
    broker.published = []

    async def publish(routing_key: str, msg: Any) -> None:
        broker.published.append((routing_key, msg))

    broker._publish_with_retry = publish
    return broker


def _incoming(task_id: str = "t-1", headers: dict[str, Any] | None = None) -> Mock:
    body = json.dumps({"task": {"id": task_id, "name": "n"}}).encode()
    return Mock(body=body, headers=dict(headers or {}))


def _ago_ms(seconds: float) -> int:
    return int((_NOW - seconds) * 1000)


async def test_reader_with_shorter_env_ttl_keeps_the_messages_own_ttl() -> None:
    thirty_days_ms = 30 * 86_400_000
    broker = _dlq_broker(_incoming(headers={_AT: _ago_ms(3 * 3600), _TTL: thirty_days_ms}), ttl=7200.0)
    entries, _ = await broker.list_dead_letters("tasks")
    [(routing_key, msg)] = broker.published
    assert [e["task_id"] for e in entries] == ["t-1"]
    assert routing_key == "tasks.XQ"
    assert msg.properties.expiration == str(thirty_days_ms - 3 * 3_600_000)
    assert msg.headers[_TTL] == thirty_days_ms


async def test_unlimited_message_is_never_dropped_and_goes_back_without_expiration() -> None:
    broker = _dlq_broker(_incoming(headers={_AT: _ago_ms(10 * 86_400), _TTL: "none"}), ttl=7200.0)
    entries, _ = await broker.list_dead_letters("tasks")
    [(_, msg)] = broker.published
    assert [e["task_id"] for e in entries] == ["t-1"]
    assert msg.properties.expiration is None
    assert msg.headers[_TTL] == "none"


async def test_timestamp_without_ttl_header_is_kept_with_full_env_ttl_from_now() -> None:
    """Zwischenstand ohne Frist-Header: welche Frist der Worker meinte, ist
    unbekannt -- nicht verwerfen, sondern ab jetzt neu stempeln."""
    broker = _dlq_broker(_incoming(headers={_AT: _ago_ms(3 * 3600)}), ttl=7200.0)
    entries, _ = await broker.list_dead_letters("tasks")
    [(_, msg)] = broker.published
    assert [e["task_id"] for e in entries] == ["t-1"]
    assert msg.properties.expiration == "7200000"
    assert msg.headers[_AT] == int(_NOW * 1000)
    assert msg.headers[_TTL] == 7_200_000


async def test_retry_forwards_without_the_ttl_header() -> None:
    stamped = _incoming(headers={_AT: _ago_ms(60), _TTL: 7_200_000})
    broker = _dlq_broker(stamped)
    found, _ = await broker.retry_dead_letter("tasks", "t-1")
    [(routing_key, msg)] = broker.published
    assert found is True and routing_key == "tasks"
    assert _TTL not in msg.headers


async def test_legacy_message_gets_both_headers_with_the_env_ttl() -> None:
    broker = _dlq_broker(_incoming(headers={"x-death": []}), ttl=7200.0)
    await broker.inspect_dead_letter("tasks", "t-1")
    [(_, msg)] = broker.published
    assert msg.properties.expiration == "7200000"
    assert msg.headers[_AT] == int(_NOW * 1000)
    assert msg.headers[_TTL] == 7_200_000


async def test_reader_without_ttl_marks_legacy_messages_unlimited() -> None:
    broker = _dlq_broker(_incoming(headers={}), ttl=None)
    await broker.list_dead_letters("tasks")
    [(_, msg)] = broker.published
    assert msg.properties.expiration is None
    assert msg.headers[_TTL] == "none"


async def test_message_past_its_own_ttl_is_dropped_even_for_a_reader_without_ttl() -> None:
    expired = _incoming("t-old", {_AT: _ago_ms(61), _TTL: 60_000})
    broker = _dlq_broker(expired, ttl=None)
    entries, _ = await broker.list_dead_letters("tasks")
    assert entries == [] and broker.published == []


async def test_unreadable_ttl_header_counts_as_missing() -> None:
    broker = _dlq_broker(_incoming(headers={_AT: _ago_ms(3 * 3600), _TTL: "garbage"}), ttl=7200.0)
    await broker.list_dead_letters("tasks")
    [(_, msg)] = broker.published
    assert msg.properties.expiration == "7200000"
    assert msg.headers[_TTL] == 7_200_000
