"""DLQ nicht-zerstoerend lesen (Plan 0.12.0, MR 16, BZ-21).

``dlq list``/``inspect``/``retry`` lesen bestaetigungspflichtig ueber einen
eigenen Channel. Spike S4: jede Rueckgabe zaehlt den Delivery-Count der XQ
hoch (nach 20 waere die Nachricht weg), deshalb Kopie ans Ende + Ack statt
Halten und Zurueckgeben. Nachweis:
``tests/integration/broker/test_befund_bz21_dlq_destructive_read.py``.

Test-Liste:
- [x] ``get(no_ack=False)``; das Original wird erst nach dem Publish seiner Kopie bestaetigt
- [x] ``dlq retry``: Kopie in die Haupt-Queue, dann Ack
- [x] Kopie scheitert: Original unbestaetigt, ``BrokerConnectionError``, die anderen laufen weiter
- [x] abgelaufene Nachricht: Ack ohne Kopie
- [x] der eigene Channel wird geschlossen, auch nach einem Fehler beim Lesen
- [x] eigener Channel auf der Consume-Verbindung, falls offen; nie der Publish-Kanal
- [x] gescheitertes Ack: WARNING, kein Fehler
- [x] Kopien tragen keine Zustellzaehler des Brokers (``x-delivery-count``, ``x-acquired-count``)

``hit_limit`` (F4-05, E-028 Nachtrag 2026-10-17): abgelaufene Eintraege
verbrauchen Lesungen, zaehlen aber nicht in ``scanned``.
- [x] list: Abgelaufener vor lebenden, Limit erreicht -> ``hit_limit``, ``scanned`` ohne Abgelaufene
- [x] list: Queue leer vor dem Limit -> kein ``hit_limit``
- [x] inspect/retry: nicht gefunden, Limit erreicht -> ``hit_limit``; gefunden -> kein ``hit_limit``
- [x] retry_all: Limit erreicht -> ``hit_limit``

Testlücken Runde 4 (Plan 2026-10-20-1500, U3; Fake an der Verbindungsgrenze
wie F4-04, über die öffentlichen DLQ-Verben):
- [x] E-1: geschlossene Verbindung -> ``BrokerConnectionError``, kein Channel
      (Mutant ``admin.py`` ``connection is None or connection.is_closed`` -> ``and``)
- [x] E-1: fehlende XQ -> der eigene Channel wird geschlossen, die Ausnahme fliegt;
      ihr Typ ist bewusst nicht festgeschrieben (Mutant ``close_channel_quietly(None)``)
- [x] E-2: inspect bricht nach dem Treffer in der Mitte ab
      (Mutant ``break_after_first_match=True`` -> ``False`` in ``inspect_dead_letter``)
- [x] E-2: retry routet den Vorgänger in die XQ, den Treffer in die Haupt-Queue
      (Mutant ``to_main = … or is_match or True)``)
- [x] E-4: Kopien in XQ und Haupt-Queue sind ``PERSISTENT``
      (Mutant ``delivery_mode=…PERSISTENT`` streichen, je Zweig)
- [x] E-5: genaue Schlüsselmengen von list und inspect, ``body_raw`` ist der dekodierte Body
      (Mutant ``"body_raw"`` -> ``"BODY_RAW"``)
- [x] E-5: ``task.id``/``task.name`` als Nicht-String -> ``None``
      (Mutant ``isinstance(task_id, str)`` -> ``… or True``)
- [x] E-6: Binär-Body wird ersetzend dekodiert, kein Abbruch (Mutant ``errors="strict"``)
- [x] E-6: bytes-Header-Schlüssel werden zu ``str`` (Mutant bytes-Zweig ohne ``decode``)
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, Mock

import aio_pika
import pytest
from aio_pika.exceptions import ChannelNotFoundEntity

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError

pytestmark = [pytest.mark.timeout(5)]

_NOW = 1_760_000_000.0


def _message(task_id: str, headers: dict[str, Any] | None = None, events: list[str] | None = None) -> Mock:
    body = json.dumps({"task": {"id": task_id, "name": "n"}}).encode()
    msg = Mock(body=body, headers={"x-swarmq-dead-lettered-at": int(_NOW * 1000), **(headers or {})})

    async def ack() -> None:
        if events is not None:
            events.append(f"ack {task_id}")

    msg.ack = AsyncMock(side_effect=ack)
    return msg


def _reader(*messages: Mock, events: list[str] | None = None, fail_for: str | None = None) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker.clock = lambda: _NOW
    queue = Mock()
    queue.get = AsyncMock(side_effect=[*messages, None])
    queue.channel = AsyncMock()
    broker._dlq_queue = AsyncMock(return_value=queue)  # type: ignore[method-assign]
    broker.published = []  # type: ignore[attr-defined]
    broker.queue = queue  # type: ignore[attr-defined]

    async def publish(routing_key: str, msg: Any) -> None:
        task_id = json.loads(msg.body)["task"]["id"]
        if task_id == fail_for:
            raise BrokerConnectionError("publish failed")
        if events is not None:
            events.append(f"publish {task_id} -> {routing_key}")
        broker.published.append((routing_key, msg))  # type: ignore[attr-defined]

    broker._publish_with_retry = publish  # type: ignore[method-assign]
    return broker


@pytest.mark.entscheidung("E-028")
async def test_list_reads_with_ack_and_acks_after_the_copy() -> None:
    events: list[str] = []
    broker = _reader(_message("t-1", events=events), _message("t-2", events=events), events=events)

    await broker.list_dead_letters("tasks")

    for call in broker.queue.get.await_args_list:  # type: ignore[attr-defined]
        assert call.kwargs["no_ack"] is False
    assert events == ["publish t-1 -> tasks.XQ", "ack t-1", "publish t-2 -> tasks.XQ", "ack t-2"]


async def test_retry_acks_the_original_after_the_copy_to_the_main_queue() -> None:
    events: list[str] = []
    broker = _reader(_message("t-1", events=events), events=events)

    assert (await broker.retry_dead_letter("tasks", "t-1"))[0] is True

    assert events == ["publish t-1 -> tasks", "ack t-1"]


@pytest.mark.entscheidung("E-028")
async def test_failed_copy_leaves_the_original_unacknowledged() -> None:
    events: list[str] = []
    first, second = _message("t-1", events=events), _message("t-2", events=events)
    broker = _reader(first, second, events=events, fail_for="t-1")

    with pytest.raises(BrokerConnectionError, match="1 of 2"):
        await broker.list_dead_letters("tasks")

    first.ack.assert_not_awaited()
    assert events == ["publish t-2 -> tasks.XQ", "ack t-2"]


async def test_expired_message_is_acked_without_copy() -> None:
    expired = _message(
        "t-1", {"x-swarmq-dead-lettered-at": int((_NOW - 7200) * 1000), "x-swarmq-dead-letter-ttl-ms": 3_600_000}
    )
    broker = _reader(expired)

    entries, scanned = await broker.list_dead_letters("tasks")

    assert (entries, scanned) == ([], 0)
    expired.ack.assert_awaited_once()
    assert broker.published == []  # type: ignore[attr-defined]


async def test_own_channel_is_closed_even_when_reading_fails() -> None:
    broker = _reader()
    broker.queue.get = AsyncMock(side_effect=ConnectionError("gone"))  # type: ignore[attr-defined]

    with pytest.raises(ConnectionError):
        await broker.list_dead_letters("tasks")

    broker.queue.channel.close.assert_awaited_once()  # type: ignore[attr-defined]


async def test_failed_ack_is_a_warning_not_an_error(swarmq_logs) -> None:
    msg = _message("t-1")
    msg.ack = AsyncMock(side_effect=ConnectionError("channel gone"))
    broker = _reader(msg)

    entries, _ = await broker.list_dead_letters("tasks")

    assert len(entries) == 1
    assert [e["event"] for e in swarmq_logs if e["log_level"] == "warning"] == ["dlq_read_ack_failed"]


def _connection() -> AsyncMock:
    conn = AsyncMock(is_closed=False)
    conn.channel.return_value = AsyncMock(is_closed=False)
    return conn


async def test_dlq_handle_lives_on_an_own_channel_of_the_consume_connection() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = _connection()
    broker._consume_connection = _connection()
    broker._publish_channel = AsyncMock(is_closed=False)

    await broker._dlq_queue("tasks")

    broker._consume_connection.channel.return_value.get_queue.assert_awaited_once_with("tasks.XQ")
    broker._connection.channel.assert_not_awaited()
    broker._publish_channel.get_queue.assert_not_awaited()


async def test_dlq_handle_without_consume_connection_uses_an_own_channel_of_the_connection() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = _connection()
    broker._publish_channel = AsyncMock(is_closed=False)

    await broker._dlq_queue("tasks")

    broker._connection.channel.return_value.get_queue.assert_awaited_once_with("tasks.XQ")
    broker._publish_channel.get_queue.assert_not_awaited()


@pytest.mark.parametrize("call", ["list", "retry"])
async def test_copies_carry_no_broker_delivery_counters(call: str) -> None:
    # Ein abgebrochenes Lesen gibt die Nachricht zurueck; der Broker zaehlt
    # dann. Die Kopie ist eine neue Nachricht mit eigenem Zustellbudget.
    counted = _message("t-1", {"x-delivery-count": 1, "x-acquired-count": 1, "x-keep": 1})
    broker = _reader(counted)

    if call == "list":
        await broker.list_dead_letters("tasks")
    else:
        await broker.retry_dead_letter("tasks", "t-1")

    [(_, msg)] = broker.published  # type: ignore[attr-defined]
    assert "x-delivery-count" not in msg.headers
    assert "x-acquired-count" not in msg.headers
    assert msg.headers["x-keep"] == 1


def _expired(task_id: str) -> Mock:
    return _message(
        task_id, {"x-swarmq-dead-lettered-at": int((_NOW - 7200) * 1000), "x-swarmq-dead-letter-ttl-ms": 3_600_000}
    )


@pytest.mark.entscheidung("E-028")
async def test_list_reports_hit_limit_when_an_expired_entry_used_up_a_read() -> None:
    broker = _reader(_expired("old"), _message("t-0"), _message("t-1"))

    result = await broker.list_dead_letters("tasks", limit=2)

    entries, scanned = result
    assert [e["task_id"] for e in entries] == ["t-0"]
    assert scanned == 1
    assert getattr(result, "hit_limit", None) is True


async def test_list_without_reaching_the_limit_reports_no_hit_limit() -> None:
    broker = _reader(_expired("old"), _message("t-0"))

    result = await broker.list_dead_letters("tasks", limit=3)

    assert tuple(result) == (result[0], 1)
    assert getattr(result, "hit_limit", None) is False


@pytest.mark.entscheidung("E-028")
async def test_inspect_miss_reports_hit_limit_when_an_expired_entry_used_up_a_read() -> None:
    broker = _reader(_expired("old"), _message("t-0"), _message("t-1"))

    result = await broker.inspect_dead_letter("tasks", "t-1", scan_limit=2)

    assert tuple(result) == (None, 1)
    assert getattr(result, "hit_limit", None) is True


@pytest.mark.entscheidung("E-028")
async def test_retry_miss_reports_hit_limit_and_a_hit_does_not() -> None:
    missed = await _reader(_expired("old"), _message("t-0"), _message("t-1")).retry_dead_letter(
        "tasks", "t-1", scan_limit=2
    )
    found = await _reader(_message("t-0"), _message("t-1")).retry_dead_letter("tasks", "t-1", scan_limit=2)

    assert (tuple(missed), getattr(missed, "hit_limit", None)) == ((False, 1), True)
    assert (tuple(found), getattr(found, "hit_limit", None)) == ((True, 2), False)


@pytest.mark.entscheidung("E-028")
async def test_retry_all_reports_hit_limit_when_an_expired_entry_used_up_a_read() -> None:
    broker = _reader(_expired("old"), _message("t-0"), _message("t-1"))

    result = await broker.retry_all_dead_letters("tasks", limit=2)

    assert tuple(result) == (1, 1)
    assert getattr(result, "hit_limit", None) is True


# --- Testlücken Runde 4 (U3): Fake an der Verbindungsgrenze -----------------


class _Wired:
    """Broker mit Fake an der Verbindungsgrenze (Muster F4-04):
    ``connection.channel()`` -> Channel -> ``get_queue`` -> Queue mit
    ``messages``. Kopien landen in ``published`` als (routing_key, msg)."""

    def __init__(self, *messages: Mock, get_queue_error: BaseException | None = None) -> None:
        self.broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        self.broker.clock = lambda: _NOW
        self.queue = Mock()
        self.queue.get = AsyncMock(side_effect=[*messages, None])
        self.channel = Mock()
        self.channel.is_closed = False
        self.channel.close = AsyncMock()
        self.channel.get_queue = AsyncMock(return_value=self.queue, side_effect=get_queue_error)
        self.queue.channel = self.channel
        self.connection = Mock()
        self.connection.is_closed = False
        self.connection.channel = AsyncMock(return_value=self.channel)
        self.broker._connection = self.connection
        self.published: list[tuple[str, aio_pika.Message]] = []

        async def publish(routing_key: str, msg: aio_pika.Message) -> None:
            self.published.append((routing_key, msg))

        self.broker._publish_with_retry = publish  # type: ignore[method-assign]

    def routes(self) -> list[tuple[str, str]]:
        """(routing_key, task.id) je Kopie."""
        return [(key, json.loads(msg.body)["task"]["id"]) for key, msg in self.published]


def _raw(body: bytes, headers: dict[Any, Any] | None = None) -> Mock:
    msg = Mock(body=body, headers={"x-swarmq-dead-lettered-at": int(_NOW * 1000), **(headers or {})})
    msg.ack = AsyncMock()
    return msg


async def test_list_on_a_closed_connection_fails_without_opening_a_channel() -> None:
    """E-1: Verbindung geschlossen -> kein Channel. Gegenprobe ``admin.py``
    ``connection is None or connection.is_closed`` -> ``and``: rot."""
    wired = _Wired(_message("t-0"))
    wired.connection.is_closed = True

    with pytest.raises(BrokerConnectionError, match="not connected"):
        await wired.broker.list_dead_letters("tasks")

    wired.connection.channel.assert_not_called()


async def test_missing_dead_letter_queue_closes_the_own_channel_and_raises() -> None:
    """E-1: fehlende XQ. Der eigene Channel wird geschlossen, die Ausnahme
    fliegt weiter; ihr Typ ist bewusst nicht festgeschrieben (heute roh).
    Gegenprobe ``close_channel_quietly(channel)`` -> ``(None)``: rot."""
    wired = _Wired(get_queue_error=ChannelNotFoundEntity("NOT_FOUND - no queue 'tasks.XQ' in vhost '/'"))

    error: BaseException | None = None
    try:
        await wired.broker.list_dead_letters("tasks")
    except Exception as exc:  # der Typ ist bewusst offen (Plan U3, E-1)
        error = exc

    assert error is not None
    wired.channel.get_queue.assert_awaited_once_with("tasks.XQ")
    wired.channel.close.assert_awaited_once()


@pytest.mark.entscheidung("E-028")
async def test_inspect_stops_reading_after_a_match_in_the_middle() -> None:
    """E-2: Treffer in der Mitte; danach wird nichts mehr gelesen. Gegenprobe
    ``break_after_first_match=True`` -> ``False`` (inspect): rot."""
    wired = _Wired(_message("t-0"), _message("t-1"), _message("t-2"))

    result = await wired.broker.inspect_dead_letter("tasks", "t-1")

    match, scanned = result
    assert match is not None
    assert match["task_id"] == "t-1"
    assert scanned == 2
    assert wired.queue.get.await_count == 2
    assert wired.routes() == [("tasks.XQ", "t-0"), ("tasks.XQ", "t-1")]


@pytest.mark.entscheidung("E-028")
async def test_retry_routes_the_predecessor_back_and_only_the_match_to_the_main_queue() -> None:
    """E-2: Vorgänger zurück in die XQ, Treffer in die Haupt-Queue, der
    Nachfolger bleibt ungelesen. Gegenprobe ``to_main = move_to_main and
    (match_task_id is None or is_match or True)``: rot."""
    wired = _Wired(_message("t-0"), _message("t-1"), _message("t-2"))

    found, scanned = await wired.broker.retry_dead_letter("tasks", "t-1")

    assert (found, scanned) == (True, 2)
    assert wired.routes() == [("tasks.XQ", "t-0"), ("tasks", "t-1")]


async def test_copies_to_the_dead_letter_queue_and_the_main_queue_are_persistent() -> None:
    """E-4: Beide Kopien sind ``PERSISTENT``. Gegenprobe: ``delivery_mode``
    in ``_copy_then_ack`` streichen, je Zweig einzeln: rot."""
    wired = _Wired(_message("t-0"), _message("t-1"))

    await wired.broker.retry_dead_letter("tasks", "t-1")

    assert [(key, msg.delivery_mode) for key, msg in wired.published] == [
        ("tasks.XQ", aio_pika.DeliveryMode.PERSISTENT),
        ("tasks", aio_pika.DeliveryMode.PERSISTENT),
    ]


async def test_inspect_and_list_return_exactly_their_documented_keys() -> None:
    """E-5: ``inspect`` liefert den vollen Eintrag samt ``body_raw`` (liest
    ``dlq inspect`` in der CLI), ``list`` nur die Zusammenfassung. Gegenprobe
    ``"body_raw"`` -> ``"BODY_RAW"``: rot."""
    body = json.dumps({"task": {"id": "t-1", "name": "n"}}).encode()
    inspected = _Wired(_raw(body))
    listed = _Wired(_raw(body))

    match, _ = await inspected.broker.inspect_dead_letter("tasks", "t-1")
    entries, _ = await listed.broker.list_dead_letters("tasks")

    assert match is not None
    assert set(match) == {"task_id", "task_name", "queue", "headers", "body", "body_raw", "body_size"}
    assert match["body_raw"] == body.decode()
    assert match["body"] == {"task": {"id": "t-1", "name": "n"}}
    assert match["body_size"] == len(body)
    assert [set(entry) for entry in entries] == [{"task_id", "task_name", "queue", "headers", "body_size"}]


async def test_non_string_task_id_and_name_are_reported_as_missing() -> None:
    """E-5: ``task.id``/``task.name`` sind nur als Text gültig. Gegenprobe
    ``isinstance(task_id, str)`` -> ``isinstance(task_id, str) or True``: rot."""
    wired = _Wired(_raw(json.dumps({"task": {"id": 5, "name": ["n"]}}).encode()))

    entries, scanned = await wired.broker.list_dead_letters("tasks")

    assert scanned == 1
    assert [(entry["task_id"], entry["task_name"]) for entry in entries] == [(None, None)]


async def test_binary_body_is_listed_and_copied_back_unchanged() -> None:
    """E-6: Ein Binär-Body wird ersetzend dekodiert; das Lesen bricht nicht
    ab, die Kopie trägt die Original-Bytes. Gegenprobe ``errors="replace"``
    -> ``errors="strict"``: rot."""
    wired = _Wired(_raw(b"\xff\xfe\x00binary"))

    entries, scanned = await wired.broker.list_dead_letters("tasks")

    assert scanned == 1
    assert [(entry["task_id"], entry["body_size"]) for entry in entries] == [(None, 9)]
    assert [(key, msg.body) for key, msg in wired.published] == [("tasks.XQ", b"\xff\xfe\x00binary")]


async def test_bytes_header_keys_are_decoded_to_text() -> None:
    """E-6: bytes-Schlüssel (mit pamqp 4.0.1 nicht beobachtet) kommen als
    ``str`` an. Gegenprobe: bytes-Zweig in ``_normalise_header_keys`` ohne
    ``decode``: rot."""
    wired = _Wired(_raw(json.dumps({"task": {"id": "t-1", "name": "n"}}).encode(), {b"x-keep": 1}))

    entries, _ = await wired.broker.list_dead_letters("tasks")

    [entry] = entries
    assert entry["headers"].get("x-keep") == 1
    assert all(isinstance(key, str) for key in entry["headers"])
    [(_, copy)] = wired.published
    assert copy.headers is not None
    assert copy.headers["x-keep"] == 1
