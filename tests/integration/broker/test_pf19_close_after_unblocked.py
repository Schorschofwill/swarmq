"""PF-19 (a): Das Close des verworfenen Publish-Kanals wartet im Alarm auf ``unblocked``.

Plan 0.14, U6; Nachtrag (3) zu E-025, Spike
``docs/solutions/spikes/2026-10-spike-kanal-close-nach-unblocked.md``. Reißt im
Speicheralarm die Frist des auslösenden Publish, verwirft swarmq den
Publish-Kanal (E-078). Bis 0.13 lief dessen ``Channel.Close`` sofort in die
5-s-Frist, und bei der Aufhebung loggte aiormq ERROR „Got frame for closed
channel“. Jetzt wartet das Close auf ``unblocked`` oder das Ende der
Verbindung, höchstens die Publish-Frist.

Test-Liste:
- [x] m0, Alarm, m1 (``PublishIndeterminate``), m2 wartet, Aufhebung nach mehr
      als 5 s: keine aiormq-ERROR, beim Broker nur der neue Kanal (Nummer nicht
      1), m2 bestätigt
- [x] Verbindungsende im Alarm: das wartende Close endet sofort, ohne Fehler
      und ohne hängenden Task; nach dem Reconnect ist m2 bestätigt

Nicht Teil dieser Tests: die Kanalzahl nach einem Reconnect. Bis 0.13 öffnete
aio-pikas ``RobustConnection`` dabei jeden noch nicht eingesammelten
``RobustChannel`` neu, auch geschlossene oder verworfene; das ist F4-20, behoben
in 0.14 und nachgewiesen in ``test_befund_f4_20_restore_closed_channels.py``.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Iterator
from typing import Any, override
from urllib.parse import quote

import httpx
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq.blocked import unblocked_event
from swarmq.exceptions import PublishIndeterminate
from tests.integration.broker.conftest import MemoryAlarm, wait_for_alarm
from tests.support.known_leaks import aiormq_streamwriter_leak
from tests.support.queues import delete_queue_family
from tests.support.services import RABBITMQ_MAIN
from tests.support.wait import wait_until

# exclusive: der Speicheralarm blockiert jeden Verleger des Brokers.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(90), pytest.mark.exclusive]

# Lang genug, dass die Aufhebung in die Wartezeit des Close fällt, und mit
# ``_RELEASE_AFTER_S`` über der alten 5-s-Close-Frist.
_PUBLISH_TIMEOUT_S = 15.0
_RELEASE_AFTER_S = 8.0
# Das Verbindungsende gibt das wartende Close frei, lange vor der Publish-Frist.
_CLOSE_ENDS_WITHIN_S = 5.0


class _AiormqRecords(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.records: list[logging.LogRecord] = []

    @override
    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def aiormq_records() -> Iterator[list[logging.LogRecord]]:
    handler = _AiormqRecords()
    logger = logging.getLogger("aiormq")
    logger.addHandler(handler)
    try:
        yield handler.records
    finally:
        logger.removeHandler(handler)


async def _connection_names(http: httpx.AsyncClient, connection_name: str) -> list[str]:
    """Namen der Broker-Verbindungen mit diesem ``connection_name``."""
    connections = (await http.get("/connections")).json()
    return [c["name"] for c in connections if c.get("client_properties", {}).get("connection_name") == connection_name]


def _management() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=RABBITMQ_MAIN.management_url, auth=("guest", "guest"), timeout=5.0)


async def _broker_channels(connection_name: str) -> list[int] | None:
    """Kanalnummern der Verbindung laut Management-API; ``None``: Verbindung unbekannt."""
    async with _management() as http:
        names = await _connection_names(http, connection_name)
        if not names:
            return None
        channels = (await http.get(f"/connections/{quote(names[0], safe='')}/channels")).json()
    return sorted(int(c["number"]) for c in channels)


async def _close_connection_at_broker(connection_name: str) -> None:
    """Der Broker beendet die Verbindung (wie ``rabbitmqctl close_connection``).

    Die Management-API führt eine neue Verbindung erst nach ihrer ersten
    Statistik-Meldung; bis dahin wird gepollt."""
    names: list[str] = []
    async with _management() as http:

        async def listed() -> bool:
            nonlocal names
            names = await _connection_names(http, connection_name)
            return bool(names)

        assert await wait_until(listed, timeout=20, interval=0.2), "the management API lists the connection"
        [name] = names
        (await http.delete(f"/connections/{quote(name, safe='')}", headers={"X-Reason": "pf19"})).raise_for_status()


async def _wait_until_blocked(broker: RabbitMQBroker) -> None:
    async def blocked() -> bool:
        event = unblocked_event(broker._connection)
        return isinstance(event, asyncio.Event) and not event.is_set()

    assert await wait_until(blocked, timeout=10, interval=0.05), "precondition: connection.blocked arrived"


async def _broker_channels_settle(connection_name: str, expected: list[int]) -> list[int] | None:
    """Wartet, bis die Management-API ``expected`` meldet (ihre Statistik
    hinkt nach), und liefert den letzten Stand."""
    seen: list[int] | None = None

    async def settled() -> bool:
        nonlocal seen
        seen = await _broker_channels(connection_name)
        return seen == expected

    await wait_until(settled, timeout=20, interval=0.5)
    return seen


def _aiormq_connection(broker: RabbitMQBroker) -> Any:
    """Die aktuelle aiormq-Verbindung des Brokers; ``None`` während eines Reconnects."""
    transport = getattr(broker._connection, "transport", None)
    return None if transport is None else transport.connection


def _open_aiormq_channels(broker: RabbitMQBroker) -> list[int]:
    return sorted(_aiormq_connection(broker).channels)


@pytest.mark.entscheidung("E-025")
async def test_the_discarded_channel_closes_after_unblocked_without_aiormq_error(
    rabbitmq_url: str, memory_alarm: MemoryAlarm, aiormq_records: list[logging.LogRecord]
) -> None:
    queue = f"pf19-{uuid.uuid4().hex[:10]}"
    name = f"pf19-{uuid.uuid4().hex[:10]}"
    broker = RabbitMQBroker(f"{rabbitmq_url}?name={name}", queue_type="classic")
    broker.publish_timeout_s = _PUBLISH_TIMEOUT_S
    await broker.connect()
    m2: asyncio.Task[object] | None = None
    try:
        await broker.declare_queue(queue)
        await broker.publish(queue, b"m0")
        assert _open_aiormq_channels(broker) == [1], "precondition: m0 runs on channel 1"

        memory_alarm()
        await wait_for_alarm(active=True)
        # Der auslösende Publish liegt schon auf dem Socket: Frist reißt, Kanal 1 wird verworfen.
        with pytest.raises(PublishIndeterminate):
            await broker.publish(queue, b"m1")
        await _wait_until_blocked(broker)
        m2 = asyncio.create_task(broker.publish(queue, b"m2"))

        # Der Alarm hält über die alte 5-s-Close-Frist hinaus, kürzer als die Publish-Frist.
        done, _ = await asyncio.wait({m2}, timeout=_RELEASE_AFTER_S)
        assert not done, "precondition: m2 waits while the broker blocks"
        await asyncio.to_thread(memory_alarm.release)
        receipt = await asyncio.wait_for(m2, timeout=_PUBLISH_TIMEOUT_S)
        assert receipt.queue == queue, "m2 is confirmed"

        async def closes_done() -> bool:
            return not broker._pending_channel_closes

        assert await wait_until(closes_done, timeout=10, interval=0.05), "the deferred close finishes"
        channels = await _broker_channels_settle(name, [2])
        assert channels == [2], f"only the new channel stays open at the broker, got {channels}"
        assert _open_aiormq_channels(broker) == [2]
        errors = [r.getMessage() for r in aiormq_records if r.levelno >= logging.ERROR]
        assert errors == [], f"no aiormq ERROR after unblocked: {errors}"
    finally:
        if m2 is not None and not m2.done():
            m2.cancel()
        if m2 is not None:
            await asyncio.gather(m2, return_exceptions=True)
        await broker.close()
        await delete_queue_family(rabbitmq_url, queue)


@aiormq_streamwriter_leak  # TS-02: der Broker schließt die Verbindung, aiormq lässt den Writer offen
@pytest.mark.entscheidung("E-025")
async def test_a_connection_ending_during_the_alarm_ends_the_waiting_close(
    rabbitmq_url: str, memory_alarm: MemoryAlarm
) -> None:
    queue = f"pf19-{uuid.uuid4().hex[:10]}"
    name = f"pf19-{uuid.uuid4().hex[:10]}"
    broker = RabbitMQBroker(f"{rabbitmq_url}?name={name}", queue_type="classic")
    broker.publish_timeout_s = 3.0
    await broker.connect()
    try:
        await broker.declare_queue(queue)
        await broker.publish(queue, b"m0")

        memory_alarm()
        await wait_for_alarm(active=True)
        with pytest.raises(PublishIndeterminate):
            await broker.publish(queue, b"m1")
        # Die Frist des auslösenden Publish war kurz; das Close liest die Frist
        # erst in seinem ersten Schritt und wartet damit lang. Nur das Ende der
        # Verbindung kann es vorher freigeben.
        broker.publish_timeout_s = _PUBLISH_TIMEOUT_S
        [closer] = broker._pending_channel_closes
        dropped = _aiormq_connection(broker)

        await _close_connection_at_broker(name)
        done, _ = await asyncio.wait({closer}, timeout=_CLOSE_ENDS_WITHIN_S)
        assert done, "the end of the connection releases the waiting close"
        assert closer.exception() is None
        assert not broker._pending_channel_closes

        await asyncio.to_thread(memory_alarm.release)

        # Bis aio-pika neu verbindet (rund 5 s), zeigt der Transport noch auf die
        # tote, blockierte aiormq-Verbindung. Bis 0.13 wartete ein Publish in
        # diesem Fenster bis zur Frist auf deren Unblocked-Event (F4-21, behoben
        # in 0.14, eigener Nachweis); dieser Test sendet erst nach dem Reconnect.
        async def reconnected() -> bool:
            return _aiormq_connection(broker) not in (None, dropped)

        assert await wait_until(reconnected, timeout=20, interval=0.1), "aio-pika reconnects"
        receipt = await asyncio.wait_for(broker.publish(queue, b"m2"), timeout=_PUBLISH_TIMEOUT_S)
        assert receipt.queue == queue, "after the reconnect m2 is confirmed"
    finally:
        await broker.close()
        await delete_queue_family(rabbitmq_url, queue)
