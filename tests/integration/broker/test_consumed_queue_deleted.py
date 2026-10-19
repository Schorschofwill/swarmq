"""Gelöschte konsumierte Queue (PF-20, Option (b), E-083).

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (PF-20, Verdacht D der
Gegenprüfung 0.14, Proben ``probe_d_separate``/``probe_d_shared``). Vor 0.14.0
meldete der Consumer-Wächter einen Consumer der gelöschten Queue neu an;
``basic.consume`` scheiterte an ``NOT_FOUND`` und schloss den gemeinsamen
Consume-Kanal (E-019). Alle Queues des Workers standen, in jedem Takt kam
``consumer_watch_tick_failed``, und ein Reconnect lief in eine NOT_FOUND-Schleife
(mit ``shared`` samt Publishes).

Entschieden (Georg, 2026-10-19): eingrenzen und laut scheitern. Der Wächter
prüft die Queue vor dem erneuten ``basic.consume`` passiv auf einem
Wegwerf-Kanal; fehlt sie, meldet er ERROR ``consumer_queue_missing``, nimmt den
Consumer aus Wächter-Neuanmeldung und aio-pikas Restore, ``/ready`` bleibt rot
und nennt die Queue. Die übrigen Queues und die Verbindung bleiben heil. Legt
jemand die Queue neu an, meldet der Wächter den Consumer wieder an.

Test-Liste:
# [x] A gelöscht: B konsumiert weiter, ERROR für A, Readiness rot mit A, kein tick_failed
# [x] danach Reconnect (separate, shared): gelingt, keine NOT_FOUND-Schleife, B und Publish laufen
# [x] A neu angelegt: der Wächter meldet A wieder an, Readiness grün
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from typing import Any, override
from urllib.parse import quote

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts.message import Delivery
from swarmq.health import consumer_check
from tests.integration.broker.test_pf19_close_after_unblocked import _management
from tests.support.known_leaks import aiormq_streamwriter_leak
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until
from tests.unit._log_capture import capture_swarmq_logs

# exclusive: Ein Verbindungsabriss über die Management-API und feste Takte;
# ein ausgelasteter Broker verschöbe Reconnect und Takte.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(120), pytest.mark.exclusive]

_WATCH_INTERVAL_S = 0.2


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    @override
    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def aio_pika_records() -> Iterator[list[logging.LogRecord]]:
    handler = _Records()
    logger = logging.getLogger("aio_pika")
    logger.addHandler(handler)
    try:
        yield handler.records
    finally:
        logger.removeHandler(handler)


class _Setup:
    """Broker mit zwei konsumierten Queues A und B und laufendem Wächter."""

    def __init__(self, rabbitmq_url: str, mode: str) -> None:
        self.url = rabbitmq_url
        self.qa = f"pf20-a-{uuid.uuid4().hex[:8]}"
        self.qb = f"pf20-b-{uuid.uuid4().hex[:8]}"
        self.name = f"pf20-{uuid.uuid4().hex[:8]}"
        self.broker = RabbitMQBroker(f"{rabbitmq_url}?name={self.name}", queue_type="classic")
        self.broker.publish_connection = mode
        self.got: dict[str, list[bytes]] = {self.qa: [], self.qb: []}
        self.ticks = 0

    async def start(self) -> None:
        await self.broker.connect()
        for queue in (self.qa, self.qb):
            await self.broker.declare_queue(queue)
            await self.broker.consume(queue, self._on(queue), concurrency=3)
        watch = self.broker.consumer_watch
        tick = watch.tick

        async def counting_tick() -> None:
            self.ticks += 1
            await tick()

        watch.tick = counting_tick  # type: ignore[method-assign]  # Takte zählen
        self.broker.start_consumer_watch(_WATCH_INTERVAL_S)

    def _on(self, queue: str) -> Any:
        async def on(delivery: Delivery) -> None:
            self.got[queue].append(delivery.body)
            await self.broker.ack(delivery)

        return on

    async def delete_a(self) -> None:
        """Löscht A über eine fremde Verbindung, wie ein Betreiber."""
        connection = await aio_pika.connect(self.url)
        try:
            channel = await connection.channel()
            await channel.queue_delete(self.qa)
        finally:
            await connection.close()

    async def consumes(self, queue: str, body: bytes, timeout: float = 15) -> bool:
        await self.broker.publish(queue, body)
        return await wait_until(lambda: body in self.got[queue], timeout=timeout, interval=0.05)

    async def ticks_pass(self, count: int) -> bool:
        target = self.ticks + count
        return await wait_until(lambda: self.ticks >= target, timeout=10, interval=0.05)

    def readiness(self) -> Any:
        return consumer_check(
            self.broker.consumer_gaps,
            [self.qa, self.qb],
            grace_s=30.0,
            missing=self.broker.consumer_missing_queues,
        )

    async def close(self) -> None:
        await self.broker.close()
        await delete_queue_family(self.url, self.qa, self.qb)


def _events(logs: list[dict[str, Any]], event: str) -> list[dict[str, Any]]:
    return [entry for entry in logs if entry["event"] == event]


async def _fence_a(setup: _Setup, logs: list[dict[str, Any]]) -> None:
    await setup.delete_a()
    assert await wait_until(lambda: bool(_events(logs, "consumer_queue_missing")), timeout=10), (
        "the watch reports the deleted queue"
    )


async def _cut_connections(connection_name: str, count: int) -> None:
    """Trennt die ``count`` Verbindungen mit diesem ``connection_name`` über die
    Management-API (``separate``: Publish- und Consume-Verbindung)."""
    names: list[str] = []
    async with _management() as http:

        async def listed() -> bool:
            nonlocal names
            connections = (await http.get("/connections")).json()
            names = [
                c["name"]
                for c in connections
                if c.get("client_properties", {}).get("connection_name") == connection_name
            ]
            return len(names) == count

        assert await wait_until(listed, timeout=20, interval=0.1), (
            "precondition: the management API lists the connections"
        )
        for name in names:
            (
                await http.delete(f"/connections/{quote(name, safe='')}", headers={"X-Reason": "pf-20"})
            ).raise_for_status()


def _text(record: logging.LogRecord) -> str:
    """Meldung ohne ``%r`` der Argumente: aio-pikas ``OneShotCallback`` scheitert
    am eigenen ``repr``, sobald der Callback gelaufen ist."""
    return " ".join(
        [str(record.msg), *(str(arg) for arg in record.args or () if isinstance(arg, (str, BaseException)))]
    )


def _aiormq(connection: Any) -> Any:
    transport = getattr(connection, "transport", None)
    return None if transport is None else transport.connection


@aiormq_streamwriter_leak  # TS-02: der Broker schließt den Kanal bzw. die Verbindung
@pytest.mark.entscheidung("E-083")
async def test_a_deleted_queue_is_fenced_and_the_other_queue_keeps_consuming(rabbitmq_url: str) -> None:
    setup = _Setup(rabbitmq_url, "separate")
    with capture_swarmq_logs() as logs:
        await setup.start()
        try:
            assert await setup.consumes(setup.qb, b"b-before"), "precondition: B consumes"

            await _fence_a(setup, logs)

            assert await setup.consumes(setup.qb, b"b-after-delete"), "B keeps consuming after A was deleted"
            [missing] = _events(logs, "consumer_queue_missing")
            assert missing["log_level"] == "error"
            assert missing["queue"] == setup.qa
            ready, reason = await setup.readiness()()
            assert ready is False
            assert reason is not None
            assert setup.qa in reason
            assert setup.qb not in reason
            assert await setup.ticks_pass(3), "the watch keeps ticking"
            assert _events(logs, "consumer_watch_tick_failed") == []
            assert _events(logs, "consumer_reregister_failed") == []
            assert len(_events(logs, "consumer_queue_missing")) == 1, "reported once, not on every tick"
        finally:
            await setup.close()


@aiormq_streamwriter_leak  # TS-02: der Broker schließt die Verbindung, aiormq lässt den Writer offen
@pytest.mark.entscheidung("E-083")
@pytest.mark.parametrize("mode", ["separate", "shared"])
async def test_after_a_deleted_queue_a_reconnect_succeeds_without_a_not_found_loop(
    rabbitmq_url: str, mode: str, aio_pika_records: list[logging.LogRecord]
) -> None:
    setup = _Setup(rabbitmq_url, mode)
    with capture_swarmq_logs() as logs:
        await setup.start()
        try:
            await _fence_a(setup, logs)
            assert await setup.consumes(setup.qb, b"b-before-cut"), "precondition: B consumes"
            consume_connection = setup.broker._consume_connection or setup.broker._connection
            dropped = _aiormq(consume_connection)

            await _cut_connections(setup.name, 2 if mode == "separate" else 1)

            def reconnected() -> bool:
                current = _aiormq(consume_connection)
                return current is not None and current is not dropped and not current.is_closed

            assert await wait_until(reconnected, timeout=30, interval=0.05), "the consume connection reconnects"
            assert await setup.consumes(setup.qb, b"b-after-cut", timeout=30), "B consumes after the reconnect"
            not_found = [_text(r) for r in aio_pika_records if "NOT_FOUND" in _text(r)]
            assert not_found == [], "no NOT_FOUND restore loop"
            ready, reason = await setup.readiness()()
            assert ready is False
            assert reason is not None
            assert setup.qa in reason
        finally:
            await setup.close()


@aiormq_streamwriter_leak  # TS-02: der Broker schließt den Kanal
@pytest.mark.entscheidung("E-083")
async def test_a_recreated_queue_is_consumed_again(rabbitmq_url: str) -> None:
    setup = _Setup(rabbitmq_url, "separate")
    with capture_swarmq_logs() as logs:
        await setup.start()
        try:
            await _fence_a(setup, logs)
            operator = RabbitMQBroker(rabbitmq_url, queue_type="classic")
            await operator.connect()
            try:
                await operator.declare_queue(setup.qa)
            finally:
                await operator.close()

            def back() -> bool:
                return any(e["queue"] == setup.qa for e in _events(logs, "consumer_reregistered"))

            assert await wait_until(back, timeout=10), "the watch registers the consumer of A again"
            [again] = [e for e in _events(logs, "consumer_reregistered") if e["queue"] == setup.qa]
            assert again["reason"] == "queue_recreated"
            assert await setup.consumes(setup.qa, b"a-again"), "A consumes again"
            assert await setup.consumes(setup.qb, b"b-still"), "B still consumes"
            assert await setup.readiness()() == (True, None)
        finally:
            await setup.close()
