"""Drain-Abbruch und Kanalverlust im Retry-Publish (FS-08 + E-001, KTD9;
fix-r-flow.md 2.2 und Abschnitt 6 Nr. 1).

Ein vom Drain abgebrochener Handler laeuft nach E-001 weiter, sein
``cancelling()`` bleibt > 0. Schliesst der Kanal, waehrend sein Retry-Publish
wartet, lehnt aiormq die offenen Futures ab, beim ``close()`` ohne Grund mit
``CancelledError`` (bis aiormq 7.2 brach der FS-08-Waechter den Versuch ab,
Plan 2026-10-29-1945, KTD2). Diese ``CancelledError`` ist ein Kanalverlust,
kein eigener Abbruch. Haelt die Pipeline ihn fuer einen
Abbruch, saehe ihr ``finally`` einen terminalen Ausgang: Completed-Marker
und Unique-Freigabe auf einer unbestaetigten Nachricht, der Fire waere nach
der Neuzustellung verloren.

Mit echtem ``RabbitMQBroker`` und echtem aio-pika/aiormq-Publish-Code
(``tests/support/aiormq_transport.py``); ohne Publish-Endfrist (etwa
Crash-Teardown), damit der Retry-Publish wirklich startet.

Testliste:
- [x] Drain-Abbruch, Retry-Publish verliert den Kanal (interne CancelledError):
      kein Completed-Marker, keine Unique-Freigabe, Abbruch nicht
      durchgereicht, Original nicht bestaetigt; der Handler endet ohne
      Ausnahme und gibt das Original zur Neuzustellung zurueck
"""

from __future__ import annotations

import asyncio
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_UNIQUE_HASH
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.aiormq_transport import FakePublishChannel, FakeTransport
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

FIRE = "cron_a:1700000000000"
UNIQUE = "a" * 64


class LongRetryable(Task):
    task_info = TaskInfo(name="fs08-long-retryable", queues=["default"], max_retries=3)
    started: asyncio.Event | None = None

    @override
    async def run(self) -> None:
        assert LongRetryable.started is not None
        LongRetryable.started.set()
        await asyncio.Event().wait()


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # Die Publish-Wiederholungen warten 0.05 * 2**n s; im Unit-Test ohne Wartezeit.
    original = asyncio.sleep

    async def _instant(_delay: float, *args: object) -> None:
        await original(0)

    monkeypatch.setattr(asyncio, "sleep", _instant)


def _closed_transport() -> FakeTransport:
    transport = FakeTransport(closing=True)
    transport.close_channel(ConnectionError("channel closed"))
    return transport


async def test_lost_channel_in_the_retry_publish_after_a_drain_cancel_is_not_terminal(
    backend: Any, config: Any
) -> None:
    first = FakeTransport(closing=True)
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker.publish_timeout_s = 60.0
    broker._connection = Mock(transport=None, is_closed=False)
    broker._connection.ready = AsyncMock()
    # Erster Kanal: der Retry-Publish reiht ein, dann schliesst der Kanal;
    # alle weiteren sind schon zu (Verbindung kommt nicht wieder).
    channels = [FakePublishChannel(first)] + [FakePublishChannel(_closed_transport()) for _ in range(5)]
    broker._publish_channel_factory = AsyncMock(side_effect=channels)

    registry = TaskRegistry()
    registry.register(LongRetryable)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    backend.release_unique = AsyncMock(return_value=True)
    msg = make_message(
        _default_payload(name="fs08-long-retryable", args=[], kwargs={}),
        headers={"x-swarmq-schedfire": FIRE, HEADER_UNIQUE_HASH: UNIQUE},
    )

    LongRetryable.started = asyncio.Event()
    handler = asyncio.create_task(worker._handle_message(msg))
    await LongRetryable.started.wait()
    handler.cancel()  # wie _drain_in_flight; E-001: Fehlerergebnis, Handler laeuft weiter
    await first.next_frame()  # Retry-Kopie eingereiht, nie geschrieben
    # Wie aiormqs ``reject_all`` beim Schliessen des Kanals (``close()`` ohne
    # Grund): Kanal zu, offene Futures mit ``CancelledError`` abgelehnt -- ein
    # aio-pika-/aiormq-interner Abbruch, nicht der eigene.
    first.lose_connection(asyncio.CancelledError())
    await asyncio.wait({handler})

    assert not handler.cancelled(), "der Kanalverlust darf nicht als eigener Abbruch durchgehen"
    backend.schedules.mark_fire_completed.assert_not_awaited()
    backend.release_unique.assert_not_awaited()
    msg.ack.assert_not_awaited()
    # Positiv: Der Handler endet regulaer, das Original geht zur Neuzustellung.
    assert handler.exception() is None
    msg.nack.assert_awaited_once_with(requeue=True)
