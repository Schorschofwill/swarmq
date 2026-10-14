"""Nachweis FS-17: Das Expiry-Gate wertet einen gescheiterten Ack nach
liegender DLQ-Kopie als Fehlschlag.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-17, Quelle R-3).

``ExpiryGate`` rief ``broker.dead_letter`` direkt auf und fing
``BrokerConnectionError``; ``SettleFailedAfterPublish`` ist eine Unterklasse
davon. Die Kopie lag also schon in der XQ, trotzdem folgten ``redeliver``
und ``Terminal.never_release()``: das Unique-Flag hing bis ``unique_for``,
und die Logzeilen meldeten einen gescheiterten Publish. E-043 verlangt: Liegt
die Kopie und scheitert nur das Ack, gilt das als terminal wie ein Erfolg
(``dead_letter_copy``).

Testliste:
# [x] SettleFailedAfterPublish (completion-Modus): kein redeliver/nack,
#     Freigabe des Flags, Log ``dlq_ack_failed_after_publish`` statt
#     ``task_expired_dlq_publish_failed``
# [x] mit Ausfuehrungsschutz: Done-Marker gesetzt, die Neuzustellung des
#     Originals wird uebersprungen (keine zweite Kopie)
# [x] Kombination FS-17 + FS-10 + FS-13: start-Modus, echter Broker-Adapter,
#     Ack scheitert am Kanal -> Kopie ohne Broker-Zaehler, Freigabe genau
#     einmal, Done-Marker gesetzt
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from typing import Any, override
from unittest.mock import AsyncMock, Mock

import aio_pika
import pytest
from aiormq.exceptions import ChannelInvalidStateError

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import SettleFailedAfterPublish
from swarmq.headers import HEADER_ENQUEUED_AT, HEADER_UNIQUE_HASH
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_HASH = "c" * 64
_TASK_ID = "fs17-task"


class Fs17Expiring(Task):
    task_info = TaskInfo(name="fs17_expiring", queues=["default"], max_age=60, unique_for=3600)

    @override
    async def run(self) -> None:
        raise AssertionError("abgelaufen, darf nicht laufen")


class Fs17StartUnique(Task):
    task_info = TaskInfo(
        name="fs17_start_unique", queues=["default"], max_age=60, unique_for=3600, unique_until="start"
    )

    @override
    async def run(self) -> None:
        raise AssertionError("abgelaufen, darf nicht laufen")


@pytest.fixture
async def guard_backend() -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(FakeClock())
    await impl.connect()
    yield impl
    await impl.close()


def _fs17_worker(broker: Any, backend: Any, config: Any, *tasks: type[Task]) -> Worker:
    registry = TaskRegistry()
    for task in tasks:
        registry.register(task)
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


def _fs17_message(task_name: str, **extra: Any) -> Any:
    message = make_message(
        _default_payload(task_id=_TASK_ID, name=task_name, args=[], kwargs={}),
        headers={HEADER_UNIQUE_HASH: _HASH, HEADER_ENQUEUED_AT: int(time.time()) - 120, **extra},
    )
    message.queue = "default"
    return message


def _events(logs: list[dict[str, Any]], event: str) -> list[dict[str, Any]]:
    return [e for e in logs if e["event"] == event]


@pytest.mark.befund("FS-17")
async def test_fs17_expired_copy_with_failed_ack_is_terminal(
    broker: Any, backend: Any, config: Any, swarmq_logs: list[dict[str, Any]]
) -> None:
    broker.dead_letter = AsyncMock(side_effect=SettleFailedAfterPublish("ack failed, channel lost"))
    backend.release_unique = AsyncMock(return_value=True)
    worker = _fs17_worker(broker, backend, config, Fs17Expiring)

    await worker._handle_message(_fs17_message("fs17_expiring"))

    broker.dead_letter.assert_awaited_once()
    broker.redeliver.assert_not_awaited()
    broker.nack.assert_not_awaited()
    backend.release_unique.assert_awaited_once()
    assert backend.release_unique.await_args.args[:2] == (_HASH, _TASK_ID)
    assert _events(swarmq_logs, "task_expired_dlq_publish_failed") == []
    [warning] = _events(swarmq_logs, "dlq_ack_failed_after_publish")
    assert warning["log_level"] == "warning"


@pytest.mark.befund("FS-17")
async def test_fs17_guard_marks_done_and_skips_the_redelivered_original(
    broker: Any, backend: Any, config: Any, guard_backend: InMemoryBackend
) -> None:
    broker.dead_letter = AsyncMock(side_effect=SettleFailedAfterPublish("ack failed, channel lost"))
    backend.release_unique = AsyncMock(return_value=True)
    worker = _fs17_worker(broker, backend, config, Fs17Expiring)
    worker._guard_store = guard_backend.execution_guard

    await worker._handle_message(_fs17_message("fs17_expiring"))
    # Der Broker stellt das unbestaetigte Original neu zu.
    await worker._handle_message(_fs17_message("fs17_expiring", **{"x-delivery-count": 1}))

    broker.dead_letter.assert_awaited_once()
    broker.redeliver.assert_not_awaited()
    backend.release_unique.assert_awaited_once()


class Fs17Exchange:
    """Aufzeichnender ``default_exchange``: (Routing-Key, Header) je Publish."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def publish(self, message: aio_pika.Message, routing_key: str, **_kwargs: Any) -> None:
        self.published.append((routing_key, dict(message.headers or {})))


class Fs17Channel:
    is_closed = False

    def __init__(self) -> None:
        self.default_exchange = Fs17Exchange()

    async def close(self) -> None:
        return None


@pytest.mark.befund("FS-17")
async def test_fs17_start_mode_with_lost_channel_copies_without_counters_and_releases_once(
    backend: Any, config: Any, guard_backend: InMemoryBackend
) -> None:
    rabbit = RabbitMQBroker("amqp://guest:guest@localhost/")
    rabbit._connection = Mock(is_closed=False, transport=None)
    rabbit._connection.ready = AsyncMock()
    channel = Fs17Channel()
    rabbit._publish_channel_factory = AsyncMock(return_value=channel)
    backend.release_unique = AsyncMock(return_value=True)
    worker = _fs17_worker(rabbit, backend, config, Fs17StartUnique)
    worker._guard_store = guard_backend.execution_guard
    original = _fs17_message("fs17_start_unique", **{"x-delivery-count": 2, "x-acquired-count": 2})
    original.consumer_tag = None
    original.ack = AsyncMock(side_effect=ChannelInvalidStateError("channel closed"))
    original.nack = AsyncMock(side_effect=ChannelInvalidStateError("channel closed"))

    await worker._handle_message(original)
    redelivered = _fs17_message("fs17_start_unique", **{"x-delivery-count": 3, "x-acquired-count": 3})
    redelivered.consumer_tag = None
    await worker._handle_message(redelivered)

    copies = [headers for key, headers in channel.default_exchange.published if key == "default.XQ"]
    assert len(copies) == 1, f"genau eine DLQ-Kopie erwartet: {channel.default_exchange.published}"
    assert "x-delivery-count" not in copies[0]
    assert "x-acquired-count" not in copies[0]
    original.nack.assert_not_awaited()
    backend.release_unique.assert_awaited_once()
    redelivered.ack.assert_awaited_once()  # als ``done`` uebersprungen
