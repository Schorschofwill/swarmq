"""Nachweis FS-10: DLQ-Kopien des Workers behalten die Zustellzaehler des
Brokers (``x-delivery-count``/``x-acquired-count``).

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-10, Quelle R-1).

``RetryHandler.dead_letter`` (``worker/retry.py``), ``reject_to_dead_letter``
(``worker/gates/reject.py``) und das Expiry-Gate (``worker/gates/expiry.py``)
kopieren alle Header des Originals; ``broker.dead_letter`` entfernt nur die
Zustands-Header der Rueckstellschleifen und ``x-death``. Die Retry- und
Rueckstellkopien lassen die Zaehler dagegen weg (``BROKER_DELIVERY_COUNTERS``
in ``worker/gates/__init__.py``), ebenso ``swarmq dlq retry``.

Folge: Wer eine am Zustelllimit dead-gelettertete Nachricht per RabbitMQ-UI
"Move messages", Shovel oder swarmv zurueckspielt (BZ-08, Migrationsanleitung),
legt sie mit ``x-delivery-count: 19`` zurueck; das Delivery-Count-Gate
schickt sie vor jeder Ausfuehrung wieder in die DLQ. Ob RabbitMQ den
publisher-gesetzten Zaehler behaelt, prueft der Integrationstest
``tests/integration/broker/test_befund_fs10_dlq_replay_counters.py``.

Geprueft wird an der Broker-Grenze: ein echter ``RabbitMQBroker`` mit
aufzeichnendem Publish-Channel; so ist egal, ob ein Fix im Worker oder im
Broker filtert.

Testliste:
# [x] Delivery-Count-Gate (WorkerHungError): DLQ-Kopie ohne Broker-Zaehler
# [x] Retry erschoepft (RetryHandler.dead_letter): DLQ-Kopie ohne Broker-Zaehler
# [x] Expiry-Gate (max_age): DLQ-Kopie ohne Broker-Zaehler
# [x] Ablehnung (undecodable, reject_to_dead_letter): DLQ-Kopie ohne Broker-Zaehler
"""

from __future__ import annotations

import time
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock, Mock

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_FS10_COUNTERS = {"x-delivery-count": 19, "x-acquired-count": 19}
_FS10_BELOW_LIMIT = {"x-delivery-count": 3, "x-acquired-count": 3}


class Fs10Fails(Task):
    task_info = TaskInfo(name="fs10_fails", queues=["default"], max_retries=0)

    @override
    async def run(self) -> None:
        raise RuntimeError("fs10 bug")


class Fs10Expiring(Task):
    task_info = TaskInfo(name="fs10_expiring", queues=["default"], max_age=60)

    @override
    async def run(self) -> None:
        raise AssertionError("abgelaufen, darf nicht laufen")


class Fs10Hung(Task):
    task_info = TaskInfo(name="fs10_hung", queues=["default"])

    @override
    async def run(self) -> None:
        raise AssertionError("am Zustelllimit, darf nicht laufen")


class Fs10Exchange:
    """Aufzeichnender ``default_exchange``: (Routing-Key, Header) je Publish."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def publish(self, message: aio_pika.Message, routing_key: str, **_kwargs: Any) -> None:
        self.published.append((routing_key, dict(message.headers or {})))


class Fs10Channel:
    is_closed = False

    def __init__(self) -> None:
        self.default_exchange = Fs10Exchange()

    async def close(self) -> None:
        return None


def _fs10_worker(backend: Any, config: Any) -> tuple[Worker, Fs10Exchange]:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    channel = Fs10Channel()
    broker._publish_channel_factory = AsyncMock(return_value=channel)
    registry = TaskRegistry()
    for task in (Fs10Fails, Fs10Expiring, Fs10Hung):
        registry.register(task)
    return Worker(broker=broker, backend=backend, registry=registry, config=config), channel.default_exchange


def _fs10_message(name: str, counters: dict[str, int], **extra: Any) -> MagicMock:
    message = make_message(
        _default_payload(task_id=f"{name}-id", name=name, args=[], kwargs={}),
        headers={**counters, **extra},
    )
    message.queue = "default"
    message.consumer_tag = None
    return message


def _fs10_assert_dlq_copy_without_counters(exchange: Fs10Exchange) -> None:
    copies = [headers for key, headers in exchange.published if key == "default.XQ"]
    assert len(copies) == 1, f"Voraussetzung: genau eine DLQ-Kopie, publiziert: {exchange.published}"
    kept = {name: copies[0][name] for name in _FS10_COUNTERS if name in copies[0]}
    assert kept == {}, (
        f"DLQ-Kopie traegt die Broker-Zaehler {kept}: ein Replay per UI-'Move'/Shovel landet am "
        "Delivery-Count-Gate sofort wieder in der DLQ"
    )


@pytest.mark.befund("FS-10")
@pytest.mark.xfail(strict=True, reason="FS-10: offen - DLQ-Kopie (Delivery-Count-Gate) behaelt x-delivery-count")
async def test_fs10_delivery_limit_dlq_copy_drops_broker_counters(backend: Any, config: Any) -> None:
    worker, exchange = _fs10_worker(backend, config)

    await worker._handle_message(_fs10_message("fs10_hung", _FS10_COUNTERS))

    _fs10_assert_dlq_copy_without_counters(exchange)


@pytest.mark.befund("FS-10")
@pytest.mark.xfail(strict=True, reason="FS-10: offen - DLQ-Kopie (Retry erschoepft) behaelt x-delivery-count")
async def test_fs10_retry_exhausted_dlq_copy_drops_broker_counters(backend: Any, config: Any) -> None:
    worker, exchange = _fs10_worker(backend, config)

    await worker._handle_message(_fs10_message("fs10_fails", _FS10_BELOW_LIMIT))

    _fs10_assert_dlq_copy_without_counters(exchange)


@pytest.mark.befund("FS-10")
@pytest.mark.xfail(strict=True, reason="FS-10: offen - DLQ-Kopie (Expiry-Gate) behaelt x-delivery-count")
async def test_fs10_expired_dlq_copy_drops_broker_counters(backend: Any, config: Any) -> None:
    worker, exchange = _fs10_worker(backend, config)
    message = _fs10_message("fs10_expiring", _FS10_BELOW_LIMIT, **{HEADER_ENQUEUED_AT: int(time.time()) - 120})

    await worker._handle_message(message)

    _fs10_assert_dlq_copy_without_counters(exchange)


@pytest.mark.befund("FS-10")
@pytest.mark.xfail(strict=True, reason="FS-10: offen - DLQ-Kopie (Ablehnung) behaelt x-delivery-count")
async def test_fs10_rejected_dlq_copy_drops_broker_counters(backend: Any, config: Any) -> None:
    worker, exchange = _fs10_worker(backend, config)
    message = make_message(body=b"fs10: kein JSON", headers=dict(_FS10_BELOW_LIMIT))
    message.queue = "default"
    message.consumer_tag = None

    await worker._handle_message(message)

    _fs10_assert_dlq_copy_without_counters(exchange)
