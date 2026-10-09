"""Spikes S1–S9 (Plan 0.12.0, Abschnitt 3.3): Charakterisierung der gepinnten Broker.

Jeder Spike läuft gegen 4.3.6 (Testbroker, ``SWARMQ_TEST_RABBITMQ_*``) und
4.2.9 (Compose-Dienst ``rabbitmq42``, ``SWARMQ_TEST_RABBITMQ42_*``, Standard
5742/15742). Die Tests halten das beobachtete Verhalten fest; ein
Broker-Upgrade, das es ändert, macht sie rot. Policies und vhosts entstehen
nur über die Management-API und verschwinden im ``finally``; Queues löscht
der Teardown. Kein Spike löst einen Broker-Alarm aus.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable

import aio_pika
import pytest
from aio_pika.abc import AbstractConnection

from tests.integration.spikes._broker import delete_queues
from tests.support.services import RABBITMQ_42, RABBITMQ_MAIN, RabbitMQNode, require_rabbitmq


@pytest.fixture(params=["4.3", "4.2"])
def node(request: pytest.FixtureRequest) -> RabbitMQNode:
    if request.param == "4.2":
        return require_rabbitmq(RABBITMQ_42, "4.2.")
    return require_rabbitmq(RABBITMQ_MAIN, "4.3.")


@pytest.fixture
def version(node: RabbitMQNode) -> str:
    """``"4.2"`` oder ``"4.3"`` für versionsabhängige Erwartungen."""
    return ".".join(node.version().split(".")[:2])


@pytest.fixture
async def amqp(node: RabbitMQNode) -> AsyncIterator[AbstractConnection]:
    connection = await aio_pika.connect(node.amqp_url)
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
async def queue_name(node: RabbitMQNode) -> AsyncIterator[Callable[[str], str]]:
    """Fabrik für Queue-Namen; jede so benannte Queue löscht der Teardown."""
    names: list[str] = []

    def _make(label: str) -> str:
        name = f"spike-{label}-{uuid.uuid4().hex[:8]}"
        names.append(name)
        return name

    yield _make
    await delete_queues(node.amqp_url, *names)
