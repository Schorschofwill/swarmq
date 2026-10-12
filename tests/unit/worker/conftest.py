"""Worker-Unit-Fixtures (Plan U7): aus ``test_worker.py`` in die Conftest
gezogen, damit die Gate-Testmodule dieselben Broker-/Backend-Mocks nutzen."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import structlog

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.worker import Worker
from tests.unit.worker._helpers import AddTask


@pytest.fixture
def broker():
    b = MagicMock(spec=RabbitMQBroker)
    b.connect = AsyncMock()
    b.declare_queue = AsyncMock()
    b.consume = AsyncMock()
    b.ack = AsyncMock()
    b.nack = AsyncMock()
    b.redeliver = AsyncMock()
    b.retry_later = AsyncMock()
    b.dead_letter = AsyncMock()
    b.route_via_dlx = AsyncMock()
    b.close = AsyncMock()
    b.cancel_consume = AsyncMock()
    return b


@pytest.fixture
def backend():
    be = MagicMock(spec=ValkeyBackend)
    be.connect = AsyncMock()
    be.store_result = AsyncMock()
    be.notify_result = AsyncMock()
    be.close = AsyncMock()
    # Cancel-flag primitives (Phase 3b.3). Default: no flag set, so
    # existing happy-path tests continue to pass the pre-start check.
    be.is_cancelled = AsyncMock(return_value=False)
    be.set_cancel_flag = AsyncMock(return_value=True)
    be.clear_cancel_flag = AsyncMock()
    # Schedfire-dedup (Phase 3c Unit 8 + Unit 1 pre/post markers).
    # Defaults represent "first delivery, no prior completion":
    # is_fire_completed=False, Inflight-Claim gewinnt, Completed-Write OK.
    be.schedules.is_fire_completed = AsyncMock(return_value=False)
    be.schedules.claim_fire = AsyncMock(return_value=True)
    be.schedules.mark_fire_completed = AsyncMock(return_value=True)
    be.schedules.release_fire_claim = AsyncMock()
    # Härtung R2 P1-10: workflow-level cancel flag, default unset.
    be.is_workflow_cancelled = AsyncMock(return_value=False)
    # _client is needed by Worker.start() to construct RateLimiter. We
    # provide a MagicMock whose register_script returns another MagicMock
    # — concrete rate-limit behaviour is covered by the integration suite.
    be._client = MagicMock()
    be._client.register_script = MagicMock(return_value=AsyncMock())
    return be


@pytest.fixture
def config():
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=10,
        queues=["default"],
    )


@pytest.fixture
def registry():
    return TaskRegistry()


@pytest.fixture
def worker(broker, backend, registry, config):
    registry.register(AddTask)
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


@pytest.fixture
def app_log_context():
    """Von der App gebundene Kontextvariable ``request_id`` (Plan
    2026-10-06-0831, U10); nach dem Test wieder entfernt."""
    tokens = structlog.contextvars.bind_contextvars(request_id="req-7")
    try:
        yield {"request_id": "req-7"}
    finally:
        structlog.contextvars.reset_contextvars(**tokens)
