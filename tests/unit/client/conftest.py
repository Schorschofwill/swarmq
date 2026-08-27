"""Gemeinsame Client-Fixtures (Plan U10) — verbatim aus ``test_client.py``."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.client import Client
from swarmq.task import TaskInfo

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def broker():
    b = AsyncMock()
    b.publish = AsyncMock()
    return b


@pytest.fixture
def backend():
    b = AsyncMock()
    b.get_result = AsyncMock(return_value=None)

    # U17: das Warten (Pub/Sub + Polling) lebt im Backend. Der Fake bildet
    # den Vertrag von ``wait_for_result`` nach — pollt ``get_result`` bis zum
    # Ergebnis oder Ablauf —, damit die Client-Tests weiter über
    # ``get_result``-side_effects sprechen können.
    async def _wait_for_result(task_id, *, timeout):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            raw = await b.get_result(task_id)
            if raw is not None or loop.time() >= deadline:
                return raw
            await asyncio.sleep(0.005)

    b.wait_for_result = AsyncMock(side_effect=_wait_for_result)
    return b


@pytest.fixture
def registry():
    r = MagicMock()
    r.has = MagicMock(return_value=True)
    entry = MagicMock()
    entry.task_info = TaskInfo(name="add", queues=["math"])
    r.get_entry = MagicMock(return_value=entry)
    return r


@pytest.fixture
def client(broker, backend, registry):
    return Client(broker=broker, backend=backend, registry=registry)


@pytest.fixture
def client_no_registry(broker, backend):
    return Client(broker=broker, backend=backend)
