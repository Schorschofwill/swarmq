"""Obergrenze für den Aufruf-Parameter ``unique_for`` (E-063, Nachtrag 2026-10-19).

``schedule()`` und ``schedule_many()`` nehmen ``unique_for`` zur Laufzeit ohne
``TaskInfo`` an (``client/schedule_resolve.py``). Dieselbe Grenze wie in
``TaskInfo`` (``MAX_DEADLINE_S``) gilt dort vor jedem Claim, Publish oder
Speichern; sonst scheiterte erst das ``SET PX`` in Valkey.

Testliste:
# [x] schedule(unique_for=MAX_DEADLINE_S + 1) -> ValueError, kein Claim, kein Publish
# [x] schedule_many(unique_for=MAX_DEADLINE_S + 1) -> ValueError, kein Claim, kein Publish
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.client import Client
from swarmq.config_validation import MAX_DEADLINE_S

pytestmark = [pytest.mark.timeout(5), pytest.mark.entscheidung("E-063")]


@pytest.fixture
def bound_client(broker: AsyncMock, backend: AsyncMock) -> Client:
    backend.claim_unique = AsyncMock(return_value=None)
    return Client(broker=broker, backend=backend)


async def test_schedule_rejects_unique_for_above_the_bound(
    bound_client: Client, broker: AsyncMock, backend: AsyncMock
) -> None:
    with pytest.raises(ValueError, match=rf"^unique_for must be at most {MAX_DEADLINE_S} seconds"):
        await bound_client.schedule("bound.task", 1, queue="bound", unique_for=MAX_DEADLINE_S + 1)

    backend.claim_unique.assert_not_awaited()
    broker.publish.assert_not_awaited()


async def test_schedule_many_rejects_unique_for_above_the_bound(
    bound_client: Client, broker: AsyncMock, backend: AsyncMock
) -> None:
    with pytest.raises(ValueError, match=rf"^unique_for must be at most {MAX_DEADLINE_S} seconds"):
        await bound_client.schedule_many([("bound.task", (1,), {})], queue="bound", unique_for=MAX_DEADLINE_S + 1)

    backend.claim_unique.assert_not_awaited()
    broker.publish.assert_not_awaited()
