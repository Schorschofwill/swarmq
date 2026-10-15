"""``unique_for`` am Aufruf von ``schedule``/``schedule_many`` (F3-07, F3-31).

E-056: ``0`` und negative Werte sind ein ``ValueError`` vor jedem Claim,
Publish oder Speichern. E-055: ein ``float`` wird ganze Sekunden
(aufgerundet), die TTL des Dedup-Flags ist immer ``int``.

Testliste:
# [x] 0.0 und -0.5 in allen drei Pfaden -> ValueError, kein Claim (ceil-Falle)
# [x] inf/nan in allen drei Pfaden -> ValueError mit "unique_for"
# [x] sofort: 0.2 -> ttl_ms=1000, 3600.0 -> ttl_ms=3_600_000 (int)
# [x] verzoegert: delay=10, unique_for=1.5 -> ttl_ms=(2+10)*1000 (int)
# [x] schedule_many: 0.5 -> ttl_ms=1000 (int)
# [x] None schaltet weiter ab (E-033), kein Claim
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.client import Client

pytestmark = pytest.mark.timeout(5)

_INVALID = [
    pytest.param(0.0, id="null-float"),
    pytest.param(-0.5, id="negativer-bruch"),
    pytest.param(float("inf"), id="inf"),
    pytest.param(float("nan"), id="nan"),
]


@pytest.fixture
def schedule_store(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    from swarmq.client import schedule as client_mod

    store = MagicMock()
    store.add_schedule = AsyncMock(return_value=True)

    def _factory(_backend: object) -> MagicMock:
        return store

    monkeypatch.setattr(client_mod, "ScheduleStore", _factory)
    return store


@pytest.fixture
def client(broker: AsyncMock, backend: AsyncMock) -> Client:
    backend.claim_unique = AsyncMock(return_value=None)
    return Client(broker=broker, backend=backend)


def _ttl_ms(backend: AsyncMock) -> Any:
    backend.claim_unique.assert_awaited_once()
    return backend.claim_unique.await_args.args[2]


# U15 erweitert die oeffentlichen Signaturen auf ``float``; bis dahin ``cast``.
def _seconds(value: float) -> int:
    return cast("int", value)


@pytest.mark.parametrize("unique_for", _INVALID)
async def test_schedule_rejects_invalid_unique_for(
    client: Client, broker: AsyncMock, backend: AsyncMock, unique_for: float
) -> None:
    with pytest.raises(ValueError, match="unique_for"):
        await client.schedule("t", 1, queue="q", unique_for=_seconds(unique_for))

    backend.claim_unique.assert_not_awaited()
    broker.publish.assert_not_awaited()


@pytest.mark.parametrize("unique_for", _INVALID)
async def test_deferred_schedule_rejects_invalid_unique_for(
    client: Client, backend: AsyncMock, schedule_store: MagicMock, unique_for: float
) -> None:
    with pytest.raises(ValueError, match="unique_for"):
        await client.schedule("t", 1, queue="q", delay=30, unique_for=_seconds(unique_for))

    backend.claim_unique.assert_not_awaited()
    schedule_store.add_schedule.assert_not_awaited()


@pytest.mark.parametrize("unique_for", _INVALID)
async def test_schedule_many_rejects_invalid_unique_for(
    client: Client, broker: AsyncMock, backend: AsyncMock, unique_for: float
) -> None:
    with pytest.raises(ValueError, match="unique_for"):
        await client.schedule_many([("t", (1,), {}), ("t", (2,), {})], queue="q", unique_for=_seconds(unique_for))

    backend.claim_unique.assert_not_awaited()
    broker.publish.assert_not_awaited()


@pytest.mark.entscheidung("E-055")
@pytest.mark.parametrize(
    ("unique_for", "expected_ttl_ms"),
    [pytest.param(0.2, 1000, id="bruch"), pytest.param(3600.0, 3_600_000, id="ganzzahlig")],
)
async def test_schedule_float_unique_for_claims_whole_seconds(
    client: Client, backend: AsyncMock, unique_for: float, expected_ttl_ms: int
) -> None:
    await client.schedule("t", 1, queue="q", unique_for=_seconds(unique_for))

    ttl_ms = _ttl_ms(backend)
    assert type(ttl_ms) is int
    assert ttl_ms == expected_ttl_ms


@pytest.mark.entscheidung("E-055")
async def test_deferred_float_unique_for_claims_whole_seconds_plus_delay(
    client: Client, backend: AsyncMock, schedule_store: MagicMock
) -> None:
    await client.schedule("t", 1, queue="q", delay=10, unique_for=_seconds(1.5))

    ttl_ms = _ttl_ms(backend)
    assert type(ttl_ms) is int
    assert ttl_ms == (2 + 10) * 1000
    schedule_store.add_schedule.assert_awaited_once()


@pytest.mark.entscheidung("E-055")
async def test_schedule_many_float_unique_for_claims_whole_seconds(client: Client, backend: AsyncMock) -> None:
    await client.schedule_many([("t", (1,), {})], queue="q", unique_for=_seconds(0.5))

    ttl_ms = _ttl_ms(backend)
    assert type(ttl_ms) is int
    assert ttl_ms == 1000


async def test_explicit_none_still_disables_dedup(client: Client, broker: AsyncMock, backend: AsyncMock) -> None:
    await client.schedule("t", 1, queue="q", unique_for=None)

    backend.claim_unique.assert_not_awaited()
    broker.publish.assert_awaited_once()
