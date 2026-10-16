"""Nachweis F3-07: ``schedule()``/``schedule_many()`` nehmen ``unique_for<=0``
an.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-07; Quelle R3C-05).

``TaskInfo`` lehnt ``unique_for<=0`` ab (CD6, ``task.py``), die CLI nimmt
``--unique-for`` nur als positive Ganzzahl. Der Aufruf-Parameter
``unique_for`` an ``schedule``/``schedule_many`` (``api.md``: „Override
``TaskInfo.unique_for``“) wird dagegen ungeprueft uebernommen: sofort
ausgefuehrt meldet Valkey ``SET PX 0`` als undokumentierte
``BackendResponseError``, verzoegert laeuft es still und das Flag lebt nur bis
zum Fire.

Behoben in 0.12.3 (E-056; ``_resolve_unique`` prueft wie ``TaskInfo``).

Erwartet (CD6 fuer den Aufruf-Override): ``ValueError`` vor jedem Claim,
Publish oder Speichern, sofort wie verzoegert.

Testliste:
# [x] schedule(unique_for=0|-5) sofort -> ValueError, kein Claim, kein Publish
# [x] schedule(unique_for=0|-5, delay=30) -> ValueError, nichts gespeichert
# [x] schedule_many(unique_for=0|-5) -> ValueError, kein Claim, kein Publish
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.client import Client

pytestmark = pytest.mark.timeout(5)

_BAD = [0, -5]


@pytest.fixture
def schedule_store(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    from swarmq.client import schedule as client_mod

    store = MagicMock()
    store.add_schedule = AsyncMock()

    def _factory(_backend: object) -> MagicMock:
        return store

    monkeypatch.setattr(client_mod, "ScheduleStore", _factory)
    return store


@pytest.fixture
def f307_client(broker: AsyncMock, backend: AsyncMock) -> Client:
    backend.claim_unique = AsyncMock(return_value=None)
    return Client(broker=broker, backend=backend)


@pytest.mark.befund("F3-07")
@pytest.mark.entscheidung("E-056")
@pytest.mark.parametrize("unique_for", _BAD)
async def test_schedule_rejects_non_positive_unique_for(
    f307_client: Client, broker: AsyncMock, backend: AsyncMock, unique_for: int
) -> None:
    with pytest.raises(ValueError, match="unique_for"):
        await f307_client.schedule("f307.task", 1, queue="f307", unique_for=unique_for)

    backend.claim_unique.assert_not_awaited()
    broker.publish.assert_not_awaited()


@pytest.mark.befund("F3-07")
@pytest.mark.entscheidung("E-056")
@pytest.mark.parametrize("unique_for", _BAD)
async def test_deferred_schedule_rejects_non_positive_unique_for(
    f307_client: Client, backend: AsyncMock, schedule_store: MagicMock, unique_for: int
) -> None:
    with pytest.raises(ValueError, match="unique_for"):
        await f307_client.schedule("f307.task", 1, queue="f307", delay=30, unique_for=unique_for)

    backend.claim_unique.assert_not_awaited()
    schedule_store.add_schedule.assert_not_awaited()


@pytest.mark.befund("F3-07")
@pytest.mark.entscheidung("E-056")
@pytest.mark.parametrize("unique_for", _BAD)
async def test_schedule_many_rejects_non_positive_unique_for(
    f307_client: Client, broker: AsyncMock, backend: AsyncMock, unique_for: int
) -> None:
    with pytest.raises(ValueError, match="unique_for"):
        await f307_client.schedule_many(
            [("f307.task", (1,), {}), ("f307.task", (2,), {})], queue="f307", unique_for=unique_for
        )

    backend.claim_unique.assert_not_awaited()
    broker.publish.assert_not_awaited()
