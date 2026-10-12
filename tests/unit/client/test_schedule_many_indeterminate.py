"""``schedule_many`` und unbestimmte Publishes (B24-Folge, Punkt 1).

Wie ``schedule`` behaelt ``schedule_many`` den Dedup-Claim eines Eintrags,
dessen Publish ``PublishIndeterminate`` meldet: Die Nachricht kann
angekommen sein, eine Freigabe erlaubte ein Duplikat. Freigegeben wird nur,
wo sicher nichts gesendet wurde.

Test-Liste:
- [x] Batch mit einem unbestimmten und einem bestimmt gescheiterten Eintrag:
      nur der Claim des bestimmt gescheiterten wird freigegeben
- [x] gemischter Batch meldet ``PublishIndeterminate``, auch wenn ein bestimmter
      Fehler an kleinerem Index steht (sonst hiesse das "nichts gesendet,
      Wiederholen sicher")
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.client import Client
from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate

pytestmark = [pytest.mark.timeout(5)]


def _publish_failing_with(errors: dict[int, BaseException]) -> AsyncMock:
    """Publish-Attrappe: der n-te Aufruf (0-basiert) wirft ``errors[n]``."""
    calls = [0]

    async def publish(*_args: object, **_kwargs: object) -> None:
        n = calls[0]
        calls[0] += 1
        if n in errors:
            raise errors[n]

    return AsyncMock(side_effect=publish)


async def test_only_the_definitely_failed_claim_is_released(broker, backend, registry) -> None:
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock(return_value=True)
    broker.publish = _publish_failing_with(
        {
            0: PublishIndeterminate("publish to 'math' lost the connection while waiting for the broker's confirm"),
            1: BrokerConnectionError("connection refused"),
        }
    )
    client = Client(broker=broker, backend=backend, registry=registry)

    with pytest.raises(BrokerConnectionError):
        await client.schedule_many([("add", (1,), {}), ("add", (2,), {})], queue="math", unique_for=60)

    released = [call.args[1] for call in backend.release_unique.await_args_list]
    claimed = [call.args[1] for call in backend.claim_unique.await_args_list]
    assert released == [claimed[1]]


async def test_a_mixed_batch_reports_the_indeterminate_failure(broker, backend, registry) -> None:
    broker.publish = _publish_failing_with(
        {
            0: BrokerConnectionError("connection refused"),
            1: PublishIndeterminate("publish to 'math' lost the connection while waiting for the broker's confirm"),
        }
    )
    client = Client(broker=broker, backend=backend, registry=registry)

    with pytest.raises(PublishIndeterminate):
        await client.schedule_many([("add", (1,), {}), ("add", (2,), {})], queue="math")
