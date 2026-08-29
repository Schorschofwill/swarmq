"""Was ein Deadline-Fehler dem Aufrufer von ``schedule`` bedeutet.

Plan 2026-08-28-001, U2 (R11 / AE6).

Ein Deckelbruch reisst waehrend des Wartens auf das Publisher-Confirm.
Der Broker kann die Nachricht also bereits haben — der Aufrufer darf
nicht von Nicht-Zustellung ausgehen. Verschaerfend rollt swarmq im
Fehlerfall den Unique-Claim zurueck, sodass ein Wiederholversuch nicht
mehr durch die eigene Dedup-Sperre gedeckt ist.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.exceptions import BrokerConnectionError


@pytest.fixture
def deadline_broker(broker):
    """Broker, dessen Publish mit einem Deckelbruch scheitert — genau die
    Form, die ``_publish_with_retry`` nach Ablauf der Frist wirft."""
    broker.publish = AsyncMock(
        side_effect=BrokerConnectionError(
            "publish to 'math' exceeded the publish deadline of 120.0s; "
            "delivery is indeterminate"
        )
    )
    return broker


class TestDeadlineErrorReachesTheCaller:
    async def test_deadline_error_propagates(self, client, deadline_broker):
        """Der Aufrufer sieht den Fehler — er wird nicht verschluckt."""
        with pytest.raises(BrokerConnectionError, match="indeterminate"):
            await client.schedule("add", 1, 2, queue="math")

    async def test_unique_claim_is_rolled_back_on_deadline(
        self, client, deadline_broker, backend,
    ):
        """Covers AE6. Der Rollback ist bestehendes Verhalten und bleibt es;
        die Folge ist, dass ein Wiederholversuch NICHT duplikatgeschuetzt
        ist, obwohl die erste Nachricht angekommen sein kann."""
        backend.claim_unique = AsyncMock(return_value=None)
        backend.release_unique = AsyncMock()

        with pytest.raises(BrokerConnectionError):
            await client.schedule("add", 1, 2, queue="math", unique_for=60)

        backend.release_unique.assert_awaited_once()
