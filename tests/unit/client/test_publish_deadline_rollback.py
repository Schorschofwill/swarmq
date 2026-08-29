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

from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate


@pytest.fixture
def deadline_broker(broker):
    """Broker, dessen Publish mit einem Deckelbruch scheitert — genau die
    Form, die ``_publish_with_retry`` nach Ablauf der Frist wirft."""
    broker.publish = AsyncMock(
        side_effect=PublishIndeterminate(
            "publish to 'math' exceeded the publish deadline of 120.0s; "
            "delivery is indeterminate"
        )
    )
    return broker


@pytest.fixture
def failing_broker(broker):
    """Broker, dessen Publish definitiv nichts gesendet hat."""
    broker.publish = AsyncMock(
        side_effect=BrokerConnectionError("connection refused")
    )
    return broker


class TestDeadlineErrorReachesTheCaller:
    async def test_deadline_error_propagates(self, client, deadline_broker):
        """Der Aufrufer sieht den Fehler — er wird nicht verschluckt."""
        with pytest.raises(BrokerConnectionError, match="indeterminate"):
            await client.schedule("add", 1, 2, queue="math")

    async def test_unique_claim_survives_an_indeterminate_publish(
        self, client, deadline_broker, backend,
    ):
        """Covers AE6. Die Sperre bleibt stehen, weil der Broker die
        Nachricht bereits haben kann. Ein Wiederholversuch laeuft dagegen
        und wird abgewiesen, statt den Task ein zweites Mal auszufuehren —
        fuer ein Einmaligkeits-Feature ist Blockieren die richtige
        Richtung, und die Sperre loest sich nach ihrer Lebensdauer."""
        backend.claim_unique = AsyncMock(return_value=None)
        backend.release_unique = AsyncMock()

        with pytest.raises(PublishIndeterminate):
            await client.schedule("add", 1, 2, queue="math", unique_for=60)

        backend.release_unique.assert_not_awaited()

    async def test_unique_claim_is_still_released_on_a_definite_failure(
        self, client, failing_broker, backend,
    ):
        """Die Ausnahme gilt nur fuer den unbestimmten Fall. Wo sicher
        nichts gesendet wurde, muss die Sperre weichen — sonst blockierte
        ein gewoehnlicher Verbindungsfehler das erneute Einreihen."""
        backend.claim_unique = AsyncMock(return_value=None)
        backend.release_unique = AsyncMock()

        with pytest.raises(BrokerConnectionError):
            await client.schedule("add", 1, 2, queue="math", unique_for=60)

        backend.release_unique.assert_awaited_once()

    async def test_indeterminate_is_catchable_as_a_broker_error(
        self, client, deadline_broker,
    ):
        """Der neue Typ ist eine Unterklasse: bestehende Aufrufer und die
        eng gefassten Handler im Ablauf-Gate fangen ihn unveraendert."""
        with pytest.raises(BrokerConnectionError):
            await client.schedule("add", 1, 2, queue="math")
