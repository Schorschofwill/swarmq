"""Nachweis F4-17: Liegt die Rückstellungs-Kopie des Rate-Limit- oder
Lock-Gates bestätigt in der Delay-Queue und scheitert nur das Ack des
Originals, loggen die Gates trotzdem ERROR.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-17; „Entscheidung
überdenken?“ Punkt 4; Gegenprüfung 2026-10-19; Entscheidungsvorlage
2026-10-19, Punkt 5).

``rate_limit.py`` und ``lock.py`` fangen jede Ausnahme von
``broker.retry_later`` gleich und loggen ERROR ``rate_limit_requeue_failed``
bzw. ``lock_conflict_requeue_failed``. ``SettleFailedAfterPublish`` heißt
aber: Die Kopie ist veröffentlicht (Publisher-Confirm), nur das Bestätigen
des Originals scheiterte (``exceptions.py``).

Erwartet: Level-Regel aus 0.8.0 (``changelog.md:3082-3086``: ERROR nur für
Datenverlust oder einen gescheiterten Task/Prozess, WARNING für
selbstheilende Störungen); gleiche Regel für die Retry-Kopie (F4-15,
``retry_ack_failed_after_publish``) und die DLQ-Kopie (E-043,
``dlq_ack_failed_after_publish``).

Grenze gemockt: nur der Broker (``retry_later`` wirft
``SettleFailedAfterPublish``, das ``nack`` gelingt).

Testliste:
# [x] Rate-Limit- und Lock-Gate: retry_later wirft SettleFailedAfterPublish -> keine ERROR-Zeile
"""

from __future__ import annotations

import pytest

from swarmq.exceptions import SettleFailedAfterPublish
from tests.unit.worker._deferral_harness import run_deferral

pytestmark = pytest.mark.timeout(5)


@pytest.mark.befund("F4-17")
@pytest.mark.xfail(strict=True, raises=AssertionError, reason="F4-17 offen")
@pytest.mark.parametrize("gate", ["rate_limit", "lock"])
async def test_deferral_copy_published_but_ack_failed_logs_no_error(gate: str) -> None:
    run = await run_deferral(
        gate,
        "completion",
        publish_exc=SettleFailedAfterPublish("ack failed after publisher confirm"),
        nack_exc=None,
    )

    assert run.events("error") == [], (
        "ERROR, obwohl die Rückstellungs-Kopie bestätigt liegt (SettleFailedAfterPublish); erwartet WARNING "
        "wie retry_ack_failed_after_publish (F4-15) und dlq_ack_failed_after_publish (E-043)"
    )
