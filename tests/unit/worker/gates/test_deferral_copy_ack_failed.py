"""E-043 für alle Rückstellungs-Kopien (Nachtrag 2026-10-19, F4-17).

Liegt die Rückstellungs-Kopie eines Gates bestätigt in der Delay-Queue und
scheitert nur das Ack des Originals (``SettleFailedAfterPublish``), ist die
Rückstellung erfolgt: WARNING ``deferral_ack_failed_after_publish``, danach
die normale Rückstellungszeile, kein ``redeliver_in_place``, das Unique-Flag
bleibt. Kein Superseded-Marker (gleicher Retry-Zähler): Kopie und neu
zugestelltes Original laufen beide durch die Gates.

Testliste:
# [x] Rate-Limit, Lock, Ausführungsschutz, Backend-Zurückstellung: WARNING mit
#     ``deferral``, normale Zeile, kein nack, keine *_failed_redelivered-Zeile,
#     Flag bleibt
# [x] echter Publish-Fehler: weiter die alte Zeile und das nack
"""

from __future__ import annotations

import pytest

from swarmq.exceptions import BrokerConnectionError, SettleFailedAfterPublish
from tests.unit.worker._deferral_harness import TASK_ID, run_deferral

pytestmark = pytest.mark.timeout(5)

# Gate -> (Wert von ``deferral``, normale Rückstellungszeile, Zeile bei gescheitertem Publish)
_LINES = {
    "rate_limit": ("rate_limit", "task_rate_limited", "rate_limit_requeue_failed"),
    "lock": ("lock_conflict", "task_lock_conflict", "lock_conflict_requeue_failed"),
    "guard": ("execution_guard", "execution_guard_deferred", "execution_guard_defer_failed_redelivered"),
    "backend_defer": ("backend_defer", "task_deferred_backend_unavailable", "task_defer_failed_redelivered"),
}


@pytest.mark.entscheidung("E-043")
@pytest.mark.parametrize("gate", list(_LINES))
async def test_published_deferral_copy_with_failed_ack_counts_as_deferred(gate: str) -> None:
    deferral, normal_line, failed_line = _LINES[gate]

    run = await run_deferral(
        gate, "completion", publish_exc=SettleFailedAfterPublish("ack failed after publisher confirm"), nack_exc=None
    )

    [entry] = [e for e in run.logs if e["event"] == "deferral_ack_failed_after_publish"]
    assert entry["log_level"] == "warning"
    assert entry["logger"] == "swarmq.worker.gates"
    assert (entry["deferral"], entry["task_id"]) == (deferral, TASK_ID)
    assert (entry["error_type"], entry["error"]) == ("SettleFailedAfterPublish", "ack failed after publisher confirm")
    events = [e["event"] for e in run.logs]
    assert normal_line in events
    assert failed_line not in events
    assert run.events("error") == []
    run.broker.redeliver.assert_not_awaited()
    assert run.flag_owner == TASK_ID


@pytest.mark.parametrize("gate", list(_LINES))
async def test_failed_deferral_publish_still_redelivers(gate: str) -> None:
    _, _, failed_line = _LINES[gate]

    run = await run_deferral(
        gate, "completion", publish_exc=BrokerConnectionError("delay publish failed"), nack_exc=None
    )

    assert failed_line in [e["event"] for e in run.logs]
    assert "deferral_ack_failed_after_publish" not in [e["event"] for e in run.logs]
    run.broker.redeliver.assert_awaited_once()
    assert run.flag_owner == TASK_ID
