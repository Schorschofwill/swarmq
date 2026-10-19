"""F4-18 über die übrigen Requeue-Pfade der Pipeline: Scheitert das ``nack``
von ``redeliver_in_place`` am toten Kanal, bleibt das Unique-Flag stehen.

Vor dem Fix warf ``redeliver_in_place`` dort ``BrokerConnectionError``; die
Ausnahme verließ das Gate, und der Terminal-Default im ``finally`` gab das
Flag frei. Seit 0.13.0rc2 gilt der Kanalverlust als erfolgte Rückgabe
(WARNING ``delivery_settle_failed``), die Gates melden ``Redelivered``.

Testliste:
# [x] Limiter-Fehler (kein Ausfall), Lock-Acquire-Fehler, Fehlerantwort des
#     Ausführungsschutzes: nack scheitert -> Flag bleibt, eine WARNING
#     delivery_settle_failed (completion und start)
# [x] Shutdown-Zweig: nack scheitert -> Flag bleibt (Verhalten unverändert)
# [x] Rückstellungs-Gate, nack scheitert mit einer anderen Ausnahme -> alte
#     Semantik: Flag wird freigegeben (KTD5, Ausgang unbekannt), ERROR
#     delivery_handler_failed
"""

from __future__ import annotations

import pytest

from swarmq.exceptions import BrokerConnectionError
from tests.unit.worker._deferral_harness import GATES, REQUEUE_PATHS, TASK_ID, channel_lost, run_deferral

pytestmark = pytest.mark.timeout(5)


@pytest.mark.befund("F4-18")
@pytest.mark.parametrize("unique_until", ["completion", pytest.param("start", marks=pytest.mark.entscheidung("E-034"))])
@pytest.mark.parametrize("path", REQUEUE_PATHS)
async def test_failed_nack_on_a_requeue_path_keeps_the_unique_flag(path: str, unique_until: str) -> None:
    run = await run_deferral(path, unique_until, publish_exc=None, nack_exc=channel_lost())

    run.broker.redeliver.assert_awaited_once()
    assert run.release_unique.await_count == 0
    assert run.flag_owner == TASK_ID
    assert run.events("warning").count("delivery_settle_failed") == 1


@pytest.mark.parametrize("gate", GATES)
async def test_foreign_nack_error_keeps_the_old_release(gate: str) -> None:
    """Eine andere Ausnahme als ``BrokerConnectionError`` sagt nichts über den
    Ausgang des ``nack``; sie geht durch, und die Pipeline gibt wie vor F4-18
    frei (über- statt unterfreigeben, KTD5)."""
    run = await run_deferral(
        gate, "completion", publish_exc=BrokerConnectionError("delay publish failed"), nack_exc=RuntimeError("bug")
    )

    run.release_unique.assert_awaited_once()
    assert run.flag_owner is None
    assert "delivery_handler_failed" in run.events("error")
