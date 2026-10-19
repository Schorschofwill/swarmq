"""Nachweis F4-18: Scheitern ``retry_later`` und danach das ``nack`` eines
Rückstellungs-Gates, gibt die Pipeline das Unique-Flag frei, obwohl die
Nachricht unbestätigt ist und wiederkommt.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-18; Gegenprüfung
2026-10-19, Probe ``probe_gates.py``; Entscheidungsvorlage 2026-10-19,
Punkt 5).

Das Rate-Limit-, Lock-, Ausführungsschutz- und Backend-Defer-Gate rufen nach
einem gescheiterten ``retry_later`` ``redeliver_in_place``. Auf dem toten
Kanal wirft das ``nack`` ``BrokerConnectionError``; die Ausnahme verlässt das
Gate, ``outcome`` bleibt ``Terminal.release_in_finally()``
(``pipeline.py:598``), und das ``finally`` gibt das Flag frei
(``pipeline.py:501-502``).

Erwartet: ``website/guides/locking-and-dedup.md`` („A message that is requeued
before that (rate limit, lock wait) keeps the flag“), E-034
(``test_start_mode_rate_limited_keeps_flag_for_redelivery``), ``outcome.py``
(``Redelivered``: Dedup-Flag bleibt), Kommentar ``pipeline.py:637-640``.

Grenze gemockt: der Broker (``create_autospec``) und für ``backend_defer``
der Limiter; Ausführungsschutz, Unique-Flag, Rate-Limit- und Lock-Store sind
echt (``InMemoryBackend``, ``_deferral_harness.py``).

Testliste:
# [x] 4 Gates x completion/start x SettleFailedAfterPublish/BrokerConnectionError aus retry_later,
#     nack scheitert -> kein release_unique, das Flag bleibt beim Task

Behoben in 0.13.0rc2: ``redeliver_in_place`` wertet einen Kanalverlust beim
``nack`` als erfolgte Rückgabe (WARNING ``delivery_settle_failed``), die Gates
melden ``Redelivered``. Seit F4-17 rufen die ``settle_failed``-Fälle gar kein
``nack`` mehr (geprüft: ``redeliver`` nicht erwartet); das Flag bleibt
ebenso. Den F4-18-Fix weisen die ``publish_failed``-Fälle nach.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from swarmq.exceptions import BrokerConnectionError, SettleFailedAfterPublish
from tests.unit.worker._deferral_harness import GATES, TASK_ID, channel_lost, run_deferral

pytestmark = pytest.mark.timeout(5)


def _publish_errors() -> list[object]:
    # (Ausnahme aus ``retry_later``, erreicht das Gate das ``nack``?)
    return [
        pytest.param(lambda: SettleFailedAfterPublish("ack failed after publisher confirm"), False, id="settle_failed"),
        pytest.param(lambda: BrokerConnectionError("delay publish failed"), True, id="publish_failed"),
    ]


@pytest.mark.befund("F4-18")
@pytest.mark.parametrize(("make_publish_exc", "reaches_nack"), _publish_errors())
@pytest.mark.parametrize("unique_until", ["completion", pytest.param("start", marks=pytest.mark.entscheidung("E-034"))])
@pytest.mark.parametrize("gate", GATES)
async def test_failed_redeliver_after_failed_deferral_keeps_the_unique_flag(
    gate: str, unique_until: str, make_publish_exc: Callable[[], BaseException], reaches_nack: bool
) -> None:
    run = await run_deferral(gate, unique_until, publish_exc=make_publish_exc(), nack_exc=channel_lost())

    run.broker.retry_later.assert_awaited_once()
    if reaches_nack:
        # Nur hier scheitert das ``nack`` wirklich; ohne den F4-18-Fix wird dieser Fall rot.
        run.broker.redeliver.assert_awaited_once()
    else:
        # F4-17/E-043: die Kopie liegt, kein ``nack`` (abgedeckt in ``test_deferral_copy_ack_failed.py``).
        run.broker.redeliver.assert_not_awaited()
    assert run.release_unique.await_count == 0, (
        "Unique-Flag freigegeben, obwohl die Nachricht unbestätigt ist und wiederkommt "
        "(locking-and-dedup.md: 'requeued before that ... keeps the flag')"
    )
    assert run.flag_owner == TASK_ID, "das Unique-Flag muss beim Task bleiben"
