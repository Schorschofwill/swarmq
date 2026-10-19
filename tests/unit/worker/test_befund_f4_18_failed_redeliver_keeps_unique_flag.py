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

Grenze gemockt: nur der Broker; Ausführungsschutz und Unique-Flag sind echt
(``InMemoryBackend``).

Testliste:
# [x] 4 Gates x completion/start x SettleFailedAfterPublish/BrokerConnectionError aus retry_later,
#     nack scheitert -> kein release_unique, das Flag bleibt beim Task
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from swarmq.exceptions import BrokerConnectionError, SettleFailedAfterPublish
from tests.unit.worker._deferral_harness import GATES, TASK_ID, channel_lost, run_deferral

pytestmark = pytest.mark.timeout(5)


def _modes() -> list[object]:
    return [
        pytest.param("completion", id="completion"),
        pytest.param("start", id="start", marks=pytest.mark.entscheidung("E-034")),
    ]


def _publish_errors() -> list[object]:
    return [
        pytest.param(lambda: SettleFailedAfterPublish("ack failed after publisher confirm"), id="settle_failed"),
        pytest.param(lambda: BrokerConnectionError("delay publish failed"), id="publish_failed"),
    ]


@pytest.mark.befund("F4-18")
@pytest.mark.xfail(strict=True, raises=AssertionError, reason="F4-18 offen")
@pytest.mark.parametrize("make_publish_exc", _publish_errors())
@pytest.mark.parametrize("unique_until", _modes())
@pytest.mark.parametrize("gate", GATES)
async def test_failed_redeliver_after_failed_deferral_keeps_the_unique_flag(
    gate: str, unique_until: str, make_publish_exc: Callable[[], BaseException]
) -> None:
    run = await run_deferral(gate, unique_until, publish_exc=make_publish_exc(), nack_exc=channel_lost())

    run.broker.redeliver.assert_awaited_once()
    assert run.release_unique.await_count == 0, (
        "Unique-Flag freigegeben, obwohl die Nachricht unbestätigt ist und wiederkommt "
        "(locking-and-dedup.md: 'requeued before that ... keeps the flag')"
    )
    assert run.flag_owner == TASK_ID, "das Unique-Flag muss beim Task bleiben"
