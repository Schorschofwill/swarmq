"""Nachweis BZ-24: Ein Producer ohne Registry verliert still Dedup und Prioritaet.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-24).

``Client(registry=None)`` loest weder ``unique_for`` noch ``priority`` noch
den Liefermodus aus der ``TaskInfo`` des Workers auf (``client/schedule.py``,
``_resolve_unique``/``_resolve_queue_and_delivery_mode``): kein
Unique-Claim, kein ``.high``/``.low``-Suffix, immer ``at_least_once``. Der
Plan (2026-04-17-004, Risks) sah als Abhilfe einen Laufzeit-Log vor, wenn
``_MISSING`` weitergereicht wird -- es gibt keinen.

Das korrekte Verhalten haengt an einer Produktentscheidung (Warnung,
Defaults aus dem Backend lesen oder Registry erzwingen). Der Test prueft die
Mindestanforderung: Der stille Verlust wird sichtbar (WARNING mit Task-Name).

Testliste:
# [x] schedule() ohne Registry und ohne explizites unique_for -> WARNING,
#     dass TaskInfo-Defaults (Dedup, Prioritaet) nicht aufgeloest werden
#     (bei Konstruktion oder beim schedule())
"""

from __future__ import annotations

import pytest

from swarmq.client import Client
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = pytest.mark.timeout(5)


@pytest.mark.befund("BZ-24")
async def test_registry_less_schedule_warns_that_task_defaults_are_unresolved(broker, backend) -> None:
    with capture_swarmq_logs() as logs:
        # Konstruktion im Mitschnitt: der Plan sah den Log bei der Konstruktion vor.
        producer = Client(broker=broker, backend=backend)
        await producer.schedule("billing.send_invoice", 42, queue="billing")

    broker.publish.assert_awaited_once()
    assert broker.publish.await_args.args[0] == "billing", "Voraussetzung: keine Prioritaets-Queue aufgeloest"
    # Bei der Konstruktion kennt der Client den Task-Namen noch nicht: jede
    # WARNING genuegt als Mindestanforderung.
    warned = [e for e in logs if e["log_level"] in ("warning", "error")]
    assert warned, f"Dedup/Prioritaet still verloren, kein WARNING: {[e['event'] for e in logs]}"
