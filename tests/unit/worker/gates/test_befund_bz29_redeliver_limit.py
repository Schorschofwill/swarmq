"""Nachweis BZ-29: ``redeliver`` (``nack(requeue=True)``) laeuft auf RabbitMQ
4.3 ohne Grenze und ohne Pause.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-29, Nachtrag aus
Spike S4, ``tests/integration/spikes/test_s4_delivery_count.py``).

Auf 4.3 zaehlt ``nack(requeue=True)`` den ``x-delivery-count`` nicht hoch,
nur ``x-acquired-count`` (ab der zweiten Zustellung 1, 2, ...). Das
``DeliveryCountGate`` liest nur ``x-delivery-count``: Eine Zustellung, die
ein Gate immer wieder per ``redeliver`` zurueckgibt (Backend-Fehlerantwort,
gescheiterter DLQ-Publish, Slot nicht verfuegbar), dreht endlos. Auf 4.2
zaehlt der ``nack`` zwar, aber die rund 20 Runden laufen ohne Pause durch --
ein kurzer Publish-Ausfall verbraucht das ganze Zustellbudget in
Sekundenbruchteilen.

Testliste:
# [x] 4.3-Zaehlerbild nach 19 ``nack``-Runden (delivery-count 2,
#     acquired-count 19) -> Anwendungs-DLQ mit ``RedeliveryLimitError``
# [x] ``redeliver_in_place`` wartet vor dem ``nack``
#     ``min(0,1 s * 2^k, 5 s)``, k = max(Zaehler)
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext, redeliver_in_place
from swarmq.worker.gates.delivery_count import DeliveryCountGate
from swarmq.worker.outcome import Terminal
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)


def _ctx() -> GateContext:
    return GateContext(task_id="bz29-task", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))


@pytest.mark.befund("BZ-29")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-29: Gate liest nur x-delivery-count, auf 4.3 zaehlt nack(requeue) dort nicht: grenzenlos",
)
async def test_acquired_count_at_limit_goes_to_application_dead_letter() -> None:
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", delivery_limit=20)
    dead_letter = AsyncMock()
    gate = DeliveryCountGate(deps, dead_letter=dead_letter)
    # Zaehlerbild auf 4.3 nach einer Zustellung per Channel-Abbruch und
    # weiteren 18 ``nack(requeue=True)``-Runden (Spike S4).
    delivery = SimpleNamespace(headers={"x-delivery-count": 2, "x-acquired-count": 19})

    outcome = await gate.apply(delivery, _ctx())

    assert isinstance(outcome, Terminal), "die nack-Schleife auf 4.3 erreicht das Zustelllimit nie"
    dead_letter.assert_awaited_once()
    assert dead_letter.await_args.args[3] == "RedeliveryLimitError"


@pytest.mark.befund("BZ-29")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-29: redeliver gibt die Zustellung ohne Pause zurueck, die rund 20 Runden laufen in Sekundenbruchteilen",
)
@pytest.mark.parametrize(
    ("headers", "pause_s"),
    [
        ({}, 0.1),
        ({"x-delivery-count": 2, "x-acquired-count": 5}, 3.2),
        ({"x-delivery-count": 7}, 5.0),
        ({"x-acquired-count": 19}, 5.0),
    ],
)
async def test_redeliver_pauses_before_the_nack(worker, broker, headers, pause_s) -> None:
    pauses: list[tuple[float, int]] = []

    async def pause(delay_s: float) -> None:
        pauses.append((delay_s, broker.redeliver.await_count))

    deps = worker._gate_deps
    deps.redeliver_pause = pause
    message = make_message(_default_payload(), headers=headers)

    await redeliver_in_place(deps, message, _ctx())

    broker.redeliver.assert_awaited_once_with(message)
    assert pauses == [(pytest.approx(pause_s), 0)], "kein Backoff vor dem nack(requeue=True)"
