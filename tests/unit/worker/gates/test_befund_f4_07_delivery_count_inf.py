"""Nachweis F4-07: ``DeliveryCountGate`` wirft ``OverflowError`` bei einem
Zähler-Header ``inf``.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-07; Verdacht
R4F-01; Gegenprüfung ``6-gegenpruefung-F.md``; Fuzz-Treffer in
``tests/fuzz/test_counter_headers_fuzz.py``, geschrumpft
``delivered=None, acquired=float("inf")``).

``delivery_count._count`` fängt nur ``TypeError``/``ValueError``; ``int(inf)``
wirft ``OverflowError``. Die Ausnahme verlässt das Gate, die Nachricht
bleibt bis ``consumer_timeout`` unbestätigt. Ein Double ``inf`` kommt von einem
Publisher (RabbitMQ 4 behält einen vom Publisher gesetzten ``x-delivery-count``
bei der ersten Zustellung, E-054) oder per Shovel-Replay.

Erwartet (E-051): „Ein Zähler-Header, der sich nicht als Zähler lesen lässt,
zählt wie kein Header; die Nachricht wird normal weiterverarbeitet.“ Die
übrigen Zähler-Helfer fangen ``OverflowError`` seit 06958c8.

Testliste:
# [x] x-acquired-count = inf (geschrumpfter Treffer), x-delivery-count = inf, -inf -> Continue, kein Dead-Letter
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.delivery_count import DeliveryCountGate
from swarmq.worker.outcome import Continue

pytestmark = pytest.mark.timeout(5)


class _Delivery:
    def __init__(self, headers: dict[str, Any]) -> None:
        self.headers = headers
        self.queue = "q"


@pytest.mark.befund("F4-07")
@pytest.mark.parametrize(
    "headers",
    [
        {"x-delivery-count": None, "x-acquired-count": float("inf")},
        {"x-delivery-count": float("inf")},
        {"x-acquired-count": float("-inf")},
    ],
    ids=["acquired-inf", "delivered-inf", "acquired-minus-inf"],
)
async def test_infinite_counter_header_counts_like_no_header(headers: dict[str, Any]) -> None:
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", delivery_limit=20)
    deps.broker = MagicMock(spec=[])  # ohne effective_delivery_limit: Konfiguration gilt
    dead_letter = AsyncMock()
    gate = DeliveryCountGate(deps, dead_letter=dead_letter)
    ctx = GateContext(task_id="t", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))

    outcome: object
    try:
        outcome = await gate.apply(_Delivery(headers), ctx)
    except OverflowError as exc:
        outcome = exc

    assert outcome is Continue, f"Zähler {headers} zählt nicht wie kein Header (E-051): {outcome!r}"
    dead_letter.assert_not_awaited()
