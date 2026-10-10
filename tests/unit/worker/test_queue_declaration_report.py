"""Worker meldet die Deklarationen seiner Queues einmal (Plan 0.12.0, MR 8, BZ-19).

Nach dem Deklarieren aller Queues ruft der Worker die optionale
Broker-Fähigkeit ``report_queue_declarations()``; sie gibt höchstens eine
Warnung zu at-most-once-Bestandsqueues aus. Fehlt sie oder scheitert sie,
startet der Worker trotzdem (Q15).

Test-Liste:
- [x] nach den Deklarationen genau ein Aufruf
- [x] Broker ohne die Fähigkeit oder mit Fehler: Start unverändert
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

pytestmark = [pytest.mark.asyncio, pytest.mark.timeout(5)]


async def test_worker_reports_queue_declarations_once_after_declaring(worker, broker):
    order: list[str] = []
    broker.declare_queue = AsyncMock(side_effect=lambda queue: order.append(f"declare:{queue}"))
    broker.report_queue_declarations = AsyncMock(side_effect=lambda: order.append("report"))
    worker._shutdown_event.set()

    await worker.start()

    assert order == ["declare:default", "report"]


@pytest.mark.parametrize(
    "report",
    [
        pytest.param(MagicMock(), id="magicmock-not-awaitable"),
        pytest.param(AsyncMock(side_effect=RuntimeError("broker gone")), id="raising"),
    ],
)
async def test_unusable_report_does_not_stop_the_start(worker, broker, report):
    broker.report_queue_declarations = report
    worker._shutdown_event.set()

    await worker.start()

    broker.consume.assert_awaited_once()
