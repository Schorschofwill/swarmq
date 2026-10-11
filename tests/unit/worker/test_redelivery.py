"""Pause vor ``redeliver`` (Plan 0.12.0, MR 3, BZ-29).

Test-Liste:
- [x] Pausenformel min(0,1 s * 2^k, 5 s), k = max(x-delivery-count,
      x-acquired-count); kaputte Zaehler zaehlen wie keiner
- [x] ``wait_unless_shutdown``: wartet die Zeit ab
- [x] ``wait_unless_shutdown``: Shutdown schon gesetzt -> sofort zurueck
- [x] ``wait_unless_shutdown``: Shutdown waehrend der Pause -> Abbruch
- [x] Worker verdrahtet die Pause; beim Shutdown endet sie sofort
"""

from __future__ import annotations

import asyncio

import pytest

from swarmq.worker import redelivery
from swarmq.worker.redelivery import redeliver_pause_s, wait_unless_shutdown

pytestmark = pytest.mark.timeout(5)


async def _forever(_delay_s: float) -> None:
    await asyncio.Event().wait()


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({}, 0.1),
        ({"x-delivery-count": 1}, 0.2),
        ({"x-acquired-count": 3, "x-delivery-count": 1}, 0.8),
        ({"x-acquired-count": 5}, 3.2),
        ({"x-acquired-count": 6}, 5.0),
        ({"x-delivery-count": 10_000}, 5.0),
        ({"x-delivery-count": "abc", "x-acquired-count": -4}, 0.1),
    ],
)
def test_pause_formula(headers, expected) -> None:
    assert redeliver_pause_s(headers) == pytest.approx(expected)


async def test_waits_the_delay() -> None:
    slept: list[float] = []

    async def sleep(delay_s: float) -> None:
        slept.append(delay_s)

    await wait_unless_shutdown(2.5, asyncio.Event(), sleep=sleep)

    assert slept == [2.5]


async def test_returns_at_once_when_shutdown_is_set() -> None:
    shutdown = asyncio.Event()
    shutdown.set()

    await wait_unless_shutdown(5.0, shutdown, sleep=_forever)


async def test_shutdown_during_the_pause_ends_it() -> None:
    shutdown = asyncio.Event()
    started = asyncio.Event()

    async def sleep(_delay_s: float) -> None:
        started.set()
        await asyncio.Event().wait()

    waiter = asyncio.create_task(wait_unless_shutdown(5.0, shutdown, sleep=sleep))
    await started.wait()
    shutdown.set()

    await waiter


async def test_worker_wires_the_pause(worker, monkeypatch) -> None:
    slept: list[float] = []

    async def sleep(delay_s: float) -> None:
        slept.append(delay_s)

    monkeypatch.setattr(redelivery, "_sleep", sleep)

    assert worker._gate_deps.redeliver_pause is not None
    await worker._gate_deps.redeliver_pause(3.2)

    assert slept == [3.2]


async def test_worker_pause_ends_at_shutdown(worker, monkeypatch) -> None:
    monkeypatch.setattr(redelivery, "_sleep", _forever)
    worker._shutdown_event.set()

    assert worker._gate_deps.redeliver_pause is not None
    await worker._gate_deps.redeliver_pause(5.0)
