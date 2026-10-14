"""Nachweis FS-24: Ein gescheiterter Ready-Handshake meldet einen asyncio-ERROR
("exception in shielded future") und zaehlt den Kindtod doppelt.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-24; Quelle MB-4).

``SlotPool._start_child`` (``worker/compute/pool.py``) wartet per
``wait_for(shield(child.ready))``. Bei Timeout oder Cancel bricht nur das
aeussere Shield-Future ab; asyncio haengt ``_log_on_exception`` an
``child.ready``. Der anschliessende Kill fuehrt ueber das EOF zu
``_finalize``, das ``ChildDied`` auf ``ready`` setzt (Loop-Fehler) und beim
frischen Kind zusaetzlich ``stats.died`` zaehlt, obwohl der Fehler schon als
``unavailable`` gezaehlt ist.

Aufbau: Fake-Kontext aus ``test_pool.py`` (keine echten Prozesse), der
Loop-Exception-Handler wird erfasst und im ``finally`` zurueckgesetzt. Geprueft
wird erst, nachdem ``_finalize`` gelaufen ist (``p._children`` leer).

Laufzeit bis rund 0,15 s: Der Handshake-Timeout (0,05 s) muss wirklich
ablaufen, beim Recycling-Fall kommen Fake-Handshakes (je 0,02 s) dazu, und
``_finalized`` wartet auf die Todesverarbeitung im Leser-Thread.

Testliste:
# [x] Handshake-Timeout beim frischen Kind: kein Loop-Fehler, died == 0, unavailable == 1
# [x] Cancel im Handshake: kein Loop-Fehler, died == 0
# [x] Recycling-Ersatzkind ohne Ready: kein Loop-Fehler, died == 0, unavailable == 1
# [x] Kind stirbt vor dem Ready: SlotUnavailable, kein Loop-Fehler, died == 0, unavailable == 1
"""

from __future__ import annotations

import asyncio
import gc
from collections.abc import AsyncIterator
from typing import Any

import pytest

from swarmq.worker.compute.pool import SlotPool, SlotUnavailable
from tests.unit.worker.compute.test_pool import (
    FakeCtx,
    _job,
    _make_pool,
    _ready,
    _result,
    _settle,
    _started,
    fresh_fake_ctx,
    install_fake_signals,
)

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
def ctx() -> FakeCtx:
    return fresh_fake_ctx()


@pytest.fixture
def signals(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int, int]]:
    return install_fake_signals(monkeypatch)


@pytest.fixture
async def loop_errors() -> AsyncIterator[list[dict[str, Any]]]:
    """Alles, was beim Loop-Exception-Handler ankommt (asyncio-ERROR-Meldungen)."""
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    seen: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: seen.append(context))
    try:
        yield seen
    finally:
        loop.set_exception_handler(previous)


async def _finalized(p: SlotPool) -> None:
    """Warten, bis jeder Kindtod verarbeitet ist; danach unabgeholte Futures einsammeln."""
    await _settle(lambda: not p._children)
    gc.collect()
    await asyncio.sleep(0.01)  # Callbacks des letzten Loop-Durchlaufs abarbeiten


def _messages(errors: list[dict[str, Any]]) -> list[str]:
    return [str(e.get("message")) for e in errors]


@pytest.mark.befund("FS-24")
async def test_handshake_timeout_logs_no_loop_error_and_counts_no_child_death(ctx, signals, loop_errors):
    p = await _make_pool(ctx, slots=1, handshake_timeout_s=0.05)
    with pytest.raises(SlotUnavailable):
        await p.run(_job("a"), use_di=False)
    await _finalized(p)
    assert _messages(loop_errors) == []
    assert p.stats.died == 0
    assert p.stats.unavailable == 1
    await p.aclose()


@pytest.mark.befund("FS-24")
async def test_cancel_during_handshake_logs_no_loop_error_and_counts_no_child_death(ctx, signals, loop_errors):
    p = await _make_pool(ctx, slots=1, handshake_timeout_s=5.0)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    await _started(1)
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    await _finalized(p)
    assert _messages(loop_errors) == []
    assert p.stats.died == 0
    await p.aclose()


@pytest.mark.befund("FS-24")
async def test_failed_recycle_replacement_handshake_logs_no_loop_error(ctx, signals, loop_errors):
    p = await _make_pool(ctx, slots=1, recycle_max_tasks=1, handshake_timeout_s=0.05)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=True))
    old = await _started(1)
    await _ready(p, old, 0)
    old.channel.send(("result", _result("a", 0)))
    await t1
    await _started(2)  # Ersatzkind: bekommt kein Ready
    await _settle(lambda: p.stats.recycled == 1)
    old.die()
    await _finalized(p)
    assert _messages(loop_errors) == []
    assert p.stats.died == 0
    assert p.stats.unavailable == 1
    await p.aclose()


@pytest.mark.befund("FS-24")
async def test_child_death_before_ready_is_slot_unavailable_without_child_death(ctx, signals, loop_errors):
    p = await _make_pool(ctx, slots=1, handshake_timeout_s=5.0)
    t1 = asyncio.create_task(p.run(_job("a"), use_di=False))
    proc = await _started(1)
    proc.die()  # Forkserver-Kind stirbt vor dem Ready (z. B. Importfehler)
    with pytest.raises(SlotUnavailable):
        await t1
    await _finalized(p)
    assert _messages(loop_errors) == []
    assert p.stats.died == 0
    assert p.stats.unavailable == 1
    await p.aclose()
