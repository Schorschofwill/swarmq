"""Nachweis FS-23: Ein im Hintergrund (Recycling) gesperrter Slot wird weiter vergeben.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-23; Quelle MB-3).

``SlotPool._kill_child`` (``worker/compute/pool.py``) sperrt einen Slot, wenn sein
Kind SIGKILL ueberlebt (E-035). ``run`` behaelt den Index aber nur ein, wenn die
Sperre entsteht, waehrend es ihn selbst haelt. Sperrt das Hintergrund-Recycling
(``_recycle`` -> ``_end_child`` -> ``_kill_child``) den Slot, liegt sein Index
schon wieder in der Freiliste, und der naechste ``run`` startet dort ein Kind.
Dass die Sperre dauerhaft bleibt, ist entschieden (E-035) und nicht Gegenstand.

Aufbau: Fake-Kontext aus ``test_pool.py`` (keine echten Prozesse). Das alte
DI-Kind auf Slot 0 ist ``immortal`` und ueberlebt Ende-Signal und SIGKILL.

Laufzeit ueber 100 ms (rund 0,15 s je gesperrtem Slot): Die Sperre entsteht
erst nach ``end_grace_s``, ``kill_grace_s`` und ``reap_timeout_s`` (je 0,05 s),
dazu kommen die Fake-Handshakes (je 0,02 s). Es sind die kurzen Fristen aus
``_make_pool``; der Ablauf ist ueber Zustaende synchronisiert, nicht ueber Zeit.

Testliste:
# [x] Nach der Hintergrund-Sperre landen zwei folgende run() nie auf dem gesperrten Slot
# [x] Ein Slot gesperrt, der andere liefert ein Ergebnis (kein SlotUnavailable, Pool gesund)
# [x] Beide Slots im Hintergrund gesperrt: run() wirft SlotUnavailable, healthy ist False
# [x] run() wartet schon (gesperrten Index verworfen), als der letzte Slot im Hintergrund
#     gesperrt wird: SlotUnavailable ueber die Poison-Werte, kein Haenger
"""

from __future__ import annotations

import asyncio

import pytest

from swarmq.worker.compute.pool import SlotPool, SlotUnavailable
from tests.unit.worker.compute.test_pool import (
    FakeCtx,
    FakeProcess,
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


async def _block_in_background(p: SlotPool, tid: str, slot: int) -> list[FakeProcess]:
    """DI-Job auf ``slot``; das alte Kind ueberlebt das Recycling-Ende und sperrt den Slot im
    Hintergrund. Liefert altes Kind und Ersatzkind zum Aufraeumen."""
    n = len(FakeCtx.started)
    t = asyncio.create_task(p.run(_job(tid), use_di=True))
    old = await _started(n + 1)
    old.behaviour = "immortal"
    await _ready(p, old, slot)
    old.channel.send(("result", _result(tid, slot)))
    await t
    replacement = await _started(n + 2)
    await _ready(p, replacement, slot)
    await _settle(lambda: not p._background)
    assert slot in p.stats.blocked_slots, "Voraussetzung: das Recycling hat den Slot gesperrt"
    return [old, replacement]


async def _close(p: SlotPool, procs: list[FakeProcess]) -> None:
    for proc in procs:
        proc.die()  # vorher beenden: aclose wartet sonst end_grace_s auf das Ersatzkind
    await p.aclose()


async def _run_fresh(p: SlotPool, tid: str) -> int:
    """Einen Job ohne DI ausfuehren; liefert den Slot, auf dem das frische Kind lief."""
    n = len(FakeCtx.started)
    t = asyncio.create_task(p.run(_job(tid), use_di=False))
    proc = await _started(n + 1)
    slot: int = proc.args[2]
    await _ready(p, proc, slot)
    proc.channel.send(("result", _result(tid, slot, value=tid)))
    assert (await t).value == tid
    proc.die()
    return slot


@pytest.mark.befund("FS-23")
async def test_a_slot_blocked_by_background_recycling_is_never_handed_out_again(ctx, signals):
    p = await _make_pool(ctx, slots=2, recycle_max_tasks=1, end_grace_s=0.05)
    procs = await _block_in_background(p, "a", 0)
    assert [await _run_fresh(p, "b"), await _run_fresh(p, "c")] == [1, 1]
    await _close(p, procs)


@pytest.mark.befund("FS-23")
async def test_with_one_slot_blocked_the_other_still_delivers(ctx, signals):
    p = await _make_pool(ctx, slots=2, recycle_max_tasks=1, end_grace_s=0.05)
    procs = await _block_in_background(p, "a", 0)
    assert await _run_fresh(p, "b") == 1
    assert p.healthy
    await _close(p, procs)


@pytest.mark.befund("FS-23")
async def test_with_every_slot_blocked_in_background_run_is_slot_unavailable(ctx, signals):
    p = await _make_pool(ctx, slots=2, recycle_max_tasks=1, end_grace_s=0.05)
    procs = await _block_in_background(p, "a", 0)
    procs += await _block_in_background(p, "b", 1)
    with pytest.raises(SlotUnavailable):
        await p.run(_job("c"), use_di=False)
    assert not p.healthy
    await _close(p, procs)


@pytest.mark.befund("FS-23")
async def test_a_waiting_run_wakes_with_slot_unavailable_when_the_last_slot_is_blocked_in_background(ctx, signals):
    """Review U19: der Wartende hat den Index des gesperrten Slots 0 schon verworfen und
    wartet auf Slot 1, der nicht frei wird (frisches Kind rechnet). Sperrt das
    Recycling Slot 1 im Hintergrund, wecken ihn die Poison-Werte.

    Laufzeit rund 1,3 s: ``end_grace_s=0.5`` gibt dem Aufbau (Slot 1 belegen, Wartenden
    einreihen) sicheren Vorlauf vor der zweiten Sperre und gilt fuer beide Sperren."""
    p = await _make_pool(ctx, slots=2, recycle_max_tasks=1, end_grace_s=0.5)
    procs = await _block_in_background(p, "a", 0)
    # Slot 1: DI-Job, das alte Kind ueberlebt sein Ende; die Sperre folgt erst nach end_grace_s.
    n = len(FakeCtx.started)
    t_old = asyncio.create_task(p.run(_job("b"), use_di=True))
    old = await _started(n + 1)
    old.behaviour = "immortal"
    await _ready(p, old, 1)
    old.channel.send(("result", _result("b", 1)))
    await t_old
    replacement = await _started(n + 2)
    await _ready(p, replacement, 1)
    procs += [old, replacement]
    # Slot 1 belegen (der Wartende verwirft dabei Index 0) und warten lassen.
    busy = asyncio.create_task(p.run(_job("c"), use_di=False))
    fresh = await _started(n + 3)
    await _ready(p, fresh, 1)
    procs.append(fresh)
    waiter = asyncio.create_task(p.run(_job("d"), use_di=False))
    done, _ = await asyncio.wait({waiter}, timeout=0.05)
    assert not done, "Voraussetzung: der Wartende haengt an der Freiliste"
    assert p.stats.blocked_slots == {0}, "Voraussetzung: Slot 1 ist noch nicht gesperrt"
    with pytest.raises(SlotUnavailable):
        await asyncio.wait_for(waiter, timeout=2.0)
    assert p.stats.blocked_slots == {0, 1}
    assert not p.healthy
    fresh.channel.send(("result", _result("c", 1, value="c")))
    assert (await busy).value == "c"
    await _close(p, procs)
