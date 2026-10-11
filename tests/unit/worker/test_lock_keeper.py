"""Lock-Keeper (Plan 0.12.0, MR 13, BZ-25): verlaengert gehaltene Task-Locks,
solange der Handler lebt -- erst, wenn ein Lock in weniger als einem Fenster
ablaeuft (Fenster = 3 Takte, Takt = min(max(ttl/3, 50 ms), 20 s)).

Test-Liste:
- [x] Takt: min(max(ttl/3, 50 ms), 20 s); Fenster = 3 Takte
- [x] weit vor dem Ablauf: kein Aufruf (Q7, kein Roundtrip im Normalfall)
- [x] weniger als ein Fenster vor dem Ablauf: ``extend`` um ein Fenster,
      die Frist wandert
- [x] ``extend`` liefert False -> WARNING ``task_lock_lost`` (stage=running),
      Lease faellt heraus, kein weiterer Aufruf
- [x] ``extend`` wirft -> WARNING ``lock_extend_failed``, naechster Takt erneut
- [x] Verlaengerung nach der urspruenglichen Frist t0 + TTL -> einmal WARNING
      ``task_lock_held_past_hard_timeout``
- [x] ``drop`` nimmt die Lease heraus; der Keeper endet mit der letzten Lease
- [x] Store ohne ``extend`` (Q15) -> einmal WARNING ``lock_extend_unsupported``,
      Lease faellt heraus
- [x] Review MR 13, P2-5: ``extend`` liefert weder True noch False
      (``AsyncMock``-Store, Protocol-Subklasse mit leerem Rumpf -> ``None``)
      -> nie als Erfolg werten: WARNING ``lock_extend_unsupported``, Lease
      faellt heraus
- [x] kein Store -> nichts passiert
- [x] Schleife: schlaeft den kleinsten Takt, endet mit der letzten Lease,
      verlaengert nach jedem Schlaf die faelligen Leases
- [x] Review MR 13 (Hinweis): ist die lokale Frist sicher verstrichen (alle
      Verlaengerungen scheiterten, z. B. Valkey-Ausfall), WARNING
      ``task_lock_lost`` mit ``stage="expired"`` ohne Roundtrip; die Lease
      faellt heraus
- [x] Review MR 13 (Hinweis): hoechstens 32 Verlaengerungen gleichzeitig
      (viele faellige Leases fluten Valkey nicht)
- [x] Review MR 13 (Hinweis): eine Ausnahme im Takt beendet die Schleife
      nicht; sie wird geloggt (``lock_keeper_tick_failed``)
"""

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from swarmq.contracts.coordination import LockStore, LockToken
from swarmq.worker.lock_keeper import LockKeeper, keeper_tick_s, keeper_window_s

pytestmark = pytest.mark.timeout(5)

_TOKEN = LockToken(name="resource", owner="tok-1", max_concurrency=1)


class _Store:
    """Lock-Store-Doppel: zeichnet ``extend``-Aufrufe auf."""

    def __init__(self, result: bool = True) -> None:
        self.result = result
        self.calls: list[tuple[LockToken, int]] = []
        self.raise_once: Exception | None = None

    async def extend(self, token: LockToken, ttl_ms: int) -> bool:
        self.calls.append((token, ttl_ms))
        if self.raise_once is not None:
            exc, self.raise_once = self.raise_once, None
            raise exc
        return self.result


class _Clock:
    """Monotone Fake-Uhr; ``sleep`` zeichnet den Takt auf und blockiert, damit
    die Hintergrundschleife den Tests mit ``tick_once`` nicht dazwischenlaeuft."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []
        self._block = asyncio.Event()

    def __call__(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        await self._block.wait()


def _keeper(store: Any, clock: _Clock) -> LockKeeper:
    return LockKeeper(lambda: store, sleep=clock.sleep, now=clock)


def test_tick_is_a_third_of_the_ttl_between_50_ms_and_20_s() -> None:
    assert keeper_tick_s(90_000) == pytest.approx(20.0)  # 30 s gedeckelt
    assert keeper_tick_s(3_000) == pytest.approx(1.0)
    assert keeper_tick_s(400) == pytest.approx(0.4 / 3)
    assert keeper_tick_s(60) == pytest.approx(0.05)  # Untergrenze
    assert keeper_window_s(90_000) == pytest.approx(60.0)


async def test_no_extend_while_the_lock_is_more_than_a_window_from_expiry() -> None:
    store, clock = _Store(), _Clock()
    keeper = _keeper(store, clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")  # Fenster 60 s

    clock.now += 29.0
    await keeper.tick_once()

    assert store.calls == []
    assert keeper.kept == 1
    keeper.close()


async def test_extend_by_a_window_once_less_than_a_window_remains() -> None:
    store, clock = _Store(), _Clock()
    keeper = _keeper(store, clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")

    clock.now += 31.0  # Rest 59 s < Fenster 60 s
    await keeper.tick_once()
    assert store.calls == [(_TOKEN, 60_000)]

    await keeper.tick_once()  # Frist ist jetzt now + 60 s: nicht faellig
    assert len(store.calls) == 1
    clock.now += 20.0  # ein Takt spaeter: Rest 40 s
    await keeper.tick_once()
    assert len(store.calls) == 2
    keeper.close()


async def test_lost_while_running_is_reported_once_and_the_lease_dropped(swarmq_logs) -> None:
    store, clock = _Store(result=False), _Clock()
    keeper = _keeper(store, clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    clock.now += 31.0

    await keeper.tick_once()
    await keeper.tick_once()

    assert len(store.calls) == 1
    assert keeper.kept == 0
    [lost] = [e for e in swarmq_logs if e["event"] == "task_lock_lost"]
    assert lost["log_level"] == "warning"
    assert lost["stage"] == "running"
    assert lost["lock_name"] == "resource"
    assert lost["task_id"] == "t1"


async def test_extend_failure_is_retried_next_tick(swarmq_logs) -> None:
    store, clock = _Store(), _Clock()
    store.raise_once = ConnectionError("valkey away")
    keeper = _keeper(store, clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    clock.now += 31.0

    await keeper.tick_once()
    [failed] = [e for e in swarmq_logs if e["event"] == "lock_extend_failed"]
    assert failed["log_level"] == "warning"
    assert failed["error_type"] == "ConnectionError"
    assert keeper.kept == 1
    await keeper.tick_once()
    assert len(store.calls) == 2
    assert not [e for e in swarmq_logs if e["event"] == "task_lock_lost"]
    keeper.close()


async def test_holding_past_the_original_ttl_warns_once(swarmq_logs) -> None:
    store, clock = _Store(), _Clock()
    keeper = _keeper(store, clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    for _ in range(6):  # 6 x 20 s = 120 s > TTL 90 s
        clock.now += 20.0
        await keeper.tick_once()

    past = [e for e in swarmq_logs if e["event"] == "task_lock_held_past_hard_timeout"]
    assert len(past) == 1
    assert past[0]["log_level"] == "warning"
    assert past[0]["lock_name"] == "resource"
    assert keeper.kept == 1  # weiter gehalten, solange der Handler lebt
    keeper.close()


async def test_drop_removes_the_lease_and_ends_the_keeper() -> None:
    store, clock = _Store(), _Clock()
    keeper = _keeper(store, clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    assert keeper.keeps(_TOKEN)

    keeper.drop(_TOKEN)
    clock.now += 31.0
    await keeper.tick_once()

    assert store.calls == []
    assert keeper.kept == 0
    assert not keeper.keeps(_TOKEN)
    keeper.drop(_TOKEN)  # idempotent


async def test_store_without_extend_is_reported_once_and_the_lease_dropped(swarmq_logs) -> None:
    class _Legacy:  # Dritt-Store ohne ``extend`` (Q15)
        pass

    clock = _Clock()
    keeper = _keeper(_Legacy(), clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    clock.now += 31.0

    await keeper.tick_once()
    await keeper.tick_once()

    [entry] = [e for e in swarmq_logs if e["event"] == "lock_extend_unsupported"]
    assert entry["log_level"] == "warning"
    assert entry["lock_name"] == "resource"
    assert keeper.kept == 0


async def test_asyncmock_store_is_not_an_extension(swarmq_logs) -> None:
    """Review MR 13, P2-5 (Q15): ``AsyncMock().extend`` liefert ein
    ``MagicMock`` -- das ist keine bestaetigte Verlaengerung."""
    clock = _Clock()
    keeper = _keeper(AsyncMock(), clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    clock.now += 31.0

    await keeper.tick_once()

    [entry] = [e for e in swarmq_logs if e["event"] == "lock_extend_unsupported"]
    assert entry["log_level"] == "warning"
    assert entry["lock_name"] == "resource"
    assert keeper.kept == 0


async def test_protocol_subclass_without_extend_is_not_an_extension(swarmq_logs) -> None:
    """Review MR 13, P2-5 (Q15): eine Subklasse des ``LockStore``-Protocols
    erbt ``extend`` mit leerem Rumpf (``None``) -- nie als Erfolg werten."""
    from swarmq.contracts.coordination import LockStore

    class _SubclassStore(LockStore):
        pass

    clock = _Clock()
    keeper = _keeper(_SubclassStore(), clock)  # type: ignore[abstract]  # absichtlich unvollstaendige Protocol-Subklasse
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    clock.now += 31.0

    await keeper.tick_once()
    await keeper.tick_once()

    [entry] = [e for e in swarmq_logs if e["event"] == "lock_extend_unsupported"]
    assert entry["log_level"] == "warning"
    assert entry["task_id"] == "t1"
    assert keeper.kept == 0


async def test_without_a_store_nothing_happens() -> None:
    clock = _Clock()
    keeper = LockKeeper(lambda: None, sleep=clock.sleep, now=clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    clock.now += 31.0
    await keeper.tick_once()
    assert keeper.kept == 1
    keeper.close()


async def test_loop_sleeps_the_smallest_tick_and_ends_with_the_last_lease() -> None:
    store, clock = _Store(), _Clock()
    keeper = _keeper(store, clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")  # Takt 20 s
    short = LockToken(name="short", owner="tok-2", max_concurrency=1)
    keeper.keep(short, 3_000, task_id="t2", task_name="locked")  # Takt 1 s
    loop = keeper._loop
    assert loop is not None
    await asyncio.sleep(0)  # die Schleife erreicht ihr erstes ``sleep``
    assert clock.sleeps == [pytest.approx(1.0)]

    keeper.drop(_TOKEN)
    assert not loop.done()  # eine Lease bleibt
    keeper.drop(short)
    await asyncio.sleep(0)
    assert loop.cancelled() or loop.done()


async def test_loop_ticks_after_each_sleep() -> None:
    """Die Schleife schlaeft einen Takt und verlaengert danach faellige Leases."""
    store = _Store()
    clock = _Clock()
    ticks = asyncio.Event()

    async def sleep(delay: float) -> None:
        clock.sleeps.append(delay)
        clock.now += 31.0
        if len(clock.sleeps) >= 2:
            ticks.set()
            await asyncio.Event().wait()

    keeper = LockKeeper(lambda: cast("LockStore", store), sleep=sleep, now=clock)  # Fake kennt nur ``extend``
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    await asyncio.wait_for(ticks.wait(), timeout=2)
    assert len(store.calls) == 1
    keeper.close()


async def test_loop_survives_a_failing_tick(swarmq_logs) -> None:
    """Review MR 13 (Hinweis): stirbt die Schleife, verlaengert niemand mehr."""
    clock = _Clock()
    ticks = asyncio.Event()
    calls = 0

    def store() -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        return None

    async def sleep(delay: float) -> None:
        clock.sleeps.append(delay)
        if len(clock.sleeps) >= 3:
            ticks.set()
            await asyncio.Event().wait()

    keeper = LockKeeper(store, sleep=sleep, now=clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    await asyncio.wait_for(ticks.wait(), timeout=2)

    assert calls == 2
    [failed] = [e for e in swarmq_logs if e["event"] == "lock_keeper_tick_failed"]
    assert failed["log_level"] == "warning"
    assert failed["error_type"] == "RuntimeError"
    keeper.close()


async def test_at_most_32_extensions_run_at_once() -> None:
    """Review MR 13 (Hinweis): ``gather`` ueber alle faelligen Leases ist
    begrenzt."""
    clock = _Clock()
    gate, full = asyncio.Event(), asyncio.Event()
    inflight = peak = 0

    class _Blocking:
        async def extend(self, token: LockToken, ttl_ms: int) -> bool:
            nonlocal inflight, peak
            inflight += 1
            peak = max(peak, inflight)
            if inflight == 32:
                full.set()
            await gate.wait()
            inflight -= 1
            return True

    keeper = _keeper(_Blocking(), clock)
    for n in range(40):
        keeper.keep(LockToken(f"l{n}", "o", 1), 90_000, task_id=f"t{n}", task_name="locked")
    clock.now += 31.0

    tick = asyncio.create_task(keeper.tick_once())
    await asyncio.wait_for(full.wait(), timeout=2)
    assert peak == 32
    gate.set()
    await asyncio.wait_for(tick, timeout=2)
    assert peak == 32
    assert inflight == 0
    keeper.close()


async def test_surely_expired_lock_is_reported_without_a_round_trip(swarmq_logs) -> None:
    store, clock = _Store(), _Clock()
    keeper = _keeper(store, clock)
    keeper.keep(_TOKEN, 90_000, task_id="t1", task_name="locked")
    for _ in range(3):  # Valkey weg: jeder Versuch scheitert
        store.raise_once = ConnectionError("valkey away")
        clock.now += 20.0
        await keeper.tick_once()
    calls = len(store.calls)
    assert not [e for e in swarmq_logs if e["event"] == "task_lock_lost"]

    clock.now += 31.0  # 1091 > Frist t0 + 90 s
    await keeper.tick_once()

    assert len(store.calls) == calls
    assert keeper.kept == 0
    [lost] = [e for e in swarmq_logs if e["event"] == "task_lock_lost"]
    assert lost["stage"] == "expired"
    assert lost["log_level"] == "warning"
    assert lost["task_id"] == "t1"
    assert lost["lock_name"] == "resource"
