"""Zeitbasierter Leader-Ruecktritt (FS-31, Plan 2026-10-14-0748 U14, KTD11).

Der Leader merkt sich den Start des letzten erfolgreichen Renews. Die
Zeitgrenze liegt bei ``Start + lock_ttl - Marge``; jeder Renew ist auf
``min(backend_op_timeout, Restzeit bis zur Grenze)`` begrenzt. Ist die Grenze
ohne erfolgreichen Renew erreicht, tritt der Leader zurueck, unabhaengig von
der Fehlerzahl (E-048, Zaehlregel bleibt daneben bestehen).

Die Wartezeiten laufen auf einer virtuellen Uhr (``_virtual_time``).

Testliste:
# [x] Renew-Erfolg bei t0, danach haengende Renews -> Ruecktritt vor t0 + ttl,
#     frueher als die Zaehlregel allein
# [x] Renew-Timeout zaehlt als Fehler (``scheduler_renew_error``, TimeoutError)
# [x] lock_ttl = 2 x heartbeat, erfolgreiche Renews -> bleibt Leader
# [x] langsame, aber erfolgreiche Renews (4 s) -> bleibt Leader
# [x] backend_op_timeout >= lock_ttl, erfolgreiche Renews -> bleibt Leader
# [x] Uebernahme, danach haengende Renews -> Ruecktritt vor Erwerb + ttl
# [x] Verlaengerung nach der Uebernahme haengt -> begrenzt, bleibt Leader (BZ-07)
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from unittest.mock import MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import LockToken
from swarmq.scheduler import SchedulerLoop
from swarmq.signals import Signal, SignalDispatcher
from tests.unit.scheduler._helpers import _make_loop  # pyright: ignore[reportUnknownVariableType] - ungetypter Bestandshelfer
from tests.unit.scheduler._virtual_time import run_virtual

pytestmark = [pytest.mark.timeout(5)]


def _config(**overrides: Any) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://localhost", backend_url="valkey://localhost", **overrides)


async def _hang() -> None:
    await asyncio.Event().wait()


class _Probe:
    """Beobachtet den Zustand des Loops auf der virtuellen Zeitachse: erster
    Zeitpunkt nach Amtsantritt, zu dem er nicht mehr Leader ist."""

    def __init__(self, loop: SchedulerLoop) -> None:
        self._loop = loop
        self.took_office_at: float | None = None
        self.left_office_at: float | None = None

    async def watch(self, until: float, step: float = 0.1) -> None:
        clock = asyncio.get_running_loop()
        while clock.time() < until:
            leading = self._loop.state == "leader"
            if leading and self.took_office_at is None:
                self.took_office_at = clock.time()
            if not leading and self.took_office_at is not None and self.left_office_at is None:
                self.left_office_at = clock.time()
            await asyncio.sleep(step)


def _run_election(loop: SchedulerLoop, duration: float, step: float = 0.1) -> _Probe:
    probe = _Probe(loop)

    async def _main() -> None:
        election = asyncio.create_task(loop._election_loop())
        await probe.watch(duration, step)
        election.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await election

    run_virtual(_main)
    return probe


@pytest.mark.entscheidung("E-048")
def test_hanging_renews_after_a_success_step_down_before_the_lock_ttl(
    backend: MagicMock,
    broker: MagicMock,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
) -> None:
    """Defaults (TTL 30, Heartbeat 10, op_timeout 5): Die Zaehlregel allein
    traete erst bei t0 + 30 zurueck, genau beim TTL-Ablauf."""
    config = _config()
    calls = 0

    async def _renew(_token: LockToken, _ttl_ms: int) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            return True
        await _hang()
        return True

    lock_manager.renew.side_effect = _renew
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"

    probe = _run_election(loop, config.scheduler_lock_ttl + 5)

    assert probe.left_office_at is not None
    assert probe.left_office_at < config.scheduler_lock_ttl - 1


@pytest.mark.parametrize(
    ("lock_ttl", "heartbeat", "op_timeout"),
    [
        pytest.param(20.0, 10.0, 5.0, id="ttl-2x-heartbeat"),
        pytest.param(1.0, 0.05, 5.0, id="op-timeout-over-ttl"),
        pytest.param(8.0, 4.0, 5.0, id="op-timeout-over-ttl-minus-heartbeat"),
    ],
)
def test_successful_renews_keep_the_leader_in_office(
    backend: MagicMock,
    broker: MagicMock,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
    lock_ttl: float,
    heartbeat: float,
    op_timeout: float,
) -> None:
    """Die Zeitgrenze darf einen gesunden Leader nie vor dem naechsten
    regulaeren Renew treffen, auch nicht bei grosszuegigem op_timeout."""
    config = _config(scheduler_lock_ttl=lock_ttl, scheduler_heartbeat_interval=heartbeat, backend_op_timeout=op_timeout)
    lock_manager.renew.return_value = True
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"

    probe = _run_election(loop, 20 * heartbeat, step=heartbeat / 4)

    assert probe.took_office_at is not None
    assert probe.took_office_at < 1
    assert probe.left_office_at is None
    assert lock_manager.renew.await_count >= 15


def _renew_once_then_hang() -> Any:
    calls = 0

    async def _renew(_token: LockToken, _ttl_ms: int) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            return True
        await _hang()
        return True

    return _renew


@pytest.mark.entscheidung("E-048")
def test_hanging_renews_after_a_takeover_step_down_before_the_lock_ttl(
    backend: MagicMock,
    broker: MagicMock,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
) -> None:
    """Die Verlaengerung direkt nach dem Erwerb (BZ-07) setzt die Grenze."""
    config = _config()
    lock_manager.renew.side_effect = _renew_once_then_hang()
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

    probe = _run_election(loop, config.scheduler_lock_ttl + 5)

    assert probe.took_office_at is not None
    assert probe.took_office_at < 1
    assert probe.left_office_at is not None
    assert probe.left_office_at < config.scheduler_lock_ttl - 1


def test_a_renew_timeout_counts_as_renew_error(
    backend: MagicMock,
    broker: MagicMock,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
    swarmq_logs: list[dict[str, Any]],
) -> None:
    config = _config()

    async def _hanging_renew(_token: LockToken, _ttl_ms: int) -> bool:
        await _hang()
        return True

    lock_manager.renew.side_effect = _hanging_renew
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"

    async def _main() -> float:
        await loop._election_tick()
        return asyncio.get_running_loop().time()

    returned_at = run_virtual(_main)

    assert returned_at == pytest.approx(config.backend_op_timeout)
    [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_renew_error"]
    assert entry["error_type"] == "TimeoutError"
    assert entry["consecutive_failures"] == 1
    assert loop.state == "leader"


def test_slow_but_successful_renews_keep_the_leader_in_office(
    backend: MagicMock,
    broker: MagicMock,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
) -> None:
    config = _config()

    async def _slow_renew(_token: LockToken, _ttl_ms: int) -> bool:
        await asyncio.sleep(4.0)
        return True

    lock_manager.renew.side_effect = _slow_renew
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"

    probe = _run_election(loop, 300.0)

    assert probe.left_office_at is None
    assert lock_manager.renew.await_count >= 20


def test_a_hanging_renew_right_after_takeover_is_bounded_and_keeps_the_role(
    backend: MagicMock,
    broker: MagicMock,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
    swarmq_logs: list[dict[str, Any]],
) -> None:
    """BZ-07/E-048: Die gescheiterte Verlaengerung kostet die Rolle nicht,
    blockiert den Wahl-Takt aber auch nicht laenger als ``op_timeout``."""
    config = _config()

    async def _hanging_renew(_token: LockToken, _ttl_ms: int) -> bool:
        await _hang()
        return True

    lock_manager.renew.side_effect = _hanging_renew
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

    async def _main() -> float:
        await loop._election_tick()
        return asyncio.get_running_loop().time()

    returned_at = run_virtual(_main)

    assert returned_at == pytest.approx(config.backend_op_timeout)
    assert loop.state == "leader"
    [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_renew_error"]
    assert entry["error_type"] == "TimeoutError"


def test_the_deadline_stepdown_is_logged_and_signalled_once(
    backend: MagicMock,
    broker: MagicMock,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
    swarmq_logs: list[dict[str, Any]],
) -> None:
    """Heartbeat 5 s: Die Zaehlregel (Grenze 5) greift hier nicht vor der Zeit."""
    config = _config(scheduler_heartbeat_interval=5.0)
    standby: list[dict[str, Any]] = []

    @signal_dispatcher.on_signal(Signal.SCHEDULER_STANDBY)
    async def _on_standby(**kw: Any) -> None:
        standby.append(kw)

    lock_manager.renew.side_effect = _renew_once_then_hang()
    lock_manager.acquire.return_value = None
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"

    _run_election(loop, config.scheduler_lock_ttl + 5)

    assert loop.state == "standby"
    assert len(standby) == 1
    assert [e["event"] for e in swarmq_logs if e["event"].endswith("_stepdown")] == [
        "scheduler_renew_deadline_stepdown"
    ]
