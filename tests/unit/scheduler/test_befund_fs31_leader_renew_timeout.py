"""Nachweis FS-31: Der Leader-Renew laeuft ohne ``op_timeout``; der
Ruecktritt nach Renew-Fehlern greift erst lange nach Ablauf des Leader-Locks.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-31, abgetrennt von FS-06; Quellen K-3, MC-4).
Teil 1 (Wiederholung nicht idempotenter Befehle) steht in
``tests/integration/backend/test_befund_fs06_retry_after_lost_reply.py``.

``SchedulerLoop._election_tick`` (``scheduler/loop.py``) ruft
``lock_manager.renew`` ohne Zeitlimit auf. Bei halb offener Verbindung oder
haengendem Valkey dauert ein Renew mit der Retry-Konfiguration des Clients
(``socket_timeout`` 10 s, 5 Wiederholungen, Backoff) rund 65 s, bevor
``BackendConnectionError`` kommt. Der Ruecktritt zaehlt nur Fehler
(Defaults: TTL 30 s, Heartbeat 10 s, Grenze 2) und greift so nach rund 150 s;
das Lock ist nach 30 s frei, ein Standby uebernimmt, der alte Leader pollt
weiter. Der Kommentar verspricht den Ruecktritt "one heartbeat BEFORE the lock
TTL can have expired". Erwartet: Ruecktritt, bevor ``scheduler_lock_ttl`` seit
dem letzten erfolgreichen Renew verstrichen ist.

Die Wartezeiten laufen auf einer virtuellen Uhr (eigene Event-Loop, deren
``time()`` beim Warten vorspringt): 30 s kosten nur Millisekunden.

Testliste:
# [x] Renew braucht 65 s bis zum Fehler (Default-Konfiguration) -> nach
#     ``scheduler_lock_ttl`` ist der Leader nicht mehr Leader
"""

from __future__ import annotations

import asyncio
import contextlib
import selectors
from collections.abc import Callable, Coroutine
from typing import Any, override
from unittest.mock import MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import LockToken
from swarmq.exceptions import BackendConnectionError
from swarmq.signals import SignalDispatcher
from tests.unit.scheduler._helpers import _make_loop  # pyright: ignore[reportUnknownVariableType] - ungetypter Bestandshelfer

pytestmark = [pytest.mark.timeout(5)]

# socket_timeout 10 s x (1 + 5 Wiederholungen) plus Backoff (connection.py).
_RENEW_FAILURE_AFTER_S = 65.0


class _FastForwardSelector(selectors.DefaultSelector):
    """Wartet nie in Echtzeit: liegt nichts an, springt die Uhr vor."""

    def __init__(self) -> None:
        super().__init__()
        self.clock = 0.0

    @override
    def select(self, timeout: float | None = None) -> list[tuple[selectors.SelectorKey, int]]:
        ready = super().select(0)
        if ready:
            return ready
        if timeout is None:
            raise RuntimeError("virtuelle Uhr: nichts geplant, die Loop wuerde ewig warten")
        self.clock += max(timeout, 0.0)
        return []


class _VirtualTimeLoop(asyncio.SelectorEventLoop):
    def __init__(self) -> None:
        self._selector_ff = _FastForwardSelector()
        super().__init__(selector=self._selector_ff)

    @override
    def time(self) -> float:
        return self._selector_ff.clock


def _run_virtual[T](main: Callable[[], Coroutine[Any, Any, T]]) -> T:
    loop = _VirtualTimeLoop()
    try:
        return loop.run_until_complete(main())
    finally:
        loop.close()


@pytest.mark.befund("FS-31")
@pytest.mark.xfail(strict=True, reason="FS-31: offen - Leader-Renew ohne op_timeout, Ruecktritt erst nach ~150 s")
def test_leader_steps_down_before_the_lock_ttl_when_renew_hangs(
    backend: MagicMock,
    broker: MagicMock,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
) -> None:
    config = SwarmQConfig(broker_url="amqp://localhost", backend_url="valkey://localhost")

    async def _slow_failing_renew(_token: LockToken, _ttl_ms: int) -> bool:
        await asyncio.sleep(_RENEW_FAILURE_AFTER_S)
        raise BackendConnectionError("Timeout reading from socket")

    lock_manager.renew.side_effect = _slow_failing_renew
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"

    async def _main() -> str:
        election = asyncio.create_task(loop._election_loop())
        # Zeitachse der virtuellen Uhr, keine Synchronisation: das Lock ist ab hier frei.
        await asyncio.sleep(config.scheduler_lock_ttl)
        state: str = loop.state
        election.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await election
        return state

    state = _run_virtual(_main)
    assert state != "leader", (
        f"nach {config.scheduler_lock_ttl:.0f} s (Lock-TTL) haelt sich der Leader noch fuer Leader; "
        f"Renew ohne Zeitlimit, erster Fehler erst nach {_RENEW_FAILURE_AFTER_S:.0f} s"
    )
