"""Lock-Keeper (Plan 0.12.0, MR 13, BZ-25): gehaltene Task-Locks ueberleben
einen Handler, der laenger laeuft als seine Lock-TTL.

Die Lock-TTL (``hard_timeout + 30 s``) ist die Absturzfrist. Bis zur
urspruenglichen Frist ``t0 + TTL`` bleibt alles wie bisher: kein Roundtrip
(Q7). Erst wenn ein Lock in weniger als einem Fenster ablaeuft und der
Handler noch lebt, verlaengert der Keeper ihn in kurzen Schritten um ein
Fenster (Fenster = 3 Takte, Takt = ``min(max(TTL/3, 50 ms), 20 s)``). Ein per
SIGKILL beendeter Halter hinterlaesst den Lock damit hoechstens wie heute bis
``t0 + TTL``, bei Ueberziehung hoechstens ein Fenster laenger (R4).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from swarmq.logging import _error_fields, get_logger

if TYPE_CHECKING:
    from swarmq.contracts.coordination import LockStore, LockToken

logger = get_logger(__name__)

TICK_MIN_S = 0.05
TICK_MAX_S = 20.0
WINDOW_TICKS = 3


def keeper_tick_s(ttl_ms: int) -> float:
    """Takt des Keepers fuer einen Lock mit dieser TTL."""
    return min(max(ttl_ms / 3000.0, TICK_MIN_S), TICK_MAX_S)


def keeper_window_s(ttl_ms: int) -> float:
    """Fenster, in dem ein Lock vor seinem Ablauf verlaengert wird."""
    return WINDOW_TICKS * keeper_tick_s(ttl_ms)


@dataclass
class KeptLock:
    """Ein gehaltener Lock samt seiner bekannten Frist (monotone Uhr)."""

    token: LockToken
    ttl_ms: int
    task_id: str
    task_name: str
    deadline: float
    original_deadline: float
    warned_past_timeout: bool = False


class LockKeeper:
    """Verlaengert die Locks laufender Handler (Modulbeschreibung).

    Eine Lease faellt beim Freigabeversuch heraus (``drop``), auch wenn der
    Versuch scheitert. Der Keeper laeuft bis zur letzten Lease oder bis
    ``close`` (das Backend schliesst), nicht ueber das Shutdown-Event.
    """

    def __init__(
        self,
        store: Callable[[], LockStore | None],
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        now: Callable[[], float] = time.monotonic,
        op_timeout_s: float | None = None,
    ) -> None:
        self._store = store
        self._sleep = sleep
        self._now = now
        self._op_timeout_s = op_timeout_s
        self._kept: dict[LockToken, KeptLock] = {}
        self._loop: asyncio.Task[None] | None = None

    @property
    def kept(self) -> int:
        """Zahl der gehaltenen Leases."""
        return len(self._kept)

    def keeps(self, token: LockToken) -> bool:
        return token in self._kept

    def keep(self, token: LockToken, ttl_ms: int, *, task_id: str, task_name: str) -> None:
        now = self._now()
        deadline = now + ttl_ms / 1000.0
        self._kept[token] = KeptLock(token, ttl_ms, task_id, task_name, deadline, deadline)
        if self._loop is None or self._loop.done():
            self._loop = asyncio.create_task(self._run())

    def drop(self, token: LockToken) -> None:
        self._kept.pop(token, None)
        if not self._kept:
            self._stop_loop()

    def close(self) -> None:
        """Keeper beenden (vor dem Schliessen des Backends)."""
        self._kept.clear()
        self._stop_loop()

    def _stop_loop(self) -> None:
        if self._loop is not None:
            self._loop.cancel()
            self._loop = None

    async def _run(self) -> None:
        while self._kept:
            await self._sleep(min(keeper_tick_s(kept.ttl_ms) for kept in self._kept.values()))
            await self.tick_once()

    async def tick_once(self) -> None:
        """Ein Takt: jede Lease, die in weniger als einem Fenster ablaeuft,
        wird um ein Fenster verlaengert."""
        store = self._store()
        if store is None:
            return
        now = self._now()
        due = [kept for kept in self._kept.values() if kept.deadline - now < keeper_window_s(kept.ttl_ms)]
        if due:
            await asyncio.gather(*(self._extend(store, kept, now) for kept in due))

    async def _extend(self, store: Any, kept: KeptLock, now: float) -> None:
        window_ms = int(keeper_window_s(kept.ttl_ms) * 1000)
        fields = {"lock_name": kept.token.name, "task_id": kept.task_id, "task_name": kept.task_name}
        try:
            call = store.extend(kept.token, ttl_ms=window_ms)
            if self._op_timeout_s is not None:
                async with asyncio.timeout(self._op_timeout_s):
                    extended = await call
            else:
                extended = await call
        except (AttributeError, NotImplementedError, TypeError) as exc:
            # Q15: ein Store ohne ``extend`` (Dritt-Store, Mock) -- Verhalten
            # wie vor 0.12, der Lock laeuft nach seiner TTL ab.
            logger.warning("lock_extend_unsupported", **fields, **_error_fields(exc))
            self._kept.pop(kept.token, None)
            return
        except Exception as exc:
            # Naechster Takt erneut; das Fenster deckt drei Takte.
            logger.warning("lock_extend_failed", **fields, **_error_fields(exc))
            return
        if kept.token not in self._kept:
            return  # waehrend des Aufrufs freigegeben
        if extended is False:
            logger.warning(
                "task_lock_lost",
                **fields,
                stage="running",
                hint="the lock expired or was taken over while the handler runs; a second holder may run in parallel",
            )
            self._kept.pop(kept.token, None)
            return
        kept.deadline = now + window_ms / 1000.0
        if now >= kept.original_deadline and not kept.warned_past_timeout:
            kept.warned_past_timeout = True
            logger.warning(
                "task_lock_held_past_hard_timeout",
                **fields,
                lock_ttl_ms=kept.ttl_ms,
                hint="the handler outlived hard_timeout + 30 s; the lock is kept while it runs",
            )
