"""Stuendliche Pruefung der Valkey-Umgebung durch den Leader (Plan 0.12.0, MR 14, BZ-04).

Der Worker prueft ``maxmemory-policy`` beim Verbinden. Eine spaeter
geaenderte Policy (Managed-Dienst, Betreiber) und der Speicherstand faellen
erst der Leader auf: im Uebernahme-Takt hoechstens einmal je
``VALKEY_RECHECK_INTERVAL_S``. Die Warnung bei Befund gibt das Backend aus;
das Signal ``SCHEDULER_VALKEY_CHECKED`` speist die Gauge
``swarmq_valkey_memory_used_ratio`` (bei ``noeviction`` haelt ein OOM alle
Apps auf diesem Valkey an).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from swarmq.logging import get_logger
from swarmq.signals import Signal
from swarmq.valkey_policy import EvictionCheck

logger = get_logger(__name__)

VALKEY_RECHECK_INTERVAL_S = 3600.0


class SchedulerValkeyWatch:
    # Mixin ueber dem Loop-Zustand (``_backend`` setzt ``SchedulerLoop.__init__``).
    _backend: Any
    _valkey_checked_at: float | None = None

    if TYPE_CHECKING:

        async def _emit(self, signal: Signal, **extra: Any) -> None: ...

    async def _maybe_recheck_valkey(self) -> None:
        """Leader-Takt: erneut pruefen, wenn die letzte Pruefung eine Stunde
        her ist. Wirft nie; ein Backend ohne ``recheck_eviction`` laesst sie
        aus (Q15)."""
        now = time.monotonic()
        if self._valkey_checked_at is not None and now - self._valkey_checked_at < VALKEY_RECHECK_INTERVAL_S:
            return
        self._valkey_checked_at = now
        try:
            check = await self._backend.recheck_eviction()
        except Exception as exc:
            logger.debug("valkey_recheck_unavailable", error_type=type(exc).__name__, error=str(exc))
            return
        if not isinstance(check, EvictionCheck):
            return
        await self._emit(
            Signal.SCHEDULER_VALKEY_CHECKED,
            status=check.status,
            policy=check.policy,
            maxmemory=check.maxmemory,
            used_memory=check.used_memory,
        )
