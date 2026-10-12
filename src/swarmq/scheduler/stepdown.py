"""Ruecktritt des Leaders nach wiederholten Publish-Fehlern (Plan 0.12.0, MR 15, BZ-10).

Ohne Namensraum teilen sich alle Apps eines Valkey einen Leader; dessen Wahl
haengt nur an Valkey. Scheitert sein Broker (Ausfall, fehlende
Schreibrechte), hielt er bisher die Crons aller Apps an. Nach mindestens 2
Publish-Fehlern ueber mindestens ``scheduler_publish_stepdown_s`` gibt er das
Lock frei (ohne Handover-Marker: der Nachfolger meldet FAILOVER) und bewirbt
sich 2 x ``scheduler_lock_ttl`` nicht erneut.

Nicht gezaehlt wird, was jeden Kandidaten gleich trifft oder an einer Queue
haengt (``_NOT_COUNTED``): ein anderer vhost ohne die Queue endet als
``UnroutableMessage``, ein Broker-Alarm als ``BrokerPublishBlocked``, ein
negatives Confirm einer Queue als ``PublishRejected`` (Review MR 15, P2-2),
ausbleibende Confirms als ``PublishIndeterminate`` (Georg, 2026-10-11).
Die Serie beginnt neu, wenn zwischen zwei Fehlern mehr als die Frist liegt,
und mit jeder neuen Amtszeit (Review MR 15, P2-1).

Mixin ueber dem ``SchedulerLoop``-Zustand; Logger ueber ``self._log``
(``swarmq.scheduler.loop``, Q17). Die Uhr ist ``time.monotonic`` dieses
Moduls.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from swarmq.exceptions import (
    BrokerConnectionError,
    BrokerPublishBlocked,
    MessageHeadersTooLarge,
    PublishIndeterminate,
    PublishRejected,
    UnroutableMessage,
)
from swarmq.logging import _error_fields
from swarmq.signals import Signal

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.contracts.coordination import LockStore, LockToken
    from swarmq.scheduler.unroutable import _SchedulerState

# Mindestzahl Publish-Fehler in einer Serie vor dem Ruecktritt.
_MIN_FAILURES_FOR_STEPDOWN = 2

# Fehler, die jeden Kandidaten gleich treffen: ein Ruecktritt liesse die
# Fuehrung nur rotieren. ``BrokerPublishBlocked``: ein Broker-Alarm blockiert
# jede publizierende Verbindung (Plan 0.12.0, MR 16). ``PublishRejected``:
# die Ablehnung haengt an einer Queue, nicht an der Verbindung; ein einzelner
# kaputter Cron kostet nicht die Fuehrung (Review MR 15, P2-2).
# ``PublishIndeterminate``: ausbleibende Confirms einer Queue ebenso (von
# Georg am 2026-10-11 entschieden).
_NOT_COUNTED: tuple[type[BaseException], ...] = (
    UnroutableMessage,
    MessageHeadersTooLarge,
    BrokerPublishBlocked,
    PublishRejected,
    PublishIndeterminate,
)


class PublishStepdown:
    """Zaehlt Publish-Fehler in Folge; ``failure`` meldet den Ruecktritt."""

    def __init__(self, window_s: float, *, clock: Callable[[], float] | None = None) -> None:
        self._window_s = window_s
        self._clock = clock if clock is not None else _monotonic
        self._count = 0
        self._first_at = 0.0
        self._last_at = 0.0

    def failure(self, exc: BaseException) -> bool:
        if self._window_s <= 0:
            return False  # 0 schaltet den Ruecktritt ab
        if not isinstance(exc, BrokerConnectionError) or isinstance(exc, _NOT_COUNTED):
            return False
        now = self._clock()
        if self._count == 0 or now - self._last_at > self._window_s:
            # Review MR 15, P2-1: ein laengerer Abstand als die Frist beginnt
            # eine neue Serie (vereinzelte Fehler ueber Stunden zaehlen nicht).
            self._count = 0
            self._first_at = now
        self._last_at = now
        self._count += 1
        if self._count >= _MIN_FAILURES_FOR_STEPDOWN and now - self._first_at >= self._window_s:
            self._count = 0
            return True
        return False

    def success(self) -> None:
        self._count = 0


def _monotonic() -> float:
    return time.monotonic()


class SchedulerStepdown:
    # Mixin ueber dem Loop-Zustand: die Attribute setzt ``SchedulerLoop.__init__``.
    if TYPE_CHECKING:
        _config: SwarmQConfig
        _lock_manager: LockStore
        _lock_token: LockToken
        _state: _SchedulerState
        _prev_leader_seen: bool

        @property
        def _log(self) -> Any: ...

        async def _emit(self, signal: Signal, **extra: Any) -> None: ...

    def _init_stepdown(self) -> None:
        self._publish_stepdown = PublishStepdown(self._config.scheduler_publish_stepdown_s or 0.0)
        self._stepdown_until = 0.0

    def _note_publish_ok(self) -> None:
        self._publish_stepdown.success()

    def _reset_publish_stepdown(self) -> None:
        """Neue Amtszeit: Fehler einer frueheren zaehlen nicht (Review MR 15,
        P2-1). Jeder Weg zurueck zum Leader beginnt eine neue Amtszeit, damit
        gilt das auch fuer Fehler vor einem Uebergang nach Standby."""
        self._publish_stepdown.success()

    async def _note_publish_failure(self, exc: BaseException) -> None:
        if self._state != "leader" or not self._publish_stepdown.failure(exc):
            return
        cooldown_s = 2 * self._config.scheduler_lock_ttl
        self._log.warning(
            "scheduler_publish_failures_stepdown",
            **_error_fields(exc),
            cooldown_s=cooldown_s,
            hint=(
                "this leader's broker keeps failing; it releases the scheduler lock so a worker "
                "with a working broker can fire the crons (SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S)"
            ),
        )
        self._state = "standby"
        self._prev_leader_seen = True
        self._stepdown_until = _monotonic() + cooldown_s
        try:
            await self._lock_manager.release(self._lock_token)
        except Exception as release_exc:  # noqa: BLE001 - Lock läuft per TTL ab
            # Das Lock laeuft dann per TTL ab; der Standby bleibt.
            self._log.warning("scheduler_lock_release_failed", **_error_fields(release_exc))
        await self._emit(Signal.SCHEDULER_STANDBY)

    def _in_stepdown_cooldown(self) -> bool:
        """Nach einem Ruecktritt 2 x Lock-TTL keine Bewerbung (BZ-10)."""
        return _monotonic() < self._stepdown_until
