"""Leader-Version (Plan 2026-10-08-1541, KTD11): ein Leader vor swarmq 0.10
kennt keine Zeitzonen und verwirft ``timezone`` bei jedem Fire. Nicht-Leader
erkennen ihn am fehlenden Versions-Key neben dem Leader-Lock oder an einer
Version unter 0.10. Ein älterer Leader ab 0.10 (rollendes Upgrade) ist kein
Fall für die Warnung."""

from __future__ import annotations

import importlib.metadata
import re
from typing import TYPE_CHECKING, Any, Literal

from swarmq.config import SwarmQConfig
from swarmq.logging import _error_fields, get_logger
from swarmq.signals import Signal

logger = get_logger(__name__)

_RELEASE = re.compile(r"\d+(?:\.\d+)*")

# Erste swarmq-Version, deren Leader die Zeitzone eines Crons beachtet.
TIMEZONE_FLOOR = (0, 10)


def _release(version: str) -> tuple[int, ...] | None:
    """Numerischer Release-Teil (``0.10.0rc1`` → ``(0, 10, 0)``); ``None``,
    wenn die Version nicht mit einer Zahl beginnt."""
    match = _RELEASE.match(version)
    if match is None:
        return None
    parts = [int(part) for part in match.group().split(".")]
    while len(parts) > 1 and parts[-1] == 0:
        parts.pop()  # 0.10 == 0.10.0
    return tuple(parts)


def swarmq_version() -> str:
    """Installierte swarmq-Version aus den Paket-Metadaten; ``"unknown"``
    ohne Installation (wie ``swarmq --version``), dann nicht vergleichbar."""
    try:
        return importlib.metadata.version("swarmq")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def is_outdated(leader_version: str | None) -> bool:
    """``True``, wenn der Leader keine Zeitzonen kennt: ohne Versionsangabe
    (vor 0.10.0) oder mit einem Release unter 0.10. Ein älterer Leader ab
    0.10 (rollendes Upgrade) gilt nicht als veraltet; eine nicht lesbare
    Leader-Version ebenfalls nicht."""
    if leader_version is None:
        return True
    leader = _release(leader_version)
    return leader is not None and leader < TIMEZONE_FLOOR


class LeaderVersionWatch:
    """Beobachtung des Leader-Locks durch einen Nicht-Leader (KTD11).

    :meth:`observe` liefert ``"outdated"`` einmal pro veraltetem Leader
    (Owner-Token und Versionsangabe), und zwar erst bei der zweiten
    Beobachtung in Folge: ein 0.10.0-Leader schreibt seinen Versions-Key
    einen Roundtrip nach dem Lock-Erwerb, dazwischen sähe er veraltet aus.
    Folgt einem gemeldeten ein aktueller Leader, liefert sie ``"current"``.
    """

    def __init__(self, own_version: str | None = None) -> None:
        # *own_version* ist seit der Untergrenzen-Regel ohne Wirkung; der
        # Parameter bleibt, bis der Loop ihn nicht mehr übergibt.
        del own_version
        self._pending: tuple[str, str | None] | None = None
        self._reported: tuple[str, str | None] | None = None

    def observe(self, owner: str | None, version: str | None) -> Literal["outdated", "current"] | None:
        if owner is None:
            self._pending = None
            return None
        if not is_outdated(version):
            self._pending = None
            if self._reported is None:
                return None
            self._reported = None
            return "current"
        seen = (owner, version)
        if seen == self._reported:
            return None
        if seen != self._pending:
            self._pending = seen
            return None
        self._reported = seen
        return "outdated"


class SchedulerLeaderVersion:
    # Mixin über dem Loop-Zustand (wie ``SchedulerCronSync``): die Attribute
    # setzt ``SchedulerLoop.__init__``.
    _backend: Any
    _config: SwarmQConfig
    _owner_token: str
    _version: str
    _leader_watch: LeaderVersionWatch

    if TYPE_CHECKING:
        async def _emit(self, signal: Signal, **extra: Any) -> None: ...

    async def _write_leader_version(self) -> None:
        """Eigene Version neben das Leader-Lock, mit dessen TTL. Ein Fehler
        kostet nicht die Leader-Rolle; die nächste Verlängerung schreibt neu."""
        try:
            await self._backend.schedules.set_leader_version(
                self._owner_token, self._version,
                ttl_ms=int(self._config.scheduler_lock_ttl * 1000),
            )
        except Exception as exc:
            logger.warning("scheduler_leader_version_write_failed", **_error_fields(exc))

    async def _check_leader_version(self) -> None:
        """Nicht-Leader: Leader-Lock samt Versions-Key beobachten."""
        try:
            info = await self._backend.schedules.leader_info()
        except Exception as exc:
            logger.warning("scheduler_leader_version_check_failed", **_error_fields(exc))
            return
        await self._observe_leader(info.owner, info.version)

    async def _observe_leader(self, owner: str | None, version: str | None) -> None:
        """Veralteten Leader melden (einmal pro Wechsel), die Meldung
        zurücknehmen, sobald ein aktueller Leader folgt."""
        verdict = self._leader_watch.observe(owner, version)
        if verdict is None:
            return
        if verdict == "outdated":
            logger.warning(
                "scheduler_leader_outdated",
                leader=owner,
                leader_version=version,
                version=self._version,
                hint=(
                    "the scheduler leader runs a swarmq before 0.10 and drops "
                    "a cron's timezone on every fire; upgrade every app on "
                    "this Valkey to 0.10 or later before changing crons or "
                    "timezones"
                ),
            )
        await self._emit(
            Signal.SCHEDULER_LEADER_OUTDATED,
            leader=owner, leader_version=version, outdated=verdict == "outdated",
        )
