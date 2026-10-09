"""Valkey-Backend (Paket seit Plan U17; Koordinationsverträge seit U18).

``ValkeyBackend`` erfüllt ``swarmq.contracts.ResultBackend`` und bietet die
Koordinationsobjekte als nicht-optionale Properties ``.locks``,
``.rate_limit``, ``.chords``, ``.schedules`` (KTD4) — vor ``connect()``
werfen sie ``BackendConnectionError``. Der Importpfad
``swarmq.backend.valkey.ValkeyBackend`` bleibt gültig; die Fassade
re-exportiert weder ``valkey`` noch ``logger`` (Tests patchen die Submodule).

Die heutigen Schedule-/Schedfire-Methodennamen bleiben als Aliase, die an
``.schedules`` delegieren (KTD10; entfallen in U15).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from swarmq.backend.valkey.chords import ChordManager
from swarmq.backend.valkey.execution_guard import ValkeyExecutionGuardStore
from swarmq.backend.valkey.locks import ValkeyLockStore
from swarmq.backend.valkey.rate_limit import ValkeyRateLimitStore
from swarmq.backend.valkey.results import ValkeyResults
from swarmq.backend.valkey.schedules import (
    ValkeyScheduleStore,
    fired_key,
    handover_key,
    inflight_key,
)
from swarmq.locking import SCHEDULER_LOCK_NAME


class ValkeyBackend(ValkeyResults):
    _locks: ValkeyLockStore | None = None
    _rate_limit: ValkeyRateLimitStore | None = None
    _chords: ChordManager | None = None
    _schedules: ValkeyScheduleStore | None = None
    _execution_guard: ValkeyExecutionGuardStore | None = None

    # ------------------------------------------------------------------
    # Koordinationsobjekte (KTD4)
    # ------------------------------------------------------------------

    @property
    def locks(self) -> ValkeyLockStore:
        if self._locks is None:
            self._locks = ValkeyLockStore(self.client, keys=self._keys)
        return self._locks

    @property
    def rate_limit(self) -> ValkeyRateLimitStore:
        if self._rate_limit is None:
            self._rate_limit = ValkeyRateLimitStore(self.client, keys=self._keys)
        return self._rate_limit

    @property
    def chords(self) -> ChordManager:
        if self._chords is None:
            self._chords = ChordManager(self.client, keys=self._keys)
        return self._chords

    @property
    def schedules(self) -> ValkeyScheduleStore:
        if self._schedules is None:
            self._schedules = ValkeyScheduleStore(self.client, keys=self._keys)
        return self._schedules

    @property
    def execution_guard(self) -> ValkeyExecutionGuardStore:
        if self._execution_guard is None:
            self._execution_guard = ValkeyExecutionGuardStore(self.client, keys=self._keys)
        return self._execution_guard

    async def close(self) -> None:
        await super().close()
        # Die Stores halten den alten Client; nach close() neu aufbauen.
        self._locks = self._rate_limit = self._chords = self._schedules = None
        self._execution_guard = None

    # ------------------------------------------------------------------
    # Aliase der heutigen Namen (KTD10; entfallen in U15)
    # ------------------------------------------------------------------



    def _sched_fired_key(self, schedfire_key: str) -> str:
        return fired_key(schedfire_key)

    def _sched_inflight_key(self, schedfire_key: str) -> str:
        return inflight_key(schedfire_key)

    def _sched_handover_key(self, owner_token: str) -> str:
        return handover_key(owner_token)















    async def get_scheduler_leader(self) -> str | None:
        """Return the current scheduler-lock holder, or ``None`` if unlocked.
        Does NOT swallow connection errors (Decision E4)."""
        return await self.locks.current_owner(SCHEDULER_LOCK_NAME)


__all__ = ["ValkeyBackend"]

if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import ResultBackend

    _conforms_backend: type[ResultBackend] = ValkeyBackend
