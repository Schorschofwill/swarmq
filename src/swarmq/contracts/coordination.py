"""Koordinationsverträge (Entwurf §2.4, KTD4): ``LockStore``,
``RateLimitStore``, ``ChordStore``, ``ScheduleStore`` samt Token-Typen.

``ScheduleStore`` arbeitet auf ``bytes`` plus Fire-Zeitpunkt, damit die
Implementierung ``scheduler.definition`` nicht importieren muss.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class LockToken:
    """Nur ``LockStore.acquire`` gibt Tokens aus; ``release``/``renew``
    nehmen ausschließlich Tokens (Owner-Check strukturell, Entwurf §4)."""

    name: str
    owner: str
    max_concurrency: int


@runtime_checkable
class LockStore(Protocol):
    async def acquire(self, name: str, *, owner: str, ttl_ms: int, max_concurrency: int) -> LockToken | None:
        """``None`` bei Konflikt."""

    async def release(self, token: LockToken) -> bool: ...

    async def renew(self, token: LockToken, ttl_ms: int) -> bool:
        """Nur für ``max_concurrency == 1``."""

    async def force_release(self, name: str, *, max_concurrency: int) -> bool:
        """Operator-Pfad ohne Owner-Check."""

    async def current_owner(self, name: str) -> str | None: ...


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    remaining: int
    retry_after_ms: int
    """≥ 1, wenn nicht erlaubt; 0 sonst."""


@runtime_checkable
class RateLimitStore(Protocol):
    async def check(self, name: str, *, max_requests: int, window_ms: int) -> RateLimitDecision:
        """Zählt den Aufruf, wenn erlaubt; meldet sonst die Restwartezeit.

        Ist das Backend nicht erreichbar (Verbindung, Timeout), wirft die
        Implementierung ``BackendConnectionError`` -- nur dann stellt der
        Worker die Nachricht mit Backoff zurück. Jeder andere Fehler (auch
        ``BackendResponseError``) führt zum Requeue an derselben Stelle."""


@runtime_checkable
class ChordStore(Protocol):
    async def record_member(self, wf_id: str, chord_id: str, member_id: str, *, expected: int, ttl_ms: int) -> bool:
        """Idempotent; ``True`` bei jedem Aufruf, sobald ``expected`` erreicht ist."""


@runtime_checkable
class ScheduleStore(Protocol):
    async def add(self, name: str, payload: bytes, fire_ts_ms: int | None) -> bool:
        """Insert-if-absent; ``False``, wenn ``name`` existiert."""

    async def put(self, name: str, payload: bytes, fire_ts_ms: int | None) -> None:
        """Überschreibt."""

    async def exists(self, name: str) -> bool: ...

    async def remove(self, name: str) -> bool: ...

    async def get(self, name: str) -> bytes | None: ...

    async def list_all(self) -> Mapping[str, bytes]: ...

    async def disable(self, name: str, payload: bytes) -> bool:
        """Atomar: Definition ersetzen und aus dem Fire-Index nehmen."""

    async def poll_due(self, now_ms: int, *, limit: int = 100) -> list[tuple[str, int, bytes]]:
        """``(name, fire_ts_ms, payload)`` aller fälligen Einträge."""

    async def commit_fire(
        self,
        name: str,
        *,
        next_fire_ts_ms: int | None,
        payload: bytes | None,
        owner: str,
        fired_ts_ms: int | None = None,
    ) -> bool:
        """Owner-Check gegen den Leader im selben atomaren Schritt;
        ``payload=None`` löscht die Definition (One-Shot).

        Mit ``fired_ts_ms`` (dem gefeuerten Index-Score) greift der Commit
        nur, wenn der Eintrag noch mit genau diesem Score im Fire-Index
        steht; wurde der Zeitplan seit dem Poll deaktiviert, entfernt oder
        umgeplant, wird der Commit verworfen. ``False`` heißt nur
        „Owner-Check abgelehnt“ — ein verworfener Commit liefert ``True``.
        Grenze: entfernt und mit demselben Score neu angelegt, ist der
        Eintrag nicht vom gefeuerten zu unterscheiden; der Commit greift.
        Der Scheduler übergibt ``fired_ts_ms`` immer -- Stores Dritter müssen
        das Schlüsselwort annehmen."""

    async def mark_fire_completed(self, fire_key: str, ttl_ms: int) -> bool: ...

    async def claim_fire(
        self, fire_key: str, ttl_ms: int, *, owner: str | None = None,
        takeover: str | Sequence[str] | None = None,
    ) -> bool:
        """In-Flight-Claim mit Owner-Token (atomar). ``True``, wenn der Claim
        frei war oder sein Wert eines der ``takeover``-Tokens ist (ein Token
        oder mehrere) -- dann gehoert er danach ``owner`` mit neuer TTL
        (Uebernahme durch die eigene Kopie). Ohne ``owner`` speichert der
        Claim einen Platzhalter."""

    async def release_fire_claim(self, fire_key: str, *, owner: str | None = None) -> None:
        """Gibt den In-Flight-Claim frei; mit ``owner`` nur, wenn er noch
        diesem Token gehoert (atomar)."""

    async def is_fire_completed(self, fire_key: str) -> bool: ...

    async def set_handover_marker(self, owner: str, ttl_ms: int) -> None: ...

    async def take_handover_marker(self) -> bool: ...
