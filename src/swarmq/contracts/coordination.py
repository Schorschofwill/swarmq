"""Koordinationsverträge (Entwurf §2.4, KTD4): ``LockStore``,
``RateLimitStore``, ``ChordStore``, ``ScheduleStore`` samt Token-Typen und
``ExecutionGuardStore`` (Plan 2026-10-08-1541, KTD5).

``ScheduleStore`` arbeitet auf ``bytes`` plus Fire-Zeitpunkt, damit die
Implementierung ``scheduler.definition`` nicht importieren muss.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class LockToken:
    """Nur ``LockStore.acquire`` gibt Tokens aus; ``release``/``renew``
    nehmen ausschließlich Tokens (Owner-Check strukturell, Entwurf §4)."""

    name: str
    owner: str
    max_concurrency: int


@dataclass(frozen=True, slots=True)
class LockObservation:
    """Was ein Wartender ueber einen Lock sieht (Plan 0.12.0, MR 13, BZ-05):
    die Halter (Mutex: der Owner; Semaphor: alle Member) und die Rest-TTL
    des Keys in ms (``-2``: kein Key, ``-1``: ohne Frist)."""

    owners: frozenset[str]
    pttl_ms: int


@runtime_checkable
class LockStore(Protocol):
    async def acquire(self, name: str, *, owner: str, ttl_ms: int, max_concurrency: int) -> LockToken | None:
        """``None`` bei Konflikt."""

    async def release(self, token: LockToken) -> bool: ...

    async def renew(self, token: LockToken, ttl_ms: int) -> bool:
        """Nur für ``max_concurrency == 1``."""

    async def extend(self, token: LockToken, ttl_ms: int) -> bool:
        """Frist des eigenen Locks (Mutex oder Semaphor-Slot) auf mindestens
        ``ttl_ms`` ab jetzt setzen -- verkuerzt nie (Plan 0.12.0, MR 13,
        BZ-25). ``False``: Lock weg, abgelaufen oder fremder Halter."""

    async def force_release(self, name: str, *, max_concurrency: int) -> bool:
        """Operator-Pfad ohne Owner-Check."""

    async def current_owner(self, name: str) -> str | None: ...

    async def observe(self, name: str, *, max_concurrency: int) -> LockObservation:
        """Halter und Rest-TTL eines Locks, ohne ihn zu veraendern."""


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


@dataclass(frozen=True, slots=True)
class CronReport:
    """Meldung eines statischen Crons durch einen Worker (Plan 2026-10-08-1541,
    KTD10). Zeiten in Backend-Zeit (ms), nicht in Worker-Uhrzeit."""

    name: str
    fingerprint: str
    first_seen_ms: int
    last_seen_ms: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class CronReports:
    """Alle Meldungen samt der Backend-Zeit des Lesens; der Leader misst die
    Lebendigkeit gegen ``now_ms``, nicht gegen die eigene Uhr."""

    now_ms: int
    reports: tuple[CronReport, ...]


@dataclass(frozen=True, slots=True)
class LeaderInfo:
    """Scheduler-Leader samt seiner swarmq-Version (Plan 2026-10-08-1541,
    KTD11). ``version`` ist ``None``, wenn der Leader keine Version neben das
    Lock geschrieben hat (Versionen vor 0.10.0)."""

    owner: str | None
    version: str | None


@runtime_checkable
class ScheduleStore(Protocol):
    async def add(self, name: str, payload: bytes, fire_ts_ms: int | None) -> bool:
        """Insert-if-absent; ``False``, wenn ``name`` existiert."""

    async def put(self, name: str, payload: bytes, fire_ts_ms: int | None) -> None:
        """Überschreibt; ``fire_ts_ms=None`` lässt den Fire-Index unberührt."""

    async def exists(self, name: str) -> bool: ...

    async def remove(self, name: str) -> bool: ...

    async def get(self, name: str) -> bytes | None: ...

    async def fire_ts(self, name: str) -> int | None:
        """Score im Fire-Index; ``None`` ohne Eintrag (deaktiviert, unbekannt)."""

    async def list_all(self) -> Mapping[str, bytes]: ...

    async def replan_if_unchanged(
        self, name: str, expected: bytes, payload: bytes, fire_ts_ms: int,
    ) -> bool:
        """Heilung eines Eintrags ohne Score, atomar: nur wenn die
        gespeicherte Definition noch genau *expected* ist und der Eintrag
        keinen Score im Fire-Index hat, wird *payload* geschrieben und
        *fire_ts_ms* eingeplant. ``False`` ohne jede Änderung, wenn
        zwischenzeitlich deaktiviert, entfernt, geändert oder eingeplant."""

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
        fired_payload: bytes | None = None,
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
        das Schlüsselwort annehmen.

        Mit ``fired_payload`` (der gepollten Definition) wird ``payload`` nur
        geschrieben, wenn die gespeicherte Definition noch genau diese ist
        (Plan 2026-10-08-1541, KTD12); sonst rückt nur der Score vor und eine
        zwischenzeitliche Übernahme bleibt stehen."""

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

    async def take_handover_marker(self, previous_owner: str | None = None) -> bool:
        """Verbraucht den Handover-Marker des letzten Leaders (``True``, wenn
        einer da war). ``previous_owner``: zuletzt gesehener Leader; erkennt
        auch den Marker eines Leaders vor 0.12, der nur unter seinem
        Owner-Token liegt (BZ-07)."""

    async def report_cron(
        self, name: str, fingerprint: str, payload: bytes, *, live_window_ms: int,
    ) -> None:
        """Meldet einen statischen Cron (KTD10), atomar und in Backend-Zeit:
        ``last_seen`` und Payload werden gesetzt; ``first_seen`` nur, wenn
        die Meldung fehlt oder ihr ``last_seen`` älter als *live_window_ms*
        ist (der Abdruck war tot)."""

    async def cron_reports(self) -> CronReports:
        """Alle Meldungen samt Backend-Zeit des Lesens."""

    async def known_cron_names(self) -> frozenset[str]:
        """Namen, die je ein Worker gemeldet hat (Plan 0.12.0, MR 11, BZ-11).
        Anders als die Meldungen werden sie nicht nach Alter aufgeräumt,
        sondern nur zusammen mit der Definition entfernt (``prune_cron``,
        ``remove``); so erkennt der Leader einen verwaisten Cron auch dann,
        wenn ein Leader vor 0.12 die Meldungen gelöscht hat."""

    async def forget_known_cron(self, name: str) -> None:
        """Nimmt *name* aus :meth:`known_cron_names` (``schedule remove
        --permanent``); ``prune_cron`` tut es von selbst."""

    async def prune_cron_reports(self, *, max_age_ms: int) -> int:
        """Löscht atomar die Meldungen, deren ``last_seen`` (Backend-Zeit)
        älter als *max_age_ms* ist; liefert ihre Anzahl."""

    async def prune_cron(
        self, name: str, expected: bytes, *, stale_after_ms: int | None, remember_disabled: bool,
    ) -> bool:
        """Entfernt einen statischen Cron atomar samt Fire-Index-Eintrag
        (KTD8), wenn die Definition noch *expected* ist und: mit
        *stale_after_ms* jede Meldung des Namens älter als die Frist ist
        (Backend-Zeit, mindestens eine Meldung); ohne Frist keine Meldung des
        Namens existiert. Mit *remember_disabled* merkt sich der Store den
        Namen (:meth:`pruned_disabled`). ``True``, wenn entfernt."""

    async def pruned_disabled(self, name: str) -> bool:
        """``True``, wenn *name* beim Entfernen deaktiviert war (KTD8): das
        Wiederanlegen übernimmt den Zustand."""

    async def forget_pruned_disabled(self, name: str) -> None:
        """Vergisst die Markierung aus :meth:`pruned_disabled`."""

    async def ensure_epoch(self, candidate: str) -> str | None:
        """Epoch des Datenbestands (Plan 0.12.0, MR 14, BZ-04), atomar: fehlt
        er, wird *candidate* gesetzt und ``None`` geliefert; sonst der
        gespeicherte Wert. Der Epoch hat keine TTL; fehlt er bei einem
        Prozess, der ihn schon kannte, hat Valkey seine Daten verloren. Stores
        Dritter duerfen die Methode weglassen (Q15: der Scheduler prueft dann
        nicht)."""

    async def report_worker_version(self, worker_id: str, version: str, *, ttl_ms: int) -> None:
        """Meldet die swarmq-Version eines Workers; die Meldung gilt *ttl_ms*
        (KTD11, Anzeige in ``schedule status``)."""

    async def worker_versions(self) -> Mapping[str, str]:
        """Worker-ID → swarmq-Version aller noch gültigen Meldungen."""

    async def set_leader_version(self, owner: str, version: str, *, ttl_ms: int) -> None:
        """Schreibt die swarmq-Version des Leaders *owner* neben das
        Leader-Lock, mit der TTL des Locks (KTD11)."""

    async def leader_info(self) -> LeaderInfo:
        """Owner des Leader-Locks und seine Version, gelesen in einem
        Schritt; eine Version, die ein anderer Owner geschrieben hat, gilt
        nicht (KTD11)."""


GuardStatus = Literal["claimed", "done", "superseded", "running", "orphan_limit"]


@dataclass(frozen=True, slots=True)
class GuardClaim:
    """Ergebnis eines Claims des Ausführungsschutzes (KTD5).

    ``claimed`` -- der Running-Marker gehört jetzt dem Aufrufer;
    ``done``/``superseded`` -- abgeschlossen bzw. durch einen Retry überholt;
    ``running`` -- ein fremder Running-Marker steht; ``orphan_limit`` -- die
    Waisen-Grenze ist erreicht (der Marker gehört trotzdem dem Aufrufer, damit
    das folgende Dead-Letter ihn abschließt). ``orphans`` zählt die
    abgelaufenen Running-Marker ohne Freigabe."""

    status: GuardStatus
    orphans: int = 0


@runtime_checkable
class ExecutionGuardStore(Protocol):
    """Running-, Done- und Superseded-Marker je Ausführungs-Identität (KTD5)."""

    async def claim(
        self, identity: str, *, owner: str, retry_count: int, ttl_ms: int, state_ttl_ms: int, max_orphans: int,
        backlog_ttl_ms: int | None = None,
    ) -> GuardClaim:
        """Atomar: Done- oder Superseded-Marker (Zähler größer als
        *retry_count*) sprechen dagegen, sonst ein fremder Running-Marker;
        andernfalls gehört der Running-Marker (TTL *ttl_ms*) *owner*. Ein
        abgelaufener Marker ohne Freigabe zählt als Waise. Meldet der Claim
        ``running`` (die Zustellung wird zurückgestellt), lebt der Zustand
        mindestens *backlog_ttl_ms* (BZ-12); ``mark_done``/``mark_superseded``
        verkürzen ihn nie."""

    async def renew(self, identity: str, *, owner: str, ttl_ms: int) -> bool:
        """Compare-and-PEXPIRE: ``False``, wenn der Marker weg oder fremd ist."""

    async def mark_done(self, identity: str, *, owner: str, state_ttl_ms: int) -> None:
        """Done-Marker setzen; den Running-Marker nur löschen, wenn er *owner* gehört."""

    async def mark_superseded(self, identity: str, *, owner: str, retry_count: int, state_ttl_ms: int) -> None:
        """Superseded-Marker mit dem Zähler der Retry-Kopie (nur steigend);
        den eigenen Running-Marker löschen."""

    async def release(self, identity: str, *, owner: str) -> None:
        """Den eigenen Running-Marker löschen (nicht-terminaler Ausgang)."""
