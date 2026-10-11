"""Backend-Kernvertrag ``ResultBackend`` (Entwurf §2.3, KTD4).

Ergebnisse, Benachrichtigung/Warten, Fortschritt, Cancel-Flags,
Workflow-Zustand und Dedup-Flags. Die Koordinationsverträge (Locks,
Rate-Limit, Chords, Schedules) stehen in ``swarmq.contracts.coordination``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Protocol, runtime_checkable


@runtime_checkable
class ResultBackend(Protocol):
    async def connect(self) -> None:
        """Idempotent; Treiberfehler → ``BackendConnectionError``."""

    async def close(self) -> None: ...

    async def ping(self) -> None:
        """Wirft ``BackendConnectionError`` vor ``connect`` oder bei Störung."""

    # --- Ergebnisse -----------------------------------------------------
    async def store_result(
        self, task_id: str, data: bytes, ttl_ms: int, *, op_timeout: float | None = None
    ) -> None: ...

    async def get_result(self, task_id: str) -> bytes | None: ...

    async def get_results(self, task_ids: Sequence[str]) -> list[bytes | None]:
        """Ein Round-Trip; Reihenfolge wie ``task_ids``."""

    async def delete_result(self, task_id: str) -> None: ...

    async def notify_result(self, task_id: str) -> None:
        """Weckt Wartende. Fehler werden geloggt, nie geworfen."""

    async def wait_for_result(self, task_id: str, *, timeout: float) -> bytes | None:
        """Liefert das Ergebnis, sobald vorhanden: wacht bei ``notify_result``
        auf, fällt sonst auf Polling zurück; ``None`` bei Ablauf."""

    # --- Fortschritt ----------------------------------------------------
    async def set_progress(
        self, task_id: str, fields: Mapping[str, str], ttl_ms: int, *, op_timeout: float | None = None
    ) -> None: ...

    async def get_progress(self, task_id: str, *, op_timeout: float | None = None) -> Mapping[bytes, bytes] | None: ...

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        """Fehler werden geloggt, nie geworfen."""

    def watch_progress(self, task_id: str) -> AsyncIterator[bytes]:
        """Fortschritts-Payloads bis zum Abbruch durch den Aufrufer; überlebt
        Verbindungsabbrüche durch erneutes Abonnieren."""

    # --- Cancel ---------------------------------------------------------
    async def set_cancel_flag(self, task_id: str, ttl_ms: int, *, op_timeout: float | None = None) -> bool: ...

    async def is_cancelled(self, task_id: str, *, op_timeout: float | None = None) -> bool: ...

    async def clear_cancel_flag(self, task_id: str, *, op_timeout: float | None = None) -> None:
        """Verkürzt das Flag auf höchstens 1 h (``PEXPIRE … LT``, BZ-23),
        verlängert ein kürzeres nie und löscht nicht. Loggt statt zu werfen;
        die TTL heilt Waisen."""

    # --- Workflow -------------------------------------------------------
    async def store_workflow_state(
        self, wf_id: str, data: bytes, ttl_ms: int, *, op_timeout: float | None = None
    ) -> None: ...

    async def compare_and_store_workflow_state(
        self, wf_id: str, data: bytes, ttl_ms: int, *, expected_version: int, op_timeout: float | None = None
    ) -> bool:
        """Compare-and-Set (KTD11): schreibt *data* nur, wenn der Bestand
        existiert und seine ``version`` (fehlend = 0) *expected_version*
        entspricht. ``True`` bei Schreibung, ``False`` bei Konflikt oder
        fehlendem Schlüssel. Die Erstanlage bleibt ``store_workflow_state``."""
        ...

    async def get_workflow_state(self, wf_id: str, *, op_timeout: float | None = None) -> bytes | None: ...

    async def set_workflow_cancel_flag(self, wf_id: str, ttl_ms: int, *, op_timeout: float | None = None) -> bool: ...

    async def is_workflow_cancelled(self, wf_id: str, *, op_timeout: float | None = None) -> bool: ...

    async def notify_workflow(self, wf_id: str) -> None: ...

    async def wait_for_workflow(self, wf_id: str, *, timeout: float) -> bool:
        """``True`` bei Benachrichtigung vor Ablauf, sonst ``False``."""

    # --- Dedup ----------------------------------------------------------
    async def claim_unique(
        self, dedup_hash: str, owner: str, ttl_ms: int, *, op_timeout: float | None = None
    ) -> str | None:
        """``None`` = beansprucht; sonst der bestehende Owner. Owner bleibt
        String (Wire-Format, Entwurf §4 Ausnahme)."""

    async def release_unique(self, dedup_hash: str, owner: str, *, op_timeout: float | None = None) -> bool:
        """Owner-Check; ``False`` bei fremdem Owner, fehlendem Flag oder Fehler."""


@runtime_checkable
class BatchResultBackend(Protocol):
    """R4.2-Andockstelle (Entwurf §3): gepipelinte Result-Writes."""

    async def store_results(
        self, items: Sequence[tuple[str, bytes, int]], *, op_timeout: float | None = None
    ) -> None: ...
