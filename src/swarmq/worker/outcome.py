"""``DeliveryOutcome`` (Plan U6, KTD5): das typisierte Ergebnis jeder
Pipeline-Stufe, das die Terminal-Flags ``_unique_release_pending`` /
``_schedfire_terminal`` des Runners ersetzt (U19).

Drei Ausgänge:

- ``Continue`` — Stufe passiert, nächste Stufe.
- ``Redelivered`` — Zustellung wurde erneut eingereiht (``redeliver`` /
  ``retry_later``): Dedup-Flag bleibt, In-Flight-Claim wird gelöscht.
- ``Terminal(release_unique, finalize_schedfire)`` — Zustellung ist erledigt;
  die Felder sagen, ob der ``finally``-Block das Dedup-Flag freigibt und den
  Schedfire-Completed-Marker schreibt. Die drei heutigen Varianten:
  ``release_in_finally()`` (Erfolg, Fehler, Cancel, Unknown-Task),
  ``already_released()`` (Lock-Pfade und Dead-Letter, die nach dem Ack inline
  freigeben), ``release_only()`` (Expiry nach dem Dead-Letter, BZ-22) und
  ``never_release()`` (Expiry, dessen Dead-Letter scheiterte).

Ein Gate *muss* einen Ausgang zurückgeben — „Reset vergessen" ist kein
möglicher Zustand mehr (Entwurf §4).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import override


class DeliveryOutcome:
    """Basisklasse der drei Ausgänge (für ``isinstance``-Prüfungen)."""

    __slots__ = ()


@dataclass(frozen=True, slots=True)
class _Continue(DeliveryOutcome):
    @override
    def __repr__(self) -> str:
        return "Continue"


@dataclass(frozen=True, slots=True)
class _Redelivered(DeliveryOutcome):
    @override
    def __repr__(self) -> str:
        return "Redelivered"


@dataclass(frozen=True, slots=True)
class Terminal(DeliveryOutcome):
    release_unique: bool
    finalize_schedfire: bool

    @classmethod
    def release_in_finally(cls) -> Terminal:
        """Erfolg, Fehler-Ergebnis, Cancel, Unknown-Task: Flag im ``finally`` freigeben."""
        return cls(release_unique=True, finalize_schedfire=True)

    @classmethod
    def already_released(cls) -> Terminal:
        """Lock-Pfade und Dead-Letter: bereits nach dem Ack inline freigegeben."""
        return cls(release_unique=False, finalize_schedfire=True)

    @classmethod
    def release_only(cls) -> Terminal:
        """Expiry nach erfolgreichem Dead-Letter (BZ-22): das Dedup-Flag im
        ``finally`` freigeben (owner-geprueft mit der ``task_id``), ohne
        Schedfire-Completed-Marker."""
        return cls(release_unique=True, finalize_schedfire=False)

    @classmethod
    def never_release(cls) -> Terminal:
        """Expiry, dessen Dead-Letter scheiterte: das Dedup-Flag bleibt stehen,
        die Neuzustellung gibt es frei."""
        return cls(release_unique=False, finalize_schedfire=False)


Continue: DeliveryOutcome = _Continue()
Redelivered: DeliveryOutcome = _Redelivered()

__all__ = ["Continue", "DeliveryOutcome", "Redelivered", "Terminal"]
