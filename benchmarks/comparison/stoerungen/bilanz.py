"""Wertung des Hauptbuchs im Garantie-Vergleich (Plan 2026-10-24-1118, KTD14).

Rein funktional: Die Funktion bekommt die Zeilen des Hauptbuchs und liefert
die Bilanz einer Kombination aus System, Profil und Störung. Gezählt wird
nach der Ruhephase.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

BESTAETIGT = "bestaetigt"
#: Der Erzeuger weiß nicht, ob der Broker die Nachricht hat (bei swarmq
#: ``PublishIndeterminate``, E-042); kein Verlust, eigene Spalte.
UNBESTIMMT = "unbestimmt"
#: Der Erzeuger bekam einen Fehler, der Publish gilt als nicht erfolgt; kein Verlust.
ABGELEHNT = "abgelehnt"


class LeeresHauptbuch(ValueError):
    """Das Hauptbuch hat keinen Publish: Der Lauf hat nichts gemessen, „0 Verluste“ wäre falsch."""


@dataclass(frozen=True)
class Publish:
    """Ein Publish des Erzeugers mit seinem Ausgang."""

    kennung: str
    ausgang: str


@dataclass(frozen=True)
class Ausfuehrung:
    """Eine abgeschlossene Ausführung eines Tasks."""

    kennung: str
    zeit: float = 0.0


@dataclass(frozen=True)
class Bilanz:
    """Zählung einer Kombination nach KTD14; ``ausgefuehrt`` zählt Tasks, nicht Ausführungen."""

    bestaetigt: int
    unbestimmt: int
    abgelehnt: int
    ausgefuehrt: int
    verluste: int
    duplikate: int
    #: Duplikate, deren spätere Ausführung nach einem Valkey-Neustart ohne
    #: Persistenz lag (E-077: die Marker des Ausführungsschutzes sind weg).
    duplikate_e077: int

    @property
    def leer(self) -> bool:
        """Ohne bestätigten Publish oder ohne Ausführung hat der Lauf nichts gemessen."""
        return self.bestaetigt == 0 or self.ausgefuehrt == 0


def bilanz(
    publishes: Iterable[Publish],
    ausfuehrungen: Iterable[Ausfuehrung],
    *,
    valkey_datenverlust: float | None = None,
) -> Bilanz:
    """Bilanz nach KTD14; ``valkey_datenverlust`` ist der Zeitpunkt eines Valkey-Neustarts ohne Persistenz."""
    publishes = list(publishes)
    if not publishes:
        raise LeeresHauptbuch("Hauptbuch ohne Publish")
    bestaetigt = {p.kennung for p in publishes if p.ausgang == BESTAETIGT}
    zeiten: dict[str, list[float]] = defaultdict(list)
    for ausfuehrung in ausfuehrungen:
        zeiten[ausfuehrung.kennung].append(ausfuehrung.zeit)
    doppelt = [sorted(z) for z in zeiten.values() if len(z) > 1]
    nach_verlust = [z for z in doppelt if valkey_datenverlust is not None and z[-1] >= valkey_datenverlust]
    return Bilanz(
        bestaetigt=len(bestaetigt),
        unbestimmt=sum(1 for p in publishes if p.ausgang == UNBESTIMMT),
        abgelehnt=sum(1 for p in publishes if p.ausgang == ABGELEHNT),
        ausgefuehrt=len(zeiten),
        verluste=len(bestaetigt - zeiten.keys()),
        duplikate=len(doppelt) - len(nach_verlust),
        duplikate_e077=len(nach_verlust),
    )
