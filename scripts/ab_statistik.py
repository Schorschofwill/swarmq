"""Statistik des A/B-Messwerkzeugs (Plan 2026-10-24-1118, KTD12, R5).

Nur reine Funktionen, reines Python (keine Abhängigkeit). ``scripts/ab_messung.py``
ruft ``auswerten`` mit den Rohwerten beider Seiten auf.

Mann-Whitney U, zweiseitig, immer über die Normalapproximation mit
Bindungs- und Stetigkeitskorrektur (wie ``scipy.stats.mannwhitneyu`` mit
``method="asymptotic"``). Einen exakten Test gibt es nicht: Das Werkzeug
misst mindestens 10 Paare, und Messwerte enthalten oft Bindungen, für die
scipy ebenfalls approximiert. ``u`` ist die Statistik der A-Seite.

Die Hodges-Lehmann-Verschiebung ist der Median aller Differenzen B minus A.
``relativ`` ist sie geteilt durch den Median der A-Seite, mit dem Vorzeichen
so gedreht, dass positiv immer besser heißt.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from typing import NamedTuple

# KTD12: gesichert bei p < 0,05 (zweiseitig); Gewinn ab 5 % vom Median der A-Seite.
ALPHA = 0.05
SCHWELLE = 0.05


class MannWhitney(NamedTuple):
    u: float
    p: float


def _raenge(werte: list[float]) -> list[float]:
    """Ränge 1..n; gleiche Werte bekommen den mittleren Rang ihrer Gruppe."""
    ordnung = sorted(range(len(werte)), key=werte.__getitem__)
    raenge = [0.0] * len(werte)
    start = 0
    while start < len(ordnung):
        ende = start
        while ende + 1 < len(ordnung) and werte[ordnung[ende + 1]] == werte[ordnung[start]]:
            ende += 1
        for index in ordnung[start : ende + 1]:
            raenge[index] = (start + ende) / 2 + 1
        start = ende + 1
    return raenge


def mann_whitney_u(a: list[float], b: list[float]) -> MannWhitney:
    n1, n2 = len(a), len(b)
    raenge = _raenge([*a, *b])
    u = sum(raenge[:n1]) - n1 * (n1 + 1) / 2
    mittel = n1 * n2 / 2
    n = n1 + n2
    bindungen = sum(t**3 - t for t in Counter([*a, *b]).values())
    sigma = math.sqrt(n1 * n2 / 12 * ((n + 1) - bindungen / (n * (n - 1))))
    if sigma == 0:
        # Alle Werte gleich: kein Unterschied (scipy liefert hier ebenfalls p = 1).
        return MannWhitney(u, 1.0)
    z = (max(u, n1 * n2 - u) - mittel - 0.5) / sigma
    return MannWhitney(u, min(1.0, math.erfc(z / math.sqrt(2))))


def hodges_lehmann(a: list[float], b: list[float]) -> float:
    """Median aller Differenzen B minus A: die geschätzte Verschiebung von A nach B."""
    return statistics.median(y - x for x in a for y in b)


class Richtung(StrEnum):
    """Welche Richtung der Kennzahl besser ist: Durchsatz größer, Latenz, CPU und RAM kleiner."""

    GROESSER = "groesser"
    KLEINER = "kleiner"


class Urteil(StrEnum):
    GEWINN = "Gewinn"
    KEIN_GEWINN = "kein Gewinn"
    VERSCHLECHTERUNG = "Verschlechterung"
    NICHT_BELEGT = "nicht belegt"


@dataclass(frozen=True)
class Auswertung:
    u: float
    p: float
    verschiebung: float
    relativ: float
    urteil: Urteil


def auswerten(a: list[float], b: list[float], *, besser: Richtung, profile: bool) -> Auswertung:
    test = mann_whitney_u(a, b)
    verschiebung = hodges_lehmann(a, b)
    relativ = verschiebung / statistics.median(a)
    if besser is Richtung.KLEINER:
        relativ = -relativ
    if test.p < ALPHA and relativ < 0:
        urteil = Urteil.VERSCHLECHTERUNG
    elif test.p >= ALPHA or relativ < SCHWELLE:
        urteil = Urteil.KEIN_GEWINN
    else:
        # R5 verlangt den Nachweis, wo eingespart wird: ohne Profil kein Gewinn.
        urteil = Urteil.GEWINN if profile else Urteil.NICHT_BELEGT
    return Auswertung(test.u, test.p, verschiebung, relativ, urteil)
