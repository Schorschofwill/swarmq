"""Sättigung der Methoden und Säulen (Plan 2026-10-24-1118, U5, KTD8, R12–R14).

Aufruf: ``uv run just saettigung``. Liest ``docs/befunde/methodenkatalog.md``
und ``docs/befunde/saettigungsbuch.toml`` und gibt je Säule den Stand aus,
dazu jede Kombination aus Methode und Modul, die nicht mehr offen ist, und
dringende offene Signale. Der Stand wird berechnet, nicht gespeichert:

- Gezählt werden nur Läufe mit Prüfumfang größer 0 (AE6).
- Zwei Läufe in Folge ohne bestätigten Befund sättigen eine Kombination.
- Ändern sich seit dem Commit des letzten Laufs mindestens 20 % der heutigen
  Zeilen eines Moduls, ist die Kombination wieder offen (AE5).

git wird über einen injizierbaren Aufruf gefragt, damit die Tests ohne
Repository-Zustand laufen.
"""

from __future__ import annotations

import datetime
import enum
import re
import subprocess
import sys
import tomllib
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path

type Git = Callable[[list[str]], str]

_WURZEL = Path(__file__).resolve().parents[1]

# Säule aus dem Kürzel der Methoden-ID (M-F-nn, M-Z-nn, M-A-nn, M-P-nn).
SAEULEN = {"F": "Fehler", "Z": "Zuverlässigkeit", "A": "Angriffssicherheit", "P": "Performance"}


# KTD8: ENTSCHEIDUNG, NICHT HALTBAR und unbeantwortete „Entscheidung überdenken?“
# zählen nicht. LÜCKE zählt (Georg, Plan 2026-10-24-1118).
BESTAETIGT = frozenset({"FEHLER", "LÜCKE", "VERBESSERUNG"})

# Anteil geänderter Zeilen eines Moduls seit dem Lauf-Commit, ab dem die
# Sättigung seiner Kombinationen zurückgesetzt wird (KTD8, AE5).
WESENTLICH = 0.20

# Dateien, deren Zeilen ein Paket unter ``src/swarmq`` ausmachen.
_QUELLEN = (".py", ".lua")


class Stand(enum.Enum):
    OFFEN = "offen"
    EINMAL_LEER = "einmal leer"
    GESAETTIGT = "gesättigt"


@dataclass(frozen=True)
class Befund:
    id: str
    einstufung: str


@dataclass(frozen=True)
class Lauf:
    id: str
    datum: datetime.date
    commit: str
    methode: str
    modul: str
    umfang: int
    einheit: str
    befunde: tuple[Befund, ...]

    @property
    def bestaetigt(self) -> bool:
        return any(befund.einstufung in BESTAETIGT for befund in self.befunde)


@dataclass(frozen=True)
class Signal:
    id: str
    quelle: str
    saeule: str
    modul: str
    dringlichkeit: str
    status: str


@dataclass(frozen=True)
class Buch:
    laeufe: tuple[Lauf, ...]
    signale: tuple[Signal, ...]


def lade_buch(pfad: Path) -> Buch:
    daten = tomllib.loads(pfad.read_text(encoding="utf-8"))
    laeufe = tuple(
        Lauf(
            id=lauf["id"],
            datum=lauf["datum"],
            commit=lauf["commit"],
            methode=lauf["methode"],
            modul=lauf["modul"],
            umfang=lauf["umfang"],
            einheit=lauf["einheit"],
            befunde=tuple(Befund(id=b["id"], einstufung=b["einstufung"]) for b in lauf["befunde"]),
        )
        for lauf in daten.get("lauf", [])
    )
    signale = tuple(
        Signal(**{feld: signal[feld] for feld in ("id", "quelle", "saeule", "modul", "dringlichkeit", "status")})
        for signal in daten.get("signal", [])
    )
    return Buch(laeufe=laeufe, signale=signale)


@dataclass(frozen=True)
class Katalog:
    methoden: tuple[str, ...]
    module: tuple[str, ...]
    entfallen: tuple[str, ...]


_METHODE = re.compile(r"^\|\s*(M-[FZAP]-\d{2})\s*\|", re.MULTILINE)
_MODUL = re.compile(r"^- `([a-z_]+)`", re.MULTILINE)


def _abschnitt(text: str, titel: str) -> str:
    """Text unter der Überschrift ``## titel`` bis zur nächsten ``##``-Überschrift."""
    _, gefunden, rest = text.partition(f"\n## {titel}\n")
    return rest.split("\n## ", 1)[0] if gefunden else ""


def lade_katalog(pfad: Path) -> Katalog:
    text = pfad.read_text(encoding="utf-8")
    return Katalog(
        methoden=tuple(_METHODE.findall(text)),
        module=tuple(_MODUL.findall(_abschnitt(text, "Module"))),
        entfallen=tuple(_MODUL.findall(_abschnitt(text, "Entfallene Module"))),
    )


def stand_der_kombination(laeufe: Sequence[Lauf], anteil: Callable[[str], float]) -> Stand:
    gezaehlt = sorted((lauf for lauf in laeufe if lauf.umfang > 0), key=lambda lauf: lauf.datum)
    leer_in_folge = 0
    for lauf in gezaehlt:
        leer_in_folge = 0 if lauf.bestaetigt else leer_in_folge + 1
    if leer_in_folge and anteil(gezaehlt[-1].commit) >= WESENTLICH:
        return Stand.OFFEN
    if leer_in_folge >= 2:
        return Stand.GESAETTIGT
    if leer_in_folge == 1:
        return Stand.EINMAL_LEER
    return Stand.OFFEN


def staende(
    katalog: Katalog, laeufe: Sequence[Lauf], anteil: Callable[[str, str], float]
) -> dict[tuple[str, str], Stand]:
    """Stand jeder Kombination aus Methode und Modul des Katalogs.

    Läufe zu Modulen, die der Katalog nicht mehr führt (umbenannt oder
    entfallen), zählen für keine Kombination.
    """
    return {
        (methode, modul): stand_der_kombination(
            [lauf for lauf in laeufe if (lauf.methode, lauf.modul) == (methode, modul)],
            partial(anteil, modul),
        )
        for methode in katalog.methoden
        for modul in katalog.module
    }


@dataclass(frozen=True)
class Saeule:
    zaehlung: dict[Stand, int]

    @property
    def gesaettigt(self) -> bool:
        """Gesättigt, wenn es Kombinationen gibt und alle gesättigt sind (R14)."""
        return bool(self.zaehlung) and set(self.zaehlung) == {Stand.GESAETTIGT}


def saeule_der_methode(methode: str) -> str:
    return SAEULEN[methode.split("-")[1]]


def saeulen(staende: dict[tuple[str, str], Stand]) -> dict[str, Saeule]:
    zaehlung: dict[str, Counter[Stand]] = {name: Counter() for name in SAEULEN.values()}
    for (methode, _), stand in staende.items():
        zaehlung[saeule_der_methode(methode)][stand] += 1
    return {name: Saeule(dict(zahlen)) for name, zahlen in zaehlung.items()}


def _modulpfad(modul: str, wurzel: Path) -> str:
    """Pfad eines Pakets (Verzeichnis) oder Moduls (``.py``) unter ``src/swarmq``."""
    paket = f"src/swarmq/{modul}"
    return paket if (wurzel / paket).is_dir() else f"{paket}.py"


def _zeilen(pfad: Path) -> int:
    dateien = [pfad] if pfad.is_file() else [d for d in pfad.rglob("*") if d.suffix in _QUELLEN]
    return sum(len(datei.read_text(encoding="utf-8").splitlines()) for datei in dateien)


def aenderungsanteil(modul: str, commit: str, *, wurzel: Path, git: Git) -> float:
    """Anteil der seit ``commit`` geänderten Zeilen an den heutigen Zeilen des Moduls.

    Eine geänderte Zeile zählt in ``--numstat`` als hinzugefügt und gelöscht;
    je Datei zählt deshalb das Größere von beiden.
    """
    pfad = _modulpfad(modul, wurzel)
    ausgabe = git(["diff", "--numstat", f"{commit}..HEAD", "--", pfad])
    geaendert = 0
    for zeile in ausgabe.splitlines():
        hinzu, weg, _ = zeile.split("\t", 2)
        geaendert += max(int(hinzu), int(weg))
    return geaendert / _zeilen(wurzel / pfad)


def _git_in(wurzel: Path) -> Git:
    def git(args: list[str]) -> str:
        return subprocess.run(["git", *args], cwd=wurzel, capture_output=True, text=True, check=True).stdout

    return git


def bericht(staende: dict[tuple[str, str], Stand], signale: Sequence[Signal]) -> list[str]:
    zeilen: list[str] = []
    je_saeule = saeulen(staende)
    for name, saeule in je_saeule.items():
        gesamt = sum(saeule.zaehlung.values())
        zahlen = ", ".join(f"{saeule.zaehlung[stand]} {stand.value}" for stand in Stand if stand in saeule.zaehlung)
        urteil = "gesättigt" if saeule.gesaettigt else "nicht gesättigt"
        offen = sum(1 for signal in signale if signal.saeule == name and signal.status == "offen")
        zeilen.append(f"{name}: {urteil} ({zahlen} von {gesamt}), offene Signale: {offen}")
        zeilen.extend(
            f"  {stand.value}: {methode} auf {modul}"
            for (methode, modul), stand in staende.items()
            if stand is not Stand.OFFEN and saeule_der_methode(methode) == name
        )
        zeilen.extend(
            f"  dringend: {signal.id} ({signal.quelle}, {signal.modul})"
            for signal in signale
            if signal.saeule == name and signal.status == "offen" and signal.dringlichkeit == "dringend"
        )
    if all(saeule.gesaettigt for saeule in je_saeule.values()):
        zeilen.append("Alle Säulen gesättigt: Georg entscheidet über neue Methoden (KTD8).")
    return zeilen


def main(*, wurzel: Path = _WURZEL, git: Git | None = None) -> int:
    befunde = wurzel / "docs" / "befunde"
    katalog = lade_katalog(befunde / "methodenkatalog.md")
    buch = lade_buch(befunde / "saettigungsbuch.toml")
    git = git or _git_in(wurzel)
    stand = staende(katalog, buch.laeufe, lambda modul, commit: aenderungsanteil(modul, commit, wurzel=wurzel, git=git))
    print("\n".join(bericht(stand, buch.signale)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
