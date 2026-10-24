"""Prüffunktionen für das Sättigungsbuch (Guard in test_saettigungsbuch.py).

Lädt ``scripts/saettigung.py`` einmal je Prozess, damit Skript-Tests und Guard
dieselben Klassen sehen.
"""

from __future__ import annotations

import datetime as dt
import re
import sys
import tomllib
from pathlib import Path
from types import ModuleType
from typing import Any

from tests.support.skripte import lade_skript

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "saettigung.py"


def _load() -> ModuleType:
    if "saettigung" in sys.modules:
        return sys.modules["saettigung"]
    return lade_skript("saettigung", _SCRIPT)


saettigung = _load()


# Befund-IDs stehen in der ersten Spalte der Registertabellen, teils mit
# Herkunft in Klammern: ``| PF-5 (R4M-E-01b) | …``.
_ROW = re.compile(r"^\|([^|]*)\|", re.MULTILINE)
_BEFUND_ID = re.compile(r"\b[A-Z][A-Z0-9]*(?:-[A-Za-z0-9]+)+\b")
_LAUF_ID = re.compile(r"L-\d{3}")
_SIGNAL_ID = re.compile(r"S-\d{3}")
_LAUF_FELDER = ("id", "datum", "commit", "methode", "modul", "umfang", "einheit", "befunde")
_SIGNAL_FELDER = ("id", "quelle", "saeule", "modul", "fundstelle", "beschreibung", "dringlichkeit", "status")
_SIGNAL_WERTE: dict[str, tuple[str, ...]] = {
    "saeule": tuple(saettigung.SAEULEN.values()),
    "dringlichkeit": ("normal", "dringend"),
    "status": ("offen", "untersucht", "verworfen"),
}
_EINSTUFUNGEN = ("FEHLER", "LÜCKE", "VERBESSERUNG", "ENTSCHEIDUNG", "NICHT HALTBAR")
_KEINE_REGISTER = ("README.md", "methodenkatalog.md")


def register_ids(befunde: Path) -> set[str]:
    """Alle Befund-IDs aus den Registern unter ``docs/befunde/`` (ohne Unterordner)."""
    ids: set[str] = set()
    for path in sorted(befunde.glob("*.md")):
        if path.name not in _KEINE_REGISTER:
            for cell in _ROW.findall(path.read_text(encoding="utf-8")):
                ids.update(_BEFUND_ID.findall(cell))
    return ids


def _check_modul(name: str, eintrag: dict[str, Any], katalog: Any) -> list[str]:
    """Das Modul steht im Katalog, bei alten Einträgen auch unter „Entfallene Module“."""
    modul = eintrag.get("modul")
    if modul in katalog.module or modul in katalog.entfallen:
        return []
    return [f"{name}: Modul {modul!r} steht nicht im Katalog (Module oder Entfallene Module)"]


def _check_status(name: str, signal: dict[str, Any], lauf_ids: set[str]) -> list[str]:
    """„untersucht“ verweist auf einen Lauf im Buch, „verworfen“ nennt einen Grund (KTD10)."""
    status = signal.get("status")
    if status == "untersucht" and signal.get("lauf") not in lauf_ids:
        return [f"{name}: status untersucht verlangt lauf mit einer Lauf-ID aus dem Buch, nicht {signal.get('lauf')!r}"]
    if status == "verworfen" and not signal.get("grund"):
        return [f"{name}: status verworfen verlangt einen grund"]
    return []


def _check_signale(signale: list[dict[str, Any]], katalog: Any, lauf_ids: set[str]) -> list[str]:
    problems: list[str] = []
    for signal in signale:
        name = signal.get("id", "?")
        problems.extend(f"{name}: Pflichtfeld {key} fehlt" for key in _SIGNAL_FELDER if key not in signal)
        problems.extend(_check_modul(name, signal, katalog))
        problems.extend(
            f"{name}: {key} {signal[key]!r} unbekannt (erlaubt: {', '.join(erlaubt)})"
            for key, erlaubt in _SIGNAL_WERTE.items()
            if key in signal and signal[key] not in erlaubt
        )
        problems.extend(_check_status(name, signal, lauf_ids))
    return problems


def _check_laeufe(laeufe: list[dict[str, Any]], katalog: Any, befund_ids: set[str]) -> list[str]:
    problems: list[str] = []
    for lauf in laeufe:
        name = lauf.get("id", "?")
        problems.extend(f"{name}: Pflichtfeld {key} fehlt" for key in _LAUF_FELDER if key not in lauf)
        if "datum" in lauf and not isinstance(lauf["datum"], dt.date):
            problems.append(f"{name}: datum {lauf['datum']!r} ist kein Datum (JJJJ-MM-TT ohne Anführungszeichen)")
        umfang = lauf.get("umfang", 0)
        if not isinstance(umfang, int) or isinstance(umfang, bool) or umfang < 0:
            problems.append(f"{name}: umfang {umfang!r} ist keine Zahl ab 0")
        problems.extend(_check_modul(name, lauf, katalog))
        if lauf.get("methode") not in katalog.methoden:
            problems.append(f"{name}: Methode {lauf.get('methode')} steht nicht im Katalog")
        for befund in lauf.get("befunde", []):
            if befund.get("id") not in befund_ids:
                problems.append(f"{name}: Befund {befund.get('id')} steht in keinem Register unter docs/befunde/")
            if befund.get("einstufung") not in _EINSTUFUNGEN:
                problems.append(
                    f"{name}: Befund {befund.get('id')} hat die unbekannte Einstufung {befund.get('einstufung')!r}"
                    f" (erlaubt: {', '.join(_EINSTUFUNGEN)})"
                )
    return problems


def _check_ids(eintraege: list[dict[str, Any]], muster: re.Pattern[str], art: str) -> list[str]:
    problems: list[str] = []
    seen: set[str] = set()
    for eintrag in eintraege:
        eintrag_id = str(eintrag.get("id", ""))
        if not muster.fullmatch(eintrag_id):
            problems.append(f"{art} {eintrag_id!r}: ID hat nicht das Format {muster.pattern}")
        if eintrag_id in seen:
            problems.append(f"{art} {eintrag_id}: ID doppelt")
        seen.add(eintrag_id)
    return problems


def module_unter(src: Path) -> set[str]:
    """Pakete und Module direkt unter ``src/swarmq`` (ohne ``__init__``, ``__pycache__``, ``py.typed``)."""
    return {
        path.stem
        for path in src.iterdir()
        if (path.is_dir() and path.name != "__pycache__") or (path.suffix == ".py" and path.stem != "__init__")
    }


def _check_module(katalog: Any, src: Path) -> list[str]:
    vorhanden = module_unter(src)
    return [
        f"Modul {modul} fehlt im Katalog (Abschnitt Module in docs/befunde/methodenkatalog.md)"
        for modul in sorted(vorhanden - set(katalog.module))
    ] + [f"Katalogmodul {modul} gibt es nicht unter src/swarmq" for modul in sorted(set(katalog.module) - vorhanden)]


def check_buch(befunde: Path, src: Path) -> list[str]:
    """Liefert die Konsistenzprobleme von Buch, Katalog, Registern und Modulen als Klartext."""
    katalog = saettigung.lade_katalog(befunde / "methodenkatalog.md")
    buch = tomllib.loads((befunde / "saettigungsbuch.toml").read_text(encoding="utf-8"))
    laeufe: list[dict[str, Any]] = buch.get("lauf", [])
    signale: list[dict[str, Any]] = buch.get("signal", [])
    return (
        _check_ids(laeufe, _LAUF_ID, "Lauf")
        + _check_ids(signale, _SIGNAL_ID, "Signal")
        + _check_laeufe(laeufe, katalog, register_ids(befunde))
        + _check_signale(signale, katalog, {str(lauf.get("id")) for lauf in laeufe})
        + _check_module(katalog, src)
    )
