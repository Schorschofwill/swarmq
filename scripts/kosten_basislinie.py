"""Basislinie der Kostensuite (Plan 2026-10-24-1118, KTD7).

Die Suite unter ``tests/kosten/`` misst zählbare Kosten je Szenario und
vergleicht sie hier exakt mit ``tests/kosten/basislinie.toml``.
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

BASISLINIE = Path("tests/kosten/basislinie.toml")
# Auch eine Senkung ist rot, damit die Basislinie nie hinter dem Code zurückbleibt.
_NACHFUEHREN = 'Basislinie nachführen mit uv run just kosten-basislinie "<Grund mit Verweis>"'
# Ein Grund verweist auf eine Entscheidung, einen Befund oder einen Plan (KTD7).
_VERWEISE = (
    "nötig ist E-nnn mit Datei in compound-packs/swarmq-entscheidungen/, eine Befund-ID "
    "aus einem Register unter docs/befunde/ oder ein existierender Pfad docs/plans/<datei>.md"
)

_ENTSCHEIDUNG = re.compile(r"\bE-(\d{3})\b")
_BEFUND = re.compile(r"\b[A-Z][A-Z0-9]*-\d+\b")
_PLAN = re.compile(r"docs/plans/[^\s,;()]+\.md")


def vergleiche(basislinie: Mapping[str, Any], szenario: str, gemessen: Mapping[str, int], python: str) -> list[str]:
    """Abweichungen der Messung *gemessen* von der Basislinie, leer bei Gleichheit."""
    if python != basislinie["python"]:
        # Modulzahlen hängen an der Python-Version (KTD5): lieber eine klare
        # Meldung als falsche Zahlen.
        return [
            f"Python {python} statt {basislinie['python']} aus der Basislinie; "
            "die Kosten gelten nur für die gepinnte Version (.python-version)"
        ]
    if szenario not in basislinie["werte"]:
        return [f"{szenario}: ohne Basislinienwert; {_NACHFUEHREN}"]
    erwartet: Mapping[str, int] = basislinie["werte"][szenario]
    fehler: list[str] = []
    for metrik in sorted(erwartet.keys() | gemessen.keys()):
        alt, neu = erwartet.get(metrik, 0), gemessen.get(metrik, 0)
        if neu > alt:
            fehler.append(f"{szenario}: {metrik} gestiegen, {alt} -> {neu}")
        elif neu < alt:
            fehler.append(f"{szenario}: {metrik} gesunken, {alt} -> {neu}; {_NACHFUEHREN}")
    return fehler


def grund_fehler(grund: str, wurzel: Path) -> str | None:
    """Warum *grund* nicht reicht, ``None`` wenn er reicht."""
    if not grund.strip():
        return "Grund fehlt"
    if _aufloesbare_verweise(grund, wurzel):
        return None
    return f"Grund ohne auflösbaren Verweis: {grund!r}; {_VERWEISE}"


def _aufloesbare_verweise(grund: str, wurzel: Path) -> list[str]:
    """Die Verweise in *grund*, die im Repo unter *wurzel* existieren."""
    gefunden: list[str] = []
    pack = wurzel / "compound-packs" / "swarmq-entscheidungen"
    for nummer in _ENTSCHEIDUNG.findall(grund):
        if any(pack.glob(f"e{nummer}-*.md")):
            gefunden.append(f"E-{nummer}")
    befunde = [befund for befund in _BEFUND.findall(grund) if not _ENTSCHEIDUNG.fullmatch(befund)]
    if befunde:
        register = [
            datei.read_text(encoding="utf-8")
            for datei in sorted((wurzel / "docs" / "befunde").glob("*.md"))
            if datei.name != "README.md"
        ]
        gefunden.extend(
            befund for befund in befunde if any(re.search(rf"\b{re.escape(befund)}\b", text) for text in register)
        )
    gefunden.extend(plan for plan in _PLAN.findall(grund) if (wurzel / plan).is_file())
    return gefunden


def aktualisiere(
    basislinie: Mapping[str, Any], messung: Mapping[str, Mapping[str, int]], python: str, grund: str, datum: str
) -> dict[str, Any]:
    """Die Basislinie mit den Werten aus *messung*; die Historie bekommt einen
    Eintrag mit alten und neuen Werten, alte Einträge bleiben, wie sie sind."""
    alte_werte: Mapping[str, Mapping[str, int]] = basislinie["werte"]
    aenderungen: list[dict[str, Any]] = []
    for szenario in sorted(alte_werte.keys() | messung.keys()):
        vorher, nachher = alte_werte.get(szenario, {}), messung.get(szenario, {})
        for metrik in sorted(vorher.keys() | nachher.keys()):
            if vorher.get(metrik) == nachher.get(metrik):
                continue
            aenderung: dict[str, Any] = {"szenario": szenario, "metrik": metrik}
            if metrik in vorher:
                aenderung["alt"] = vorher[metrik]
            if metrik in nachher:
                aenderung["neu"] = nachher[metrik]
            aenderungen.append(aenderung)
    historie = list(basislinie["historie"])
    if aenderungen or python != basislinie["python"]:
        historie.append({"datum": datum, "grund": grund, "python": python, "aenderungen": aenderungen})
    werte = {szenario: dict(metriken) for szenario, metriken in messung.items()}
    return {"python": python, "werte": werte, "historie": historie}


_KOPF = """\
# Basislinie der Kostensuite (Plan 2026-10-24-1118, U3, KTD7).
# Nicht von Hand ändern: uv run just kosten-basislinie "<Grund mit Verweis>"
# schreibt die Werte neu und hängt einen Historieneintrag an.
"""


def als_toml(basislinie: Mapping[str, Any]) -> str:
    """TOML-Text der Basislinie (``tomllib`` liest nur; das Format ist fest)."""
    zeilen = [_KOPF, f"python = {_text(basislinie['python'])}"]
    for szenario, metriken in basislinie["werte"].items():
        zeilen += ["", f"[werte.{_schluessel(szenario)}]"]
        zeilen += [f"{_schluessel(metrik)} = {wert}" for metrik, wert in metriken.items()]
    for eintrag in basislinie["historie"]:
        zeilen += ["", "[[historie]]"]
        zeilen += [f"{feld} = {_text(eintrag[feld])}" for feld in ("datum", "grund", "python")]
        if not eintrag["aenderungen"]:
            zeilen.append("aenderungen = []")
            continue
        zeilen.append("aenderungen = [")
        for aenderung in eintrag["aenderungen"]:
            felder = ", ".join(
                f"{feld} = {_text(wert) if isinstance(wert, str) else wert}" for feld, wert in aenderung.items()
            )
            zeilen.append(f"    {{ {felder} }},")
        zeilen.append("]")
    return "\n".join(zeilen) + "\n"


def _text(wert: str) -> str:
    # JSON-Strings sind gültige TOML-Basisstrings (dieselben Escapes).
    return json.dumps(wert, ensure_ascii=False)


def _schluessel(name: str) -> str:
    return name if re.fullmatch(r"[A-Za-z0-9_-]+", name) else _text(name)


def unbelegte_anstiege(basislinie: Mapping[str, Any], wurzel: Path) -> list[str]:
    """Historieneinträge mit einem Anstieg, deren Grund keinen auflösbaren
    Verweis trägt (Wächter, KTD7)."""
    gemeldet: list[str] = []
    for eintrag in basislinie["historie"]:
        anstieg = any(
            "neu" in aenderung and aenderung["neu"] > aenderung.get("alt", 0) for aenderung in eintrag["aenderungen"]
        )
        if anstieg and grund_fehler(eintrag["grund"], wurzel) is not None:
            gemeldet.append(f"{eintrag['datum']}: Anstieg ohne auflösbaren Verweis im Grund {eintrag['grund']!r}")
    return gemeldet


def handaenderungen(basislinie: Mapping[str, Any]) -> list[str]:
    """Abweichungen der ``werte`` von der nachgespielten Historie, also
    Handänderungen ohne Historieneintrag (Wächter, KTD7)."""
    nachgespielt: dict[str, dict[str, int]] = {}
    for eintrag in basislinie["historie"]:
        for aenderung in eintrag["aenderungen"]:
            metriken = nachgespielt.setdefault(aenderung["szenario"], {})
            if "neu" in aenderung:
                metriken[aenderung["metrik"]] = aenderung["neu"]
            else:
                # Nur ``alt``: Die Metrik fiel weg (``aktualisiere``).
                metriken.pop(aenderung["metrik"], None)
    werte: Mapping[str, Mapping[str, int]] = basislinie["werte"]
    gemeldet: list[str] = []
    for szenario in sorted(nachgespielt.keys() | werte.keys()):
        historie, datei = nachgespielt.get(szenario, {}), werte.get(szenario, {})
        for metrik in sorted(historie.keys() | datei.keys()):
            if historie.get(metrik) != datei.get(metrik):
                gemeldet.append(
                    f"{szenario}: {metrik} laut Historie {historie.get(metrik)}, in werte {datei.get(metrik)}; "
                    f"{_NACHFUEHREN}"
                )
    return gemeldet


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Basislinie der Kostensuite neu schreiben")
    parser.add_argument("grund", help="Grund mit Verweis: E-nnn, Befund-ID oder docs/plans/<datei>.md")
    parser.add_argument("messung", type=Path, help="JSON-Zeilen der Kostensuite (SWARMQ_KOSTEN_MESSUNG)")
    parser.add_argument("--wurzel", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--entfernen",
        action="append",
        default=[],
        metavar="SZENARIO",
        help="Szenario ohne Messung ausdrücklich aus der Basislinie nehmen (mehrfach möglich)",
    )
    args = parser.parse_args(argv)
    fehler = grund_fehler(args.grund, args.wurzel)
    if fehler is not None:
        print(fehler, file=sys.stderr)
        return 2
    zeilen = [json.loads(zeile) for zeile in args.messung.read_text(encoding="utf-8").splitlines() if zeile]
    messung = {z["szenario"]: dict(sorted(z["werte"].items())) for z in sorted(zeilen, key=lambda z: z["szenario"])}
    python = zeilen[0]["python"]
    pfad = args.wurzel / BASISLINIE
    alt = (
        tomllib.loads(pfad.read_text(encoding="utf-8"))
        if pfad.exists()
        else {"python": python, "werte": {}, "historie": []}
    )
    # Ein Teil ohne Messung (etwa kein Test-Valkey, alle Tests übersprungen)
    # darf seine Szenarien nicht still löschen.
    fehlend = sorted(alt["werte"].keys() - messung.keys() - set(args.entfernen))
    if fehlend:
        print(
            f"Szenarien der Basislinie fehlen in der Messung: {', '.join(fehlend)}; "
            "lief jeder Teil? Entfernen nur ausdrücklich mit --entfernen <szenario>",
            file=sys.stderr,
        )
        return 2
    neu = aktualisiere(alt, messung, python, args.grund, datetime.date.today().isoformat())
    pfad.parent.mkdir(parents=True, exist_ok=True)
    pfad.write_text(als_toml(neu), encoding="utf-8")
    print(f"{pfad.relative_to(args.wurzel)}: {len(neu['historie']) - len(alt['historie'])} neuer Historieneintrag")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
