"""Garantie-Vergleich unter Störungen (Plan 2026-10-24-1118, U9, KTD14, R21, R22).

Startet den eigenen Compose-Stack (``compose.yml``, Projekt
``swarmq-vergleich-stoerung``), fährt jede Kombination aus System, Profil und
Störung reihum als eigenen Prozess (``stoerungen/lauf.py`` im Python der
Vergleichsumgebung des Systems), zieht aus jedem Hauptbuch die Bilanz und
entfernt den Stack am Ende mit ``down -v``. Misst keine Zeit; läuft deshalb
auch auf dem Laptop.

Aufruf (aus der Wurzel des Repos):
    uv run just vergleich-garantie                     # alles, ein Durchgang
    uv run just vergleich-garantie --runs 2            # zwei Durchgänge
    uv run just vergleich-garantie --systems swarmq --stoerungen sigkill

Ergebnisse: ``stoerungen/ergebnisse/<zeitstempel>/`` mit Hauptbuch und
Bilanz je Kombination und ``bilanz.md``.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import platform
import signal
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import runner
from garantieprofile import PAKETE, PROFILNAMEN
from stoerungen import stack, toxiproxy
from stoerungen.bilanz import LeeresHauptbuch, bilanz
from stoerungen.hauptbuch import Hauptbuch
from stoerungen.lauf import VALKEY_DATENVERLUST, WORKER_NEUSTART

STOERUNGEN = (stack.SIGKILL, stack.BROKER_NEUSTART, stack.NETZTRENNUNG, stack.VALKEY_PERSISTENT, stack.VALKEY_FLUECHTIG)
ERGEBNISSE_DIR = Path(__file__).with_name("ergebnisse")

_SPALTEN = ("bestaetigt", "ausgefuehrt", "verluste", "duplikate", "unbestimmt", "abgelehnt", "duplikate_e077")
_KOPF = (
    "| System | Profil | Störung | Status | bestätigt | ausgeführt | Verluste | Duplikate | unbestimmt "
    "| abgelehnt | Duplikate E-077 | Worker neu gestartet |\n"
    "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|"
)


def tabelle(eintraege: list[dict[str, Any]]) -> str:
    """Bilanzen als Markdown-Tabelle; ein leerer oder gescheiterter Lauf zeigt keine Zahlen."""
    zeilen = [_KOPF]
    for e in eintraege:
        zahlen = [str(e["bilanz"][s]) if e["bilanz"] else "-" for s in _SPALTEN]
        neustart = "ja" if e["worker_neustart"] else "nein"
        zeilen.append(
            f"| {e['system']} | {e['profil']} | {e['stoerung']} | {e['status']} | {' | '.join(zahlen)} | {neustart} |"
        )
    return "\n".join(zeilen) + "\n"


async def _stack_herstellen() -> None:
    """Nach jeder Kombination: alle Proxys an, alle Dienste gesund, für die nächste."""
    await asyncio.to_thread(toxiproxy.verbinden, stack.ADMIN_URL, stack.PROXYS)
    await stack.compose("up", "-d", "--wait")


def _worker_beenden(pids: list[int]) -> None:
    """SIGKILL an die Gruppe jedes Workers, den der Lauf gestartet hat; keiner bleibt zurück."""
    for pid in pids:
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(pid, signal.SIGKILL)


async def _kombination(k: runner.Kombination, ordner: Path, args: argparse.Namespace) -> dict[str, Any]:
    pfad = ordner / f"run{k.run}" / f"{k.system}-{k.profil}-{k.szenario}.sqlite"
    pfad.parent.mkdir(parents=True, exist_ok=True)
    kommando = [
        str(args.venvs / k.system / "bin" / "python"),
        "-m",
        "stoerungen.lauf",
        "--system",
        k.system,
        "--profil",
        k.profil,
        "--stoerung",
        k.szenario,
        "--hauptbuch",
        str(pfad),
        "--ruhe",
        str(args.ruhe),
        "--obergrenze",
        str(args.obergrenze),
    ]
    ausgang = await runner.fuehre_aus(k, kommando, zeitlimit=args.zeitlimit)
    buch = Hauptbuch(pfad)
    _worker_beenden(buch.worker_pids())
    await _stack_herstellen()

    meta_pfad = pfad.with_suffix(".meta.json")
    eintrag: dict[str, Any] = {
        "run": k.run,
        "system": k.system,
        "profil": k.profil,
        "stoerung": k.szenario,
        "status": ausgang.status,
        "worker_neustart": buch.ereignis_zeit(WORKER_NEUSTART) is not None,
        "meta": json.loads(meta_pfad.read_text()) if meta_pfad.exists() else {},
        "bilanz": None,
    }
    try:
        ergebnis = bilanz(*buch.zeilen(), valkey_datenverlust=buch.ereignis_zeit(VALKEY_DATENVERLUST))
    except LeeresHauptbuch:
        eintrag["status"] = "leer"
    else:
        eintrag["bilanz"] = asdict(ergebnis)
        if ergebnis.leer and ausgang.status == "ok":
            eintrag["status"] = "leer"
    pfad.with_suffix(".json").write_text(json.dumps(eintrag, indent=2))
    return eintrag


def _auswahl(wert: str, alle: tuple[str, ...] | list[str]) -> list[str]:
    auswahl = list(alle) if wert == "all" else [teil.strip() for teil in wert.split(",")]
    unbekannt = [a for a in auswahl if a not in alle]
    if unbekannt:
        raise SystemExit(f"Unbekannt: {', '.join(unbekannt)}")
    return auswahl


async def main(args: argparse.Namespace) -> int:
    plan = runner.reihenfolge(
        _auswahl(args.systems, list(PAKETE)),
        _auswahl(args.stoerungen, STOERUNGEN),
        _auswahl(args.profile, PROFILNAMEN),
        args.runs,
    )
    fehlend = sorted({k.system for k in plan if not (args.venvs / k.system / "bin" / "python").exists()})
    if fehlend:
        print(f"Vergleichsumgebungen fehlen: {', '.join(fehlend)}. Erst benchmarks/comparison/einrichten.sh ausführen.")
        return 2

    ordner = args.ergebnisse / time.strftime("%Y%m%d-%H%M%S")
    ordner.mkdir(parents=True)
    print(f"Garantie-Vergleich: {len(plan)} Kombinationen -> {ordner}", flush=True)
    eintraege: list[dict[str, Any]] = []
    await stack.compose("up", "-d", "--wait")
    try:
        for nummer, k in enumerate(plan, start=1):
            print(f"[{nummer}/{len(plan)}] run {k.run} {k.system}/{k.profil}/{k.szenario}", flush=True)
            eintrag = await _kombination(k, ordner, args)
            b = eintrag["bilanz"] or {}
            print(f"  {eintrag['status']}: " + ", ".join(f"{s}={b.get(s, '-')}" for s in _SPALTEN), flush=True)
            eintraege.append(eintrag)
    finally:
        if not args.behalten:
            await stack.compose("down", "-v")

    kopf = {
        "images": runner._images(stack.COMPOSE),  # noqa: SLF001 - derselbe Leser wie im Performance-Vergleich
        "host": {"name": platform.node(), "plattform": platform.platform(), "cpus": os.cpu_count()},
    }
    (ordner / "lauf.json").write_text(json.dumps({**kopf, "kombinationen": eintraege}, indent=2))
    bericht = "".join(
        f"\n## Durchgang {run}\n\n" + tabelle([e for e in eintraege if e["run"] == run])
        for run in sorted({e["run"] for e in eintraege})
    )
    (ordner / "bilanz.md").write_text(f"# Garantie-Vergleich {ordner.name}\n{bericht}")
    print(bericht)
    return 0 if all(e["status"] == "ok" for e in eintraege) else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Garantie-Vergleich unter Störungen")
    parser.add_argument("--systems", default="all", help="Systeme, kommagetrennt, oder 'all'")
    parser.add_argument("--profile", default="all", help="Profile (ab_werk, stark), kommagetrennt, oder 'all'")
    parser.add_argument("--stoerungen", default="all", help=f"Störungen ({', '.join(STOERUNGEN)}) oder 'all'")
    parser.add_argument("--runs", type=int, default=1, help="Durchgänge (Standard: 1)")
    parser.add_argument("--ruhe", type=float, default=20.0, help="Ruhephase: Sekunden ohne neue Ausführung")
    parser.add_argument("--obergrenze", type=float, default=360.0, help="Ruhephase: höchstens so viele Sekunden")
    parser.add_argument("--zeitlimit", type=float, default=900.0, help="Hänger-Wächter je Kombination, Sekunden")
    parser.add_argument("--venvs", type=Path, default=runner.VENVS_DIR, help="Vergleichsumgebungen (einrichten.sh)")
    parser.add_argument("--ergebnisse", type=Path, default=ERGEBNISSE_DIR, help="Ordner für die Ergebnisse")
    parser.add_argument("--behalten", action="store_true", help="Stack am Ende nicht entfernen")
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
