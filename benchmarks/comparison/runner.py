#!/usr/bin/env python3
"""Runner des Vergleichs (Plan 2026-10-24-1118, KTD13).

Der Runner plant die Kombinationen aus System, Szenario und Profil reihum und
startet jede in einem frischen Prozess mit dem Python der Vergleichsumgebung
des Systems (``.venvs/<system>``, angelegt mit ``einrichten.sh``). Hängt ein
Prozess länger als ``--zeitlimit``, beendet ihn der Hänger-Wächter, und der
Vergleich geht weiter.

Usage:
    python runner.py --scenario noop,io_bound --systems all --profile ab_werk,stark --runs 3
    python runner.py --scenario all --systems all --parallelitaet gleich
    python runner.py --scenario all --systems all

Intern (eine Kombination, im Python der Systemumgebung):
    python runner.py --einzeln --systems celery --scenario noop --profile stark --parallelitaet gleich
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import functools
import importlib.metadata
import os
import platform
import signal
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from garantieprofile import PAKETE, PROFILNAMEN, STARK
from parallelitaet import AB_WERK
from parallelitaet import NAMEN as PARALLELITAETEN
from scenarios.base import save_result

VERGLEICH_DIR = Path(__file__).parent
RESULTS_DIR = VERGLEICH_DIR / "results"
COMPOSE_PATH = VERGLEICH_DIR / "docker-compose.yml"
VENVS_DIR = VERGLEICH_DIR / ".venvs"


@dataclass(frozen=True)
class Kombination:
    """Ein Lauf eines Szenarios gegen ein System in einem Profil und einer Parallelität (``parallelitaet.py``)."""

    run: int
    szenario: str
    profil: str
    system: str
    parallelitaet: str = AB_WERK


def reihenfolge(
    systeme: list[str],
    szenarien: list[str],
    profile: list[str],
    runs: int,
    parallelitaeten: tuple[str, ...] | list[str] = (AB_WERK,),
) -> list[Kombination]:
    """Alle Kombinationen, reihum über die Systeme (KTD13).

    Das System wechselt bei jedem Schritt, damit Drift der Maschine (Wärme,
    Hintergrundlast) alle Systeme gleich trifft statt eines im Block.
    """
    return [
        Kombination(run, szenario, profil, system, parallelitaet)
        for run in range(1, runs + 1)
        for szenario in szenarien
        for parallelitaet in parallelitaeten
        for profil in profile
        for system in systeme
    ]


@dataclass(frozen=True)
class Ausgang:
    """Wie der Prozess einer Kombination endete: ``ok``, ``fehler`` oder ``haenger``."""

    kombination: Kombination
    status: str
    pid: int


async def fuehre_aus(
    kombination: Kombination,
    kommando: list[str],
    zeitlimit: float,
    frist: float = 10.0,
) -> Ausgang:
    """Startet eine Kombination als eigenen Prozess und wacht über das Zeitlimit.

    Ein frischer Prozess je Kombination, weil ein hängender Client (etwa der
    aiormq-Reader, docs/solutions/logic-errors/aiormq-reader-leak-and-stale-
    connection-recovery-2026-04-16.md) sonst alle folgenden Läufe mitreißt.
    Nach dem Zeitlimit bekommt der Prozess SIGTERM; sein Handler beendet die
    Worker-Prozesse (``worker_prozess.beende_alle``). Nach ``frist`` folgt
    SIGKILL an seine Gruppe.
    """
    prozess = await asyncio.create_subprocess_exec(*kommando, cwd=VERGLEICH_DIR, start_new_session=True)
    try:
        code = await asyncio.wait_for(prozess.wait(), zeitlimit)
    except TimeoutError:
        prozess.terminate()
        try:
            await asyncio.wait_for(prozess.wait(), frist)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(prozess.pid, signal.SIGKILL)
            await prozess.wait()
        return Ausgang(kombination, "haenger", prozess.pid)
    return Ausgang(kombination, "ok" if code == 0 else "fehler", prozess.pid)


def _images(compose: Path) -> dict[str, str]:
    """Image je Dienst aus der Compose-Datei des Vergleichsaufbaus."""
    images: dict[str, str] = {}
    dienst = ""
    for zeile in compose.read_text().splitlines():
        # Dienste stehen unter ``services:`` mit zwei Leerzeichen Einzug.
        if zeile.startswith("  ") and not zeile.startswith("   ") and zeile.rstrip().endswith(":"):
            dienst = zeile.strip().rstrip(":")
        elif zeile.strip().startswith("image:"):
            images[dienst] = zeile.split("image:", 1)[1].strip()
    return images


def metadaten(system: str, profil: str, parallelitaet: str) -> dict[str, Any]:
    """Versionen, Image-Tags, Host, Profil und Parallelität eines Laufs (R22).

    Die Versionen kommen aus der Umgebung, in der der Lauf tatsächlich
    stattfindet; fehlt eine Verteilung, bricht der Lauf ab.
    """
    return {
        "profil": profil,
        "parallelitaet": parallelitaet,
        "versionen": {paket: importlib.metadata.version(paket) for paket in PAKETE[system]},
        "images": _images(COMPOSE_PATH),
        "host": {
            "name": platform.node(),
            "plattform": platform.platform(),
            "cpus": os.cpu_count(),
            "python": platform.python_version(),
        },
    }


def get_scenarios() -> dict[str, object]:
    """Static registry of available scenarios."""
    from scenarios.noop import run_noop
    from scenarios.io_bound import run_io_bound
    from scenarios.latency import run_latency
    from scenarios.scaling import run_scaling

    return {
        "noop": run_noop,
        "io_bound": run_io_bound,
        "latency": run_latency,
        "scaling": run_scaling,
    }


def get_swarmq_scenarios() -> dict[str, object]:
    """Static registry of SwarmQ-specific scenarios."""
    from scenarios.quorum_classic import run_quorum_classic
    from scenarios.semaphore_scale import run_semaphore_scale
    from scenarios.polling_interval import run_polling_interval

    return {
        "quorum_classic": run_quorum_classic,
        "semaphore_scale": run_semaphore_scale,
        "polling_interval": run_polling_interval,
    }


def get_systems() -> dict[str, type]:
    """Die fünf Systeme des Vergleichs (R19). Die Adapter laden ihre Bibliothek erst in ``setup()``."""
    from systems.arq_sys import ArqSystem
    from systems.celery_sys import CelerySystem
    from systems.dramatiq_sys import DramatiqSystem
    from systems.swarmq_sys import SwarmQSystem
    from systems.taskiq_rmq_sys import TaskIQRabbitMQSystem

    return {
        "swarmq": SwarmQSystem,
        "celery": CelerySystem,
        "dramatiq": DramatiqSystem,
        "arq": ArqSystem,
        "taskiq": TaskIQRabbitMQSystem,
    }


def kind_kommando(kombination: Kombination, venvs: Path) -> list[str]:
    """Aufruf des Runners für eine Kombination im Python der Systemumgebung."""
    return [
        str(venvs / kombination.system / "bin" / "python"),
        str(VERGLEICH_DIR / "runner.py"),
        "--einzeln",
        "--systems",
        kombination.system,
        "--scenario",
        kombination.szenario,
        "--profile",
        kombination.profil,
        "--parallelitaet",
        kombination.parallelitaet,
    ]


async def einzeln(system: str, szenario: str, profil: str, parallelitaet: str) -> None:
    """Eine Kombination im aktuellen Prozess: Szenario laufen lassen, Ergebnis mit Metadaten speichern."""
    import worker_prozess

    def _beenden(signum: int, _frame: object) -> None:
        # Der Hänger-Wächter schickt SIGTERM: Worker mitnehmen, sonst laufen
        # sie in eigenen Prozessgruppen weiter und stören die nächste Messung.
        worker_prozess.beende_alle()
        os._exit(128 + signum)

    signal.signal(signal.SIGTERM, _beenden)

    scenario_fn = {**get_scenarios(), **get_swarmq_scenarios()}[szenario]
    system_cls = functools.partial(get_systems()[system], profil=profil, parallelitaet=parallelitaet)
    result = await scenario_fn(system_cls)
    result.meta = metadaten(system, profil, parallelitaet)
    pfad = save_result(result, RESULTS_DIR)
    print(f"  {result.system_name}/{profil}/{parallelitaet}: {result.throughput:.0f} tasks/s -> {pfad.name}")


def _auswahl(wert: str, alle: list[str]) -> list[str]:
    return list(alle) if wert == "all" else [teil.strip() for teil in wert.split(",")]


def plane(args: argparse.Namespace) -> list[Kombination]:
    """Kombinationen des Laufs; swarmq-eigene Szenarien laufen nur für swarmq."""
    allgemein = list(get_scenarios())
    nur_swarmq = list(get_swarmq_scenarios())
    szenarien = _auswahl(args.scenario, allgemein)
    unbekannt = [s for s in szenarien if s not in allgemein + nur_swarmq]
    systeme = _auswahl(args.systems, list(PAKETE))
    unbekannt += [s for s in systeme if s not in PAKETE]
    profile = _auswahl(args.profile, list(PROFILNAMEN))
    unbekannt += [p for p in profile if p not in PROFILNAMEN]
    parallelitaeten = _auswahl(args.parallelitaet, list(PARALLELITAETEN))
    unbekannt += [p for p in parallelitaeten if p not in PARALLELITAETEN]
    if unbekannt:
        raise SystemExit(f"Unbekannt: {', '.join(unbekannt)}")
    allgemeine = [s for s in szenarien if s in allgemein]
    return reihenfolge(systeme, allgemeine, profile, args.runs, parallelitaeten) + reihenfolge(
        ["swarmq"], [s for s in szenarien if s in nur_swarmq], [STARK], args.runs
    )


async def main(args: argparse.Namespace) -> int:
    plan = plane(args)
    fehlend = sorted(
        {str(args.venvs / k.system) for k in plan if not (args.venvs / k.system / "bin" / "python").exists()}
    )
    if fehlend:
        print(f"Vergleichsumgebungen fehlen: {', '.join(fehlend)}. Erst ./einrichten.sh ausführen.")
        return 2

    print(f"Vergleich: {len(plan)} Kombinationen, Zeitlimit {args.zeitlimit:.0f} s je Kombination")
    print(f"Results → {RESULTS_DIR}")
    ausgaenge = []
    for nummer, kombination in enumerate(plan, start=1):
        k = kombination
        print(f"[{nummer}/{len(plan)}] run {k.run} {k.szenario}/{k.system}/{k.profil}/{k.parallelitaet}", flush=True)
        ausgang = await fuehre_aus(k, kind_kommando(k, args.venvs), zeitlimit=args.zeitlimit)
        if ausgang.status != "ok":
            print(f"  {ausgang.status.upper()}")
        ausgaenge.append(ausgang)

    nicht_ok = [a for a in ausgaenge if a.status != "ok"]
    for a in nicht_ok:
        k = a.kombination
        print(f"{a.status}: run {k.run} {k.szenario}/{k.system}/{k.profil}/{k.parallelitaet}")
    print("Done. Run `python report.py` to see results.")
    return 1 if nicht_ok else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark comparison runner")
    parser.add_argument(
        "--scenario",
        default="all",
        help="Comma-separated scenario names or 'all' (default: all general scenarios)",
    )
    parser.add_argument(
        "--systems",
        default="all",
        help="Comma-separated system names or 'all' (default: all)",
    )
    parser.add_argument(
        "--profile",
        default="all",
        help="Comma-separated profiles (ab_werk, stark) or 'all' (default: all)",
    )
    parser.add_argument(
        "--parallelitaet",
        default="all",
        help="Comma-separated worker concurrency settings (gleich, ab_werk) or 'all' (default: all)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of runs per combination (default: 3)",
    )
    parser.add_argument(
        "--zeitlimit",
        type=float,
        default=900.0,
        help="Hänger-Wächter: seconds per combination before it is stopped (default: 900)",
    )
    parser.add_argument(
        "--venvs",
        type=Path,
        default=VENVS_DIR,
        help="Directory with one virtualenv per system (default: .venvs)",
    )
    parser.add_argument("--einzeln", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.einzeln:
        try:
            import uvloop

            loop_factory = uvloop.new_event_loop
        except ImportError:
            loop_factory = None
        asyncio.run(einzeln(args.systems, args.scenario, args.profile, args.parallelitaet), loop_factory=loop_factory)
        sys.exit(0)
    sys.exit(asyncio.run(main(args)))
