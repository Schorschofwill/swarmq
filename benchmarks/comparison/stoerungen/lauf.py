"""Eine Kombination des Garantie-Vergleichs (Plan 2026-10-24-1118, KTD14, R21).

Läuft als eigener Prozess im Python der Vergleichsumgebung des Systems
(``garantie.py`` startet ihn mit ``-m stoerungen.lauf`` in
``benchmarks/comparison``). Ablauf, für jedes System und Profil gleich:

1. Adapter einrichten, Worker als eigenen Prozess starten.
2. ``ANZAHL_VOR`` Tasks einreihen und warten, bis laut Hauptbuch einer läuft.
3. Die Störung auslösen und währenddessen weiter einreihen (höchstens
   ``ANZAHL_WAEHREND``, im Abstand ``ABSTAND_S``).
4. Danach ``ANZAHL_NACH`` Tasks einreihen.
5. Ruhephase (``ruhe_beendet``). Stirbt der Worker, startet ihn der Lauf neu
   wie ein Supervisor und hält das im Hauptbuch fest.

Die Bilanz zieht ``garantie.py`` aus dem Hauptbuch; dieser Prozess misst
keine Zeit.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib.metadata
import json
import os
import platform
import signal
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any

from garantieprofile import PAKETE
from stoerungen import stack, toxiproxy
from stoerungen.bilanz import ABGELEHNT, BESTAETIGT, UNBESTIMMT, bilanz
from stoerungen.hauptbuch import UMGEBUNG, Hauptbuch

ANZAHL_VOR = 30
ANZAHL_WAEHREND = 30
ANZAHL_NACH = 30
#: Dauer der Garantie-Task: lang genug, dass jede Störung laufende Tasks trifft.
DAUER_MS = 1000
ABSTAND_S = 0.1
#: Frist des Erzeugers je Publish; danach ist der Ausgang unbestimmt.
PUBLISH_FRIST_S = 10.0
#: Dauer der Netztrennung über Toxiproxy.
TRENNUNG_S = 10.0
#: So lange wartet der Lauf, bis vor der Störung ein Task nachweislich läuft.
LAUFEND_FRIST_S = 60.0
#: Ereignisse im Hauptbuch neben den Störungen selbst.
VALKEY_DATENVERLUST = "valkey_datenverlust"
WORKER_NEUSTART = "worker_neustart"


def ruhe_beendet(*, still_s: float, offen: int, vergangen_s: float, ruhe_s: float, obergrenze_s: float) -> bool:
    """Ob die Ruhephase nach einer Störung vorbei ist.

    Sie endet, wenn seit ``ruhe_s`` keine Ausführung dazukam und jeder
    bestätigte Publish ausgeführt ist. Ist noch einer offen, wartet sie bis
    ``obergrenze_s``: Manche Systeme stellen erst nach einer Frist neu zu
    (swarmq nach dem Running-Marker, 60 s; arq nach dem In-Progress-Schlüssel,
    310 s), und ein früheres Ende zählte das als Verlust.
    """
    return vergangen_s >= obergrenze_s or (offen == 0 and still_s >= ruhe_s)


class PublishFrist(Exception):
    """Der Publish kam innerhalb der Frist des Erzeugers nicht zurück; ob er ankam, ist offen."""


def ausgang_von(system: str, fehler: BaseException) -> str:
    """Ausgang eines Publishs, der mit ``fehler`` endete.

    Ein Fehler heißt für den Aufrufer „nicht erfolgt“ (abgelehnt). swarmq
    unterscheidet als einziges System den Fall, dass die Nachricht schon
    gesendet war (``PublishIndeterminate``, E-042): Der ist unbestimmt,
    ebenso ein Publish, der die Frist des Erzeugers überschritt.
    """
    if isinstance(fehler, PublishFrist):
        return UNBESTIMMT
    if system == "swarmq":
        from swarmq.exceptions import PublishIndeterminate

        if isinstance(fehler, PublishIndeterminate):
            return UNBESTIMMT
    return ABGELEHNT


async def _worker_starten(adapter: Any, buch: Hauptbuch) -> None:
    await adapter.start_worker()
    buch.worker(adapter.worker_pid)


async def _worker_beaufsichtigen(adapter: Any, buch: Hauptbuch) -> None:
    """Startet einen beendeten Worker neu, wie ein Supervisor im Betrieb."""
    if not adapter.worker_laeuft:
        buch.ereignis(WORKER_NEUSTART)
        await adapter.stop_worker()
        await _worker_starten(adapter, buch)


async def _publizieren(system: str, adapter: Any, buch: Hauptbuch) -> None:
    """Reiht eine Garantie-Task ein und schreibt den Ausgang ins Hauptbuch."""
    kennung = uuid.uuid4().hex
    try:
        await asyncio.wait_for(adapter.enqueue("garantie", {"kennung": kennung, "dauer_ms": DAUER_MS}), PUBLISH_FRIST_S)
    except TimeoutError:
        ausgang = ausgang_von(system, PublishFrist())
    except Exception as fehler:  # noqa: BLE001 - jeder Fehler des Systems ist ein Ausgang, kein Abbruch
        ausgang = ausgang_von(system, fehler)
    else:
        ausgang = BESTAETIGT
    buch.publish(kennung, ausgang)


async def _warten_bis_laufend(buch: Hauptbuch) -> None:
    ende = time.monotonic() + LAUFEND_FRIST_S
    while buch.laufend() == 0:
        if time.monotonic() > ende:
            raise RuntimeError(f"vor der Störung lief binnen {LAUFEND_FRIST_S:.0f} s kein Task")
        await asyncio.sleep(0.05)


async def _stoeren(stoerung: str, system: str, adapter: Any, buch: Hauptbuch) -> None:
    if stoerung == stack.SIGKILL:
        # Die ganze Prozessgruppe, also auch geforkte Pool-Prozesse: wie ein
        # Absturz der Maschine des Workers. Danach startet er neu.
        buch.ereignis(stoerung)
        os.killpg(adapter.worker_pid, signal.SIGKILL)
        await adapter.stop_worker()
        await _worker_starten(adapter, buch)
    elif stoerung == stack.NETZTRENNUNG:
        await asyncio.to_thread(toxiproxy.trennen, stack.ADMIN_URL, stack.PROXYS)
        buch.ereignis(stoerung)
        await asyncio.sleep(TRENNUNG_S)
        await asyncio.to_thread(toxiproxy.verbinden, stack.ADMIN_URL, stack.PROXYS)
    else:
        if stoerung == stack.VALKEY_FLUECHTIG:
            buch.ereignis(VALKEY_DATENVERLUST)
        buch.ereignis(stoerung)
        ziel = stack.dienst(stoerung, system)
        await stack.compose("restart", ziel)
        await stack.compose("up", "-d", "--wait", ziel)


async def _ruhephase(adapter: Any, buch: Hauptbuch, ruhe_s: float, obergrenze_s: float) -> None:
    beginn = time.monotonic()
    anzahl, seit = -1, beginn
    while True:
        publishes, ausfuehrungen = buch.zeilen()
        jetzt = time.monotonic()
        if len(ausfuehrungen) != anzahl:
            anzahl, seit = len(ausfuehrungen), jetzt
        offen = bilanz(publishes, ausfuehrungen).verluste
        if ruhe_beendet(
            still_s=jetzt - seit, offen=offen, vergangen_s=jetzt - beginn, ruhe_s=ruhe_s, obergrenze_s=obergrenze_s
        ):
            return
        await _worker_beaufsichtigen(adapter, buch)
        await asyncio.sleep(1.0)


def _metadaten(system: str, profil: str, stoerung: str) -> dict[str, Any]:
    return {
        "system": system,
        "profil": profil,
        "stoerung": stoerung,
        "versionen": {paket: importlib.metadata.version(paket) for paket in PAKETE[system]},
        "python": platform.python_version(),
    }


async def lauf(system: str, profil: str, stoerung: str, pfad: Path, ruhe_s: float, obergrenze_s: float) -> None:
    """Eine Kombination; das Ergebnis steht danach im Hauptbuch unter ``pfad``."""
    os.environ.update({**stack.umgebung(stoerung), UMGEBUNG: str(pfad)})
    buch = Hauptbuch(pfad)
    pfad.with_suffix(".meta.json").write_text(json.dumps(_metadaten(system, profil, stoerung), indent=2))

    # Erst nach den Variablen: ``systems.base`` liest die Adressen beim Import.
    from runner import get_systems

    adapter = get_systems()[system](profil=profil)
    await adapter.setup()
    try:
        await _worker_starten(adapter, buch)
        for _ in range(ANZAHL_VOR):
            await _publizieren(system, adapter, buch)
        await _warten_bis_laufend(buch)
        stoeren = asyncio.create_task(_stoeren(stoerung, system, adapter, buch))
        for _ in range(ANZAHL_WAEHREND):
            if stoeren.done():
                break
            await _publizieren(system, adapter, buch)
            await asyncio.sleep(ABSTAND_S)
        await stoeren
        await _worker_beaufsichtigen(adapter, buch)
        for _ in range(ANZAHL_NACH):
            await _publizieren(system, adapter, buch)
        await _ruhephase(adapter, buch, ruhe_s, obergrenze_s)
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(adapter.teardown(), 60.0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--system", required=True)
    parser.add_argument("--profil", required=True)
    parser.add_argument("--stoerung", required=True)
    parser.add_argument("--hauptbuch", type=Path, required=True)
    parser.add_argument("--ruhe", type=float, default=20.0)
    parser.add_argument("--obergrenze", type=float, default=360.0)
    args = parser.parse_args()

    # Ausgaben des Laufs und seiner Worker (sie erben die Deskriptoren) in
    # eine Datei neben dem Hauptbuch, nicht in die Übersicht des Vergleichs.
    log = os.open(args.hauptbuch.with_suffix(".log"), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(log, 1)
    os.dup2(log, 2)

    import worker_prozess

    def _beenden(signum: int, _frame: object) -> None:
        # Der Hänger-Wächter schickt SIGTERM: Worker mitnehmen.
        worker_prozess.beende_alle()
        os._exit(128 + signum)

    signal.signal(signal.SIGTERM, _beenden)
    code = 0
    # Kein ``asyncio.run``: Es wartet beim Schließen auf Threads, und ein
    # Publish synchroner Systeme, den die Frist abbrach, läuft in seinem
    # Thread weiter. ``os._exit`` am Ende beendet den Prozess trotzdem.
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(
            lauf(args.system, args.profil, args.stoerung, args.hauptbuch, args.ruhe, args.obergrenze)
        )
    except Exception:  # noqa: BLE001 - der Lauf meldet jeden Fehler und endet mit Code 1
        traceback.print_exc()
        code = 1
    worker_prozess.beende_alle()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


if __name__ == "__main__":
    main()
