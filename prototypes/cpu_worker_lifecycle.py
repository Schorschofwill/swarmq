"""Lebenszyklus-Serien G/H/I fuer den CPU-Worker-Modus — die Luecken, die
``cpu_worker_parallel.py`` offengelassen hat.

Drei davon nennt ``docs/cpu-worker-machbarkeit.md`` selbst als offen; die
vierte ist eine Entwurfsregel ohne Beleg: **alle bisherigen 36 Messpunkte
laufen mit einem frischen Kind je Task.** Kein einziger misst ein
*wiederverwendetes* Kind — obwohl die Regel „Task mit DI → Kind wird
wiederverwendet, Recycling nach RSS" genau darauf beruht.

    docker start swarmq-proto-rabbit swarmq-proto-valkey

    SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5699/" \
    SWARMQ_BACKEND_URL="redis://localhost:6399/0" \
    uv run --extra dishka --with numpy --with pillow \
        python -u prototypes/cpu_worker_lifecycle.py

``PROTO_SERIES=g|h|i|all`` waehlt die Serien, ``PROTO_MINUTES`` die Dauer
des Speicherdruck-Laufs (Vorgabe 3).

Geprueft wird:

  G-Serie — Speicherdruck ueber Zeit (E4 hielt 1,6 GB nur 1,5 Sekunden):
    G1  Alle Tasks bleiben unter Dauerdruck korrekt
    G2  Die Fork-Latenz haelt unter Speicherdruck ueber die Laufzeit
    G3  Der Durchsatz bricht unter Dauerdruck nicht ein
    G4  Loop-Prozess driftet auch unter Speicherdruck nicht
    G5  Der Systemspeicher kehrt nach dem Lauf zurueck

  H-Serie — Wiederverwendung, Recycling, Verbindungen (die unbelegte Regel):
    H1  Ein wiederverwendetes Kind haelt APP- und REQUEST-Scope auseinander
    H2  Wiederverwendung amortisiert den DI-Aufbau messbar
    H3  Recycling nach RSS greift und gibt den Speicher zurueck
    H4  N Kinder = N Verbindungen, und Recycling hinterlaesst keine Leiche
    H5  Rollierendes Recycling laesst den Slot nicht brachliegen
    H6  Die Slot-Queue uebersteht den Tod ihres Kindes

  I-Serie — Lebenszyklus am echten Broker (was der Zusammenbau riskiert):
    I1  Graceful Shutdown faehrt laufende Rechenkinder zu Ende
    I2  Kindtod vor dem Ack fuehrt zur Redelivery, nicht zum Verlust
    I3  Ein Langlaeufer geht durch; consumer_timeout ist die Obergrenze
    I4  Der Loop-Prozess hortet nicht mehr Nachrichten als er Slots hat
    I5  Eine Exception im Rechenkind traegt bis in den DLQ-Pfad
"""

from __future__ import annotations

import os

# MUSS vor jedem numpy-Import und vor dem Start des Forkservers stehen
# (Befund E7): OpenBLAS liest die Threadzahl beim Laden der Bibliothek,
# also im Forkserver — lange bevor ein Rechenkind existiert.
# PROTO_PIN=0 schaltet die Begrenzung ab, wie im Parallel-Prototyp.
if os.environ.get("PROTO_PIN", "1") == "1":
    for _var in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ.setdefault(_var, "1")

import asyncio
import json
import multiprocessing as mp
import random
import statistics
import subprocess
import sys
import time
from collections import deque
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT))

BROKER_URL = os.environ.get("SWARMQ_BROKER_URL", "amqp://guest:guest@localhost:5699/")
BACKEND_URL = os.environ.get("SWARMQ_BACKEND_URL", "redis://localhost:6399/0")
RABBIT_CONTAINER = os.environ.get("PROTO_RABBIT_CONTAINER", "swarmq-proto-rabbit")
QUEUE = "swarmq-cpu-proto-lc"

PRELOAD = ["prototypes.cpu_lifecycle_tasks"]
SLOTS = int(os.environ.get("PROTO_SLOTS", "4"))
MINUTES = float(os.environ.get("PROTO_MINUTES", "3"))
SERIES = os.environ.get("PROTO_SERIES", "all").lower()

_RESULTS: list[tuple[str, bool, str]] = []


def check(eid: str, ok: bool, detail: str) -> None:
    _RESULTS.append((eid, ok, detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {eid}  {detail}", flush=True)


def note(text: str) -> None:
    print(f"        {text}", flush=True)


# ---------------------------------------------------------------------------
# Messgroessen des Loop-Prozesses und des Hosts
# ---------------------------------------------------------------------------


def rss_mb() -> float:
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0


def fd_count() -> int:
    return len(os.listdir("/proc/self/fd"))


def meminfo_mb() -> tuple[float, float]:
    """(MemAvailable, SwapFree) in MB — fuer G5.

    Ohne den Systemblick misst der Speicherdrucktest nur den eigenen
    Prozess und uebersieht, dass der Host in den Swap gelaufen ist.
    """
    avail = swapfree = -1.0
    with open("/proc/meminfo") as fh:
        for line in fh:
            if line.startswith("MemAvailable:"):
                avail = int(line.split()[1]) / 1024
            elif line.startswith("SwapFree:"):
                swapfree = int(line.split()[1]) / 1024
    return avail, swapfree


def nachkommen() -> list[tuple[int, float, str]]:
    """Alle transitiven Nachkommen dieses Prozesses: (pid, RSS MB, Zustand).

    Transitiv, weil die Rechenkinder Kinder des *Forkservers* sind und
    nicht des Loop-Prozesses. Nach dem Lauf darf hier nur noch der
    Forkserver selbst stehen — das ist der deterministische Nachweis,
    dass die Kinder ihren Speicher wirklich zurueckgegeben haben, und
    er haengt nicht am verrauschten ``MemAvailable`` des Hosts.
    """
    eltern: dict[int, int] = {}
    daten: dict[int, tuple[float, str]] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            with open(f"/proc/{pid}/stat") as fh:
                fields = fh.read().rsplit(") ", 1)[1].split()
            zustand, ppid = fields[0], int(fields[1])
            rss_seiten = int(fields[21])
        except (OSError, IndexError, ValueError):
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                roh = fh.read().replace(b"\x00", b" ").decode(errors="replace")
        except OSError:
            roh = ""
        # Nur der sprechende Teil: "forkserver" bzw. "resource_tracker"
        # stehen im -c-Aufruf von multiprocessing.
        kurz = "rechenkind"
        for marke in ("forkserver", "resource_tracker"):
            if marke in roh:
                kurz = marke
                break
        eltern[pid] = ppid
        daten[pid] = (rss_seiten * 4096 / 1024 / 1024, kurz if zustand != "Z" else "zombie")

    me = os.getpid()
    out: list[tuple[int, float, str]] = []
    for pid in eltern:
        p, tiefe = eltern.get(pid), 0
        while p is not None and p > 1 and tiefe < 20:
            if p == me:
                out.append((pid, *daten[pid]))
                break
            p, tiefe = eltern.get(p), tiefe + 1
    return out


def steigung_mb_pro_min(proben: list[Any], index: int) -> float:
    """Lineare Regression statt Endpunktdifferenz.

    Die Lehre aus F2 lautet „die Steigung zaehlt". Zwei Endpunkte sind
    dafuer das schlechteste Schaetzverfahren: ein einzelner Ausreisser
    am Rand kippt das Ergebnis. Ueber alle Proben gerechnet nicht.
    """
    xs = [p[0] / 60 for p in proben]
    ys = [p[index] for p in proben]
    n = len(xs)
    if n < 3:
        return 0.0
    mx, my = sum(xs) / n, sum(ys) / n
    nenner = sum((x - mx) ** 2 for x in xs)
    if nenner <= 0:
        return 0.0
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / nenner


def own_zombies() -> list[int]:
    me = os.getpid()
    out: list[int] = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as fh:
                fields = fh.read().rsplit(") ", 1)[1].split()
            if fields[0] == "Z" and int(fields[1]) == me:
                out.append(int(entry))
        except (OSError, IndexError, ValueError):
            continue
    return out


# ---------------------------------------------------------------------------
# Kind-Entrypoints
# ---------------------------------------------------------------------------


class _QueueBackend:
    """Rueckkanal: die zwei Methoden, die Task.update_progress braucht."""

    def __init__(self, queue: Any) -> None:
        self._queue = queue

    async def set_progress(self, task_id: str, fields: dict[str, str], ttl_ms: int) -> None:
        self._queue.put(("progress", task_id, dict(fields), ttl_ms))

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        return None


def _kind_umgebung() -> tuple[Any, Any, Any, Any]:
    """Die Importe, die erst im Kind gebraucht werden.

    Bewusst hier und nicht auf Modulebene: der Loop-Prozess soll die
    Task-Module gar nicht kennen muessen — er rechnet nie.
    """
    from prototypes import cpu_lifecycle_tasks  # noqa: F401  (Preload-Modul)
    from swarmq.dishka import _MISSING, TaskContext, get_registered_container, request_container
    from swarmq.task import Task

    return Task, get_registered_container(), _MISSING, (TaskContext, request_container)


async def _fuehre_aus(
    task_basis: Any,
    scope_container: Any,
    ctx_tools: tuple[Any, Any],
    job: tuple[str, tuple[Any, ...], str],
    progress_q: Any,
) -> tuple[str, str, Any]:
    """Ein Task im REQUEST-Scope. Gibt (status, task_id, payload) zurueck."""
    kontext_typ, request_container = ctx_tools
    name, args, tid = job
    cls = next(c for c in task_basis._registered_tasks if c.task_info.name == name)

    inst = cls()
    inst._task_id = tid
    inst._backend = _QueueBackend(progress_q)
    inst._task_start_time = time.monotonic()
    inst._result_ttl_ms = 86_400_000

    scope = token = None
    if scope_container is not None:
        scope = scope_container(
            context={
                kontext_typ: kontext_typ(
                    task_id=tid, task_name=name, queue=QUEUE, retry_count=0
                )
            }
        )
        token = request_container.set(await scope.__aenter__())
    try:
        return ("ok", tid, await inst.run(*args))
    except Exception as exc:
        return ("error", tid, f"{type(exc).__name__}: {exc}")
    finally:
        if token is not None:
            request_container.reset(token)
        if scope is not None:
            await scope.__aexit__(None, None, None)


def child_once(
    task_name: str,
    args: tuple[Any, ...],
    task_id: str,
    progress_q: Any,
    result_q: Any,
    slot: int,
) -> None:
    """Frisches Kind: ein Task, dann Ende. Der Modus fuer Tasks ohne DI."""
    task_basis, container, missing, ctx_tools = _kind_umgebung()
    scope_container = None if container is missing else container

    async def _run() -> None:
        status, tid, payload = await _fuehre_aus(
            task_basis, scope_container, ctx_tools, (task_name, args, task_id), progress_q
        )
        result_q.put((status, tid, payload, slot, os.getpid(), rss_mb(), 1))
        if scope_container is not None:
            await scope_container.close()

    asyncio.run(_run())


def child_loop(job_q: Any, progress_q: Any, result_q: Any, slot: int) -> None:
    """Wiederverwendetes Kind: zieht Jobs, bis ``None`` kommt.

    Der APP-Scope wird beim ersten Task aufgeloest und lebt danach ueber
    alle Tasks dieses Kindes — das ist die Amortisation, die die
    Recycling-Regel ueberhaupt erst begruendet.
    """
    task_basis, container, missing, ctx_tools = _kind_umgebung()
    scope_container = None if container is missing else container

    async def _run() -> None:
        erledigt = 0
        while True:
            try:
                job = job_q.get(timeout=30.0)
            except Exception:
                break
            if job is None:
                break
            status, tid, payload = await _fuehre_aus(
                task_basis, scope_container, ctx_tools, job, progress_q
            )
            erledigt += 1
            # RSS wird NACH dem Taskende gemessen, nie waehrenddessen:
            # waehrend der Rechnung ist hoher Speicherbedarf legitim.
            result_q.put((status, tid, payload, slot, os.getpid(), rss_mb(), erledigt))
        if scope_container is not None:
            await scope_container.close()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Der Pool: ein Loop-Prozess, der nie rechnet — in beiden Betriebsarten
# ---------------------------------------------------------------------------


class Ergebnis:
    __slots__ = ("nr", "payload", "pid", "rss", "slot", "status", "task_id")

    def __init__(self, roh: tuple[Any, ...]) -> None:
        (self.status, self.task_id, self.payload, self.slot,
         self.pid, self.rss, self.nr) = roh


class SlotPool:
    """N Rechenslots, wahlweise mit frischem oder wiederverwendetem Kind.

    ``mode="fresh"``  — ein Kind je Task (der bisher gemessene Fall).
    ``mode="reuse"``  — ein Kind je Slot, recycelt nach RSS oder Taskzahl.

    Im ``reuse``-Modus hat **jeder Slot seine eigene Job-Queue**. Eine
    geteilte Job-Queue waere hier falsch: der Loop-Prozess muss ein
    bestimmtes Kind ansprechen koennen, um es zu recyceln — bei einer
    geteilten Queue zoege das Ende-Signal irgendein Kind. Die Zahl der
    Queues ist mit den Slots gedeckelt und waechst nicht mit den Tasks,
    also driftet auch nichts (Befund F3).
    """

    def __init__(
        self,
        fs: Any,
        slots: int,
        *,
        mode: str = "fresh",
        rss_limit_mb: float | None = None,
        max_tasks: int | None = None,
        keep_results: bool = True,
    ) -> None:
        self._fs = fs
        self.slots = slots
        self.mode = mode
        self.rss_limit_mb = rss_limit_mb
        self.max_tasks = max_tasks
        self.keep_results = keep_results

        self.pq = fs.Queue()
        self.rq = fs.Queue()

        # fresh: task_id -> Process. reuse: slot -> Zustand.
        self.laufend: dict[str, tuple[int, Any]] = {}
        self.kinder: dict[int, dict[str, Any]] = {}
        self.frei: list[int] = list(range(slots))

        self.results: dict[str, Ergebnis] = {}
        self.ok_count = 0
        self.err_count = 0
        self.wrong_count = 0
        self.errors: list[str] = []
        self.completed = 0
        self.busy_s = 0.0
        self.killed: set[str] = set()

        # Buchfuehrung, die NICHT pro Task wachsen darf (Lehre aus F2).
        # Ein einfacher Deckel waere hier falsch: er schnitte die SPAETEN
        # Werte ab, und genau die braucht G2 fuer den Vergleich Anfang
        # gegen Ende. Deshalb zwei feste Fenster — die ersten N und,
        # ringfoermig, die letzten N.
        self.spawn_anfang: list[float] = []
        self.spawn_ende: deque[float] = deque(maxlen=1000)
        self.spawn_n = 0
        self.spawn_summe = 0.0
        self.spawn_max = 0.0
        self.recyclings: list[tuple[int, str, float]] = []
        self.recycle_luecken_ms: list[float] = []
        self._start_ts: dict[str, float] = {}
        self._sterbend: list[tuple[Any, Any]] = []

    # -- Kindverwaltung ----------------------------------------------------

    def _starte_kind(self, slot: int) -> dict[str, Any]:
        job_q = self._fs.Queue()
        t0 = time.perf_counter()
        p = self._fs.Process(target=child_loop, args=(job_q, self.pq, self.rq, slot))
        p.start()
        self._merke_spawn((time.perf_counter() - t0) * 1000)
        return {"proc": p, "job_q": job_q, "tasks": 0, "rss": 0.0}

    def _merke_spawn(self, dt_ms: float) -> None:
        self.spawn_n += 1
        self.spawn_summe += dt_ms
        self.spawn_max = max(self.spawn_max, dt_ms)
        if len(self.spawn_anfang) < 1000:
            self.spawn_anfang.append(dt_ms)
        self.spawn_ende.append(dt_ms)

    def _recycle(self, slot: int, grund: str, messwert: float) -> None:
        """Rollierend: erst das Ersatzkind, dann das Ende des alten.

        Celerys ``max_tasks_per_child`` laesst den Slot waehrend des
        Neustarts brachliegen. Bei 6 ms Startzeit ist das unnoetig — der
        Slot ist wieder belegbar, bevor das alte Kind ueberhaupt tot ist.
        """
        alt = self.kinder.get(slot)
        t0 = time.perf_counter()
        self.kinder[slot] = self._starte_kind(slot)
        self.recycle_luecken_ms.append((time.perf_counter() - t0) * 1000)
        self.recyclings.append((slot, grund, messwert))
        if alt is not None:
            try:
                alt["job_q"].put(None)
            except Exception:
                pass
            self._sterbend.append((alt["proc"], alt["job_q"]))

    def _ernte_sterbende(self) -> None:
        rest: list[tuple[Any, Any]] = []
        for proc, job_q in self._sterbend:
            if proc.is_alive():
                rest.append((proc, job_q))
                continue
            proc.join()
            job_q.close()
            job_q.join_thread()
        self._sterbend = rest

    # -- Jobvergabe --------------------------------------------------------

    def _vergib(self, job: tuple[str, tuple[Any, ...], str]) -> None:
        slot = self.frei.pop()
        self._start_ts[job[2]] = time.perf_counter()
        if self.mode == "fresh":
            t0 = time.perf_counter()
            p = self._fs.Process(
                target=child_once, args=(job[0], job[1], job[2], self.pq, self.rq, slot)
            )
            p.start()
            self._merke_spawn((time.perf_counter() - t0) * 1000)
            self.laufend[job[2]] = (slot, p)
        else:
            if slot not in self.kinder:
                self.kinder[slot] = self._starte_kind(slot)
            self.kinder[slot]["job_q"].put(job)
            self.laufend[job[2]] = (slot, self.kinder[slot]["proc"])

    def _buche(self, erg: Ergebnis) -> None:
        """Zaehlwerk. Waechst bewusst nicht pro Task, wenn
        ``keep_results`` aus ist — sonst maesse der Dauerlauf die
        Buchfuehrung des Beobachters statt den Entwurf (Lehre aus F2)."""
        if erg.status == "ok":
            self.ok_count += 1
            if isinstance(erg.payload, dict) and not erg.payload.get("correct"):
                self.wrong_count += 1
        else:
            self.err_count += 1
            if len(self.errors) < 5:
                self.errors.append(f"{erg.task_id}: {erg.payload}")
        if self.keep_results:
            self.results[erg.task_id] = erg

    def _pruefe_recycling(self, slot: int, erg: Ergebnis) -> None:
        """Nach dem Taskende entscheiden, ob das Kind ersetzt wird.

        RSS ist die primaere Linie, die Taskzahl die zweite. Gemessen
        wird ausschliesslich hier — nie waehrend der Rechnung, wo hoher
        Speicherbedarf legitim ist.
        """
        kind = self.kinder.get(slot)
        if kind is None:
            return
        kind["tasks"] = erg.nr
        kind["rss"] = erg.rss
        if self.rss_limit_mb is not None and erg.rss > self.rss_limit_mb:
            self._recycle(slot, "rss", erg.rss)
        elif self.max_tasks is not None and erg.nr >= self.max_tasks:
            self._recycle(slot, "tasks", float(erg.nr))

    def _leere_rueckkanal(self) -> None:
        """Fortschrittsmeldungen abraeumen: der Rueckkanal darf nicht
        volllaufen, nur sein Inhalt ist hier nicht Gegenstand."""
        while True:
            try:
                self.pq.get_nowait()
            except Exception:
                break

    def _ernte(self) -> None:
        """Ergebnisse abholen und den Slot freigeben. Rechnet nie."""
        while True:
            try:
                roh = self.rq.get_nowait()
            except Exception:
                break
            erg = Ergebnis(roh)
            self._buche(erg)

            eintrag = self.laufend.pop(erg.task_id, None)
            if eintrag is None:
                continue
            self.busy_s += time.perf_counter() - self._start_ts.pop(
                erg.task_id, time.perf_counter()
            )
            self.completed += 1
            slot = eintrag[0]
            if self.mode == "reuse":
                self._pruefe_recycling(slot, erg)
            self.frei.append(slot)

        self._leere_rueckkanal()

    def _ernte_tote_fresh(self) -> None:
        """Im fresh-Modus: gestorbene Kinder einsammeln, damit kein Slot
        haengen bleibt, wenn ein Kind ohne Ergebnis endet (Kill)."""
        for tid, (slot, p) in list(self.laufend.items()):
            if not p.is_alive():
                p.join()
                del self.laufend[tid]
                self._start_ts.pop(tid, None)
                self.completed += 1
                self.frei.append(slot)

    def kill(self, task_id: str) -> bool:
        eintrag = self.laufend.get(task_id)
        if eintrag is None or not eintrag[1].is_alive():
            return False
        eintrag[1].kill()
        self.killed.add(task_id)
        if self.mode == "reuse":
            # Das Kind ist weg, sein Slot braucht ein neues — und der
            # Job ist verloren, genau wie beim echten Kindtod.
            slot = eintrag[0]
            del self.laufend[task_id]
            self._start_ts.pop(task_id, None)
            alt = self.kinder.pop(slot, None)
            if alt is not None:
                alt["proc"].join()
                self._sterbend.append((alt["proc"], alt["job_q"]))
            self.completed += 1
            self.frei.append(slot)
        return True

    async def run_all(
        self,
        jobs: Iterable[tuple[str, tuple[Any, ...], str]],
        *,
        deadline: float | None = None,
        beobachter: Any = None,
    ) -> None:
        it = iter(jobs)
        erschoepft = False
        while True:
            if deadline is not None and time.perf_counter() > deadline:
                erschoepft = True
            while not erschoepft and self.frei:
                try:
                    job = next(it)
                except StopIteration:
                    erschoepft = True
                    break
                self._vergib(job)

            if erschoepft and not self.laufend:
                break

            if self.mode == "fresh":
                self._ernte_tote_fresh()
            self._ernte()
            self._ernte_sterbende()
            if beobachter is not None:
                beobachter(self)
            await asyncio.sleep(0.002)
        self._ernte()

    def ok_values(self) -> list[dict[str, Any]]:
        return [r.payload for r in self.results.values() if r.status == "ok"]

    def close(self) -> None:
        for kind in self.kinder.values():
            try:
                kind["job_q"].put(None)
            except Exception:
                pass
        deadline = time.perf_counter() + 5.0
        for kind in self.kinder.values():
            rest = max(0.1, deadline - time.perf_counter())
            kind["proc"].join(timeout=rest)
            if kind["proc"].is_alive():
                kind["proc"].kill()
                kind["proc"].join()
        for _slot, (_s, p) in list(self.laufend.items()):
            if p.is_alive():
                p.kill()
            p.join()
        for proc, _q in self._sterbend:
            if proc.is_alive():
                proc.kill()
            proc.join()
        self.kinder.clear()
        self.laufend.clear()
        self._sterbend.clear()


class Jitter:
    """Laufende Statistik ueber den Loop-Jitter — mit KONSTANTEM Speicher.

    Eine Liste aller Messwerte waere bei 10-ms-Takt ueber fuenf Minuten
    rund 30.000 Eintraege und damit gut ein Megabyte, das im
    Loop-Prozess mitwaechst. Genau so hat sich der erste F2-Lauf selbst
    ein Leck gemessen. Der Median kommt deshalb aus einem Reservoir
    fester Groesse (Vitter's Algorithmus R), Anzahl und Maximum aus
    laufenden Zaehlern.
    """

    reservoir_groesse = 2000

    def __init__(self) -> None:
        self.n = 0
        self.max = 0.0
        self.summe = 0.0
        self._reservoir: list[float] = []

    def add(self, wert: float) -> None:
        self.n += 1
        self.summe += wert
        self.max = max(self.max, wert)
        if len(self._reservoir) < self.reservoir_groesse:
            self._reservoir.append(wert)
            return
        j = random.randrange(self.n)
        if j < self.reservoir_groesse:
            self._reservoir[j] = wert

    @property
    def median(self) -> float:
        return statistics.median(self._reservoir) if self._reservoir else 0.0

    def __len__(self) -> int:
        return self.n


async def heartbeat(stop: asyncio.Event, interval: float = 0.01) -> Jitter:
    """Misst, wie zuverlaessig der Loop-Prozess drankommt.

    Der Jitter ist die Zusage des Entwurfs: waehrend die Kinder rechnen,
    muss der Beobachter Broker, Health und Metriken bedienen koennen.
    """
    jitter = Jitter()
    last = time.perf_counter()
    while not stop.is_set():
        await asyncio.sleep(interval)
        now = time.perf_counter()
        jitter.add((now - last - interval) * 1000)
        last = now
    return jitter


async def waerme_forkserver(fs: Any) -> None:
    """Den einmaligen Hochlauf des Forkservers (rund 150 ms) vorwegnehmen,
    damit er keiner Messung angelastet wird."""
    warm = SlotPool(fs, 1)
    await warm.run_all([("proto.lc_blas", (200, 1), "warmup")])
    warm.close()


# ---------------------------------------------------------------------------
# G-Serie: Speicherdruck ueber Zeit
# ---------------------------------------------------------------------------

Probe = tuple[float, float, int, int, float, float]


async def series_g(fs: Any, minutes: float) -> None:
    print(f"\nG  Speicherdruck ueber {minutes:.0f} Minuten mit {SLOTS} Slots", flush=True)
    await waerme_forkserver(fs)

    avail0, swap0 = meminfo_mb()
    mb_je_kind = int(os.environ.get("PROTO_MB", "400"))
    note(f"MemAvailable vorher {avail0:.0f}MB, SwapFree {swap0:.0f}MB; "
         f"{SLOTS} Kinder x {mb_je_kind}MB = {SLOTS * mb_je_kind}MB Dauerdruck")

    def jobs() -> Iterator[tuple[str, tuple[Any, ...], str]]:
        i = 0
        while True:
            yield ("proto.lc_mem", (mb_je_kind, 0.25), f"g-{i}")
            i += 1

    pool = SlotPool(fs, SLOTS, mode="fresh", keep_results=False)
    proben: list[Probe] = []
    stop = asyncio.Event()

    async def sampler() -> None:
        t0 = time.perf_counter()
        while not stop.is_set():
            avail, swap = meminfo_mb()
            proben.append(
                (time.perf_counter() - t0, rss_mb(), fd_count(), pool.completed, avail, swap)
            )
            for _ in range(25):
                if stop.is_set():
                    break
                await asyncio.sleep(0.2)

    smp = asyncio.create_task(sampler())
    hb_stop = asyncio.Event()
    hb = asyncio.create_task(heartbeat(hb_stop))
    t0 = time.perf_counter()
    await pool.run_all(jobs(), deadline=t0 + minutes * 60)
    wall = time.perf_counter() - t0
    stop.set()
    hb_stop.set()
    await smp
    jitter = await hb
    pool.close()
    avail, swap = meminfo_mb()
    proben.append((wall, rss_mb(), fd_count(), pool.completed, avail, swap))

    _bewerte_g(pool, proben, jitter, wall, mb_je_kind)
    await _bewerte_g_speicher(proben, avail0, swap0)

    print("\n      Verlauf (s / Loop-RSS / FDs / fertig / MemAvail / SwapFree):", flush=True)
    schritt = max(1, len(proben) // 10)
    for probe in proben[::schritt]:
        print(f"        {probe[0]:6.0f}  {probe[1]:7.1f}  {probe[2]:4d}  {probe[3]:6d}  "
              f"{probe[4]:9.0f}  {probe[5]:9.0f}", flush=True)


async def _bewerte_g_speicher(proben: list[Probe], avail0: float, swap0: float) -> None:
    """G5 — kehrt der Speicher zurueck?

    Deterministisch geprueft wird, was uns gehoert: ueberlebende
    Nachkommen, Zombies, Swap. ``MemAvailable`` ist auf einem Host mit
    fremden Containern zu verrauscht fuer eine Schranke — es wird
    berichtet, nicht bewertet.
    """
    await asyncio.sleep(2.0)   # dem Kernel Zeit geben, die Seiten zurueckzunehmen
    avail, swap = meminfo_mb()
    avail_min = min(p[4] for p in proben)
    swap_min = min(p[5] for p in proben)
    zombies = own_zombies()
    rest = nachkommen()
    rest_rss = sum(r[1] for r in rest)
    einbruch = avail0 - avail_min
    # Auf 100 gedeckelt: der Host gibt waehrenddessen auch fremden
    # Speicher frei, und "zu 195% zurueck" liest sich wie ein Messfehler.
    erholt = min(100.0, (avail - avail_min) / einbruch * 100) if einbruch > 1 else 100.0
    # Erwartet bleiben genau die zwei Infrastrukturprozesse, die
    # multiprocessing dauerhaft haelt: der Forkserver und sein
    # resource_tracker. Ein ueberlebendes Rechenkind haette drei- bis
    # vierhundert MB und faellt sofort auf.
    rechenkinder = [r for r in rest if r[2] == "rechenkind"]
    check("G5", not zombies and not rechenkinder and rest_rss < 150
          and (swap0 - swap_min) < 100,
          f"nach dem Lauf {len(rest)} Nachkomme(n) mit zusammen {rest_rss:.0f}MB "
          f"({', '.join(f'{r[2]} {r[1]:.0f}MB' for r in rest) or 'keine'}), "
          f"davon {len(rechenkinder)} Rechenkinder, {len(zombies)} Zombies, SwapFree "
          f"{swap0:.0f} -> {swap:.0f}MB (Tiefpunkt {swap_min:.0f}MB); "
          f"MemAvailable brach um {einbruch:.0f}MB ein und ist zu {erholt:.0f}% "
          f"zurueck ({avail0:.0f} -> {avail:.0f}MB)")


def _bewerte_g(
    pool: SlotPool,
    proben: list[Probe],
    jitter: Jitter,
    wall: float,
    mb_je_kind: int,
) -> None:
    """G1 bis G4 — Korrektheit, Fork-Latenz, Durchsatz, Loop-Drift."""
    # --- G1: Korrektheit unter Dauerdruck ---------------------------------
    check("G1", pool.ok_count > 0 and pool.wrong_count == 0 and pool.err_count == 0,
          f"{pool.ok_count} Tasks mit je {mb_je_kind}MB in {wall:.0f}s, "
          f"{pool.wrong_count} falsch, {pool.err_count} Fehler"
          + (f" {pool.errors}" if pool.errors else ""))

    # --- G2: Fork-Latenz unter Druck --------------------------------------
    # Die 6 ms sind die Grundlage des ganzen Entwurfs. Wenn sie unter
    # Speicherdruck wegbrechen, wird "frisches Kind je Task" teuer.
    sp_frueh = statistics.median(pool.spawn_anfang) if pool.spawn_anfang else 0.0
    spaet = list(pool.spawn_ende)
    sp_spaet = statistics.median(spaet) if spaet else 0.0
    p95 = sorted(spaet)[int(len(spaet) * 0.95)] if spaet else 0.0
    mittel = pool.spawn_summe / pool.spawn_n if pool.spawn_n else 0.0
    check("G2", sp_spaet < 30 and sp_spaet < sp_frueh * 3,
          f"Fork-Latenz: erste {len(pool.spawn_anfang)} Kinder median {sp_frueh:.1f}ms, "
          f"letzte {len(spaet)} median {sp_spaet:.1f}ms (Mittel ueber alle "
          f"{pool.spawn_n} Kinder {mittel:.1f}ms, p95 der letzten {p95:.1f}ms, "
          f"max {pool.spawn_max:.1f}ms)")

    # --- G3: Durchsatz ----------------------------------------------------
    def rate(seg: list[Probe]) -> float:
        dt = seg[-1][0] - seg[0][0]
        return (seg[-1][3] - seg[0][3]) / dt if dt > 0 else 0.0

    f5 = max(1, len(proben) // 5)
    r_frueh, r_spaet = rate(proben[: f5 + 1]), rate(proben[-(f5 + 1):])
    quote = r_spaet / r_frueh if r_frueh > 0 else 0.0
    check("G3", quote >= 0.85,
          f"Durchsatz erstes Fuenftel {r_frueh:.1f}/s, letztes {r_spaet:.1f}/s "
          f"({quote * 100:.0f}%)")

    # --- G4: Loop-Prozess unter Druck -------------------------------------
    rss_a, rss_e = proben[0][1], proben[-1][1]
    steigung = steigung_mb_pro_min(proben, 1)
    fd_a, fd_e = proben[0][2], proben[-1][2]
    # Die Buchfuehrung des Tests wird mit ausgewiesen: nur so ist ein
    # Restanstieg kuenftig belegt statt erklaert (Lehre aus F2).
    check("G4", steigung < 0.2 and (fd_e - fd_a) <= 8 and jitter.max < 100,
          f"Loop-RSS {rss_a:.1f} -> {rss_e:.1f}MB (Regression {steigung:+.3f} MB/min "
          f"= {steigung * 1440:+.0f} MB/Tag), FDs {fd_a} -> {fd_e}, Jitter max "
          f"{jitter.max:.1f}ms / median {jitter.median:.2f}ms ueber {len(jitter)} Proben "
          f"[Buchfuehrung: {len(pool._start_ts)} offen, {len(pool.results)} Ergebnisse, "
          f"{len(pool.spawn_anfang) + len(pool.spawn_ende)} Spawn-Zeiten, "
          f"{len(proben)} Messproben]")




# ---------------------------------------------------------------------------
# H-Serie: Wiederverwendung, Recycling, Verbindungen
# ---------------------------------------------------------------------------


async def _zaehle_kindverbindungen() -> int:
    """Verbindungen der Rechenkinder am echten Valkey.

    Gezaehlt wird am Server, nicht im Client: nur so faellt eine
    Verbindung auf, die ein gestorbenes Kind hinterlassen hat.
    """
    import valkey.asyncio

    from prototypes.cpu_lifecycle_tasks import CLIENT_NAME

    admin = valkey.asyncio.Valkey.from_url(
        BACKEND_URL, client_name="swarmq-proto-admin", decode_responses=True
    )
    try:
        raw = await admin.client_list()
        return sum(1 for c in raw if c.get("name") == CLIENT_NAME)
    finally:
        await admin.aclose()


async def series_h(fs: Any) -> None:
    print("\nH  Wiederverwendung, Recycling und Verbindungen", flush=True)
    await waerme_forkserver(fs)

    try:
        vorher = await _zaehle_kindverbindungen()
    except Exception as exc:
        check("H4", False, f"Valkey nicht erreichbar ({type(exc).__name__}: {exc})")
        vorher = -1

    await _h1_scope_trennung(fs)
    await _h2_amortisation(fs)
    if vorher >= 0:
        await _h4_verbindungen(fs, vorher)
    await _h3_recycling_nach_rss(fs)
    await _h5_rollierend(fs)
    await _h6_slot_queue(fs)


async def _h1_scope_trennung(fs: Any) -> None:
    # --- H1: Scope-Trennung im wiederverwendeten Kind ----------------------
    # Der Kern der Regel: ein Kind, das mehrere Tasks sieht, darf den
    # REQUEST-Scope nicht ueber die Tasks hinweg verschmieren.
    m = 8
    p1 = SlotPool(fs, 1, mode="reuse")
    await p1.run_all([("proto.di_pool", (250,), f"h1-{i}") for i in range(m)])
    v1 = p1.ok_values()
    p1.close()
    pids = {v["pid"] for v in v1}
    app_ids = {v["app_ident"] for v in v1}
    req_ids = {v["req_ident"] for v in v1}
    check("H1", len(v1) == m and all(v["correct"] for v in v1)
          and len(pids) == 1 and len(app_ids) == 1 and len(req_ids) == m,
          f"{len(v1)}/{m} Tasks in {len(pids)} Prozess: {len(app_ids)} APP-Identitaet "
          f"(wiederverwendet), {len(req_ids)}/{m} REQUEST-Identitaeten (je Task frisch)")


async def _h2_amortisation(fs: Any) -> None:
    # --- H2: Amortisation --------------------------------------------------
    n_amort = 40
    jobs_a = [("proto.di_pool", (250,), f"h2f-{i}") for i in range(n_amort)]
    pf = SlotPool(fs, SLOTS, mode="fresh")
    t0 = time.perf_counter()
    await pf.run_all(jobs_a)
    wall_fresh = time.perf_counter() - t0
    vf = pf.ok_values()
    pf.close()

    jobs_b = [("proto.di_pool", (250,), f"h2r-{i}") for i in range(n_amort)]
    pr = SlotPool(fs, SLOTS, mode="reuse")
    t0 = time.perf_counter()
    await pr.run_all(jobs_b)
    wall_reuse = time.perf_counter() - t0
    vr = pr.ok_values()
    pr.close()

    faktor = wall_fresh / wall_reuse if wall_reuse > 0 else 0.0
    aufbau_fresh = statistics.median([v["app_build_ms"] for v in vf])
    aufbauten = [v["app_build_ms"] for v in vr]
    check("H2", len(vf) == n_amort and len(vr) == n_amort and faktor > 1.0,
          f"{n_amort} DI-Tasks: frisches Kind {wall_fresh:.2f}s, wiederverwendet "
          f"{wall_reuse:.2f}s -> Faktor {faktor:.1f}x; DI-Aufbau {aufbau_fresh:.0f}ms "
          f"faellt {len(vf)}x statt {len(set(v['app_ident'] for v in vr))}x an "
          f"(median {statistics.median(aufbauten):.0f}ms)")


async def _h4_verbindungen(fs: Any, vorher: int) -> None:
    # --- H4: Verbindungen bei N gleichzeitigen Kindern ---------------------
    pk = SlotPool(fs, SLOTS, mode="reuse")
    lauf = asyncio.create_task(
        pk.run_all([("proto.di_pool", (600,), f"h4-{i}") for i in range(SLOTS * 3)])
    )
    await asyncio.sleep(0.9)
    waehrend = await _zaehle_kindverbindungen()
    await lauf
    pk.close()
    await asyncio.sleep(0.5)
    nachher = await _zaehle_kindverbindungen()
    check("H4", waehrend >= SLOTS and nachher <= vorher + 1,
          f"{waehrend} Kindverbindungen bei {SLOTS} Slots waehrend des Laufs "
          f"(vorher {vorher}); nach dem Ende {nachher} — "
          f"{'keine Leiche' if nachher <= vorher + 1 else 'Verbindung ueberlebt das Kind'}")


async def _h3_recycling_nach_rss(fs: Any) -> None:
    # --- H3: Recycling nach RSS -------------------------------------------
    # Der Leak-Task laesst den RSS im selben Kind wachsen; die Schwelle
    # muss greifen und der Slot danach wieder klein anfangen.
    grenze = 250.0
    pl = SlotPool(fs, 1, mode="reuse", rss_limit_mb=grenze, max_tasks=None)
    await pl.run_all([("proto.lc_leak", (60,), f"h3-{i}") for i in range(12)])
    vl = [pl.results[f"h3-{i}"] for i in range(12) if f"h3-{i}" in pl.results]
    rss_verlauf = [e.rss for e in vl]
    pids_l = [e.pid for e in vl]
    recyclings = [r for r in pl.recyclings if r[1] == "rss"]
    pl.close()
    faellt_zurueck = any(
        rss_verlauf[i + 1] < rss_verlauf[i] - 50 for i in range(len(rss_verlauf) - 1)
    )
    check("H3", len(recyclings) >= 1 and len(set(pids_l)) >= 2 and faellt_zurueck,
          f"RSS nach Taskende {[f'{r:.0f}' for r in rss_verlauf]}MB bei Grenze "
          f"{grenze:.0f}MB -> {len(recyclings)} Recycling(s), "
          f"{len(set(pids_l))} verschiedene Kinder")


async def _h5_rollierend(fs: Any) -> None:
    # --- H5: rollierendes Recycling ---------------------------------------
    # Celerys max_tasks_per_child laesst den Slot waehrend des Neustarts
    # brachliegen. Hier startet das Ersatzkind, bevor das alte tot ist.
    pm = SlotPool(fs, SLOTS, mode="reuse", max_tasks=5)
    t0 = time.perf_counter()
    await pm.run_all([("proto.lc_blas", (300, 2), f"h5-{i}") for i in range(60)])
    wall_rec = time.perf_counter() - t0
    n_rec = len(pm.recyclings)
    luecken = pm.recycle_luecken_ms
    ok_rec = len(pm.ok_values())
    pm.close()

    pn = SlotPool(fs, SLOTS, mode="reuse")
    t0 = time.perf_counter()
    await pn.run_all([("proto.lc_blas", (300, 2), f"h5n-{i}") for i in range(60)])
    wall_ohne = time.perf_counter() - t0
    pn.close()

    # Der Prozentaufschlag waere hier irrefuehrend: die Tasks sind mit
    # wenigen Millisekunden kuerzer als ein Fork. Uebertragbar ist der
    # Preis JE RECYCLING — er muss in der Groessenordnung einer Forkzeit
    # liegen, nicht einer Kindlebensdauer. Genau darin unterscheidet sich
    # rollierendes Recycling von Celerys max_tasks_per_child.
    je_recycling_ms = (wall_rec - wall_ohne) / n_rec * 1000 if n_rec else 0.0
    check("H5", ok_rec == 60 and n_rec >= 8 and luecken and max(luecken) < 60
          and je_recycling_ms < 30,
          f"60 Tasks mit Recycling alle 5 Tasks: {n_rec} Recyclings kosten "
          f"{je_recycling_ms:.1f}ms je Stueck ({wall_rec:.2f}s gegen {wall_ohne:.2f}s "
          f"ohne); Slot-Luecke median {statistics.median(luecken):.1f}ms / "
          f"max {max(luecken):.1f}ms — der Slot liegt nie eine Kindlebensdauer brach")


async def _h6_slot_queue(fs: Any) -> None:
    # --- H6: Slot-Queue uebersteht den Kindtod -----------------------------
    # Die Slot-eigene Job-Queue ist eine Pipe. Stirbt das Kind mitten im
    # Task, muss der Slot wieder brauchbar werden — sonst ist ein
    # Kindtod im reuse-Modus ein dauerhaft verlorener Slot.
    pk2 = SlotPool(fs, 2, mode="reuse")
    lauf = asyncio.create_task(
        pk2.run_all([("proto.lc_long", (2.0,), f"h6-{i}") for i in range(6)])
    )
    await asyncio.sleep(0.6)
    getoetet = [t for t in list(pk2.laufend) if pk2.kill(t)]
    await lauf
    ok6 = len(pk2.ok_values())
    pk2.close()
    check("H6", getoetet and ok6 == 6 - len(getoetet),
          f"{len(getoetet)} Kind(er) mitten im Task getoetet; danach "
          f"{ok6}/{6 - len(getoetet)} verbleibende Tasks auf denselben Slots korrekt")


# ---------------------------------------------------------------------------
# I-Serie: Lebenszyklus am echten Broker
# ---------------------------------------------------------------------------


async def _frischer_broker(queue: str) -> Any:
    from swarmq.broker.rabbitmq import RabbitMQBroker

    broker = RabbitMQBroker(BROKER_URL)
    await broker.connect()
    await broker.declare_queue(queue)
    return broker


def _rabbit_eval(ausdruck: str) -> str:
    """Setzt eine Broker-Einstellung zur Laufzeit (nur fuer den Test)."""
    proc = subprocess.run(
        ["docker", "exec", RABBIT_CONTAINER, "rabbitmqctl", "eval", ausdruck],
        capture_output=True, text=True, timeout=60,
    )
    return (proc.stdout + proc.stderr).strip()


async def series_i(fs: Any) -> None:
    print("\nI  Lebenszyklus am echten Broker", flush=True)
    await waerme_forkserver(fs)

    try:
        broker = await _frischer_broker(QUEUE)
    except Exception as exc:
        for eid in ("I1", "I2", "I3", "I4", "I5"):
            check(eid, False, f"Broker nicht erreichbar ({type(exc).__name__}: {exc})")
        return

    try:
        await _i1_graceful(fs, broker)
        await _i2_redelivery(fs)
        await _i3_consumer_timeout(fs)
        await _i4_prefetch(fs)
        await _i5_dlq(fs)
    finally:
        await broker.close()


async def _i1_graceful(fs: Any, broker: Any) -> None:
    """Shutdown-Signal, waehrend N Kinder rechnen: laufende Tasks zu Ende,
    keine neuen mehr, dann erst der Ack."""
    q = f"{QUEUE}-i1"
    b = await _frischer_broker(q)
    try:
        # Bewusst lange Tasks: bei Millisekunden-Tasks waeren beim Signal
        # alle laengst fertig und der Drain maesse nichts.
        anzahl = SLOTS * 2
        for i in range(anzahl):
            await b.publish(q, json.dumps({"task": "proto.lc_long", "args": [2.5],
                                           "id": f"i1-{i}"}).encode())

        pool = SlotPool(fs, SLOTS, mode="fresh")
        shutdown = asyncio.Event()
        angenommen: list[Any] = []
        gestartet: set[str] = set()
        geackt: list[str] = []

        async def on_delivery(delivery: Any) -> None:
            if shutdown.is_set():
                # Nach dem Signal wird nichts Neues mehr angefasst.
                await b.redeliver(delivery)
                return
            angenommen.append(delivery)

        await b.consume(q, on_delivery, concurrency=SLOTS)

        async def fahre() -> None:
            while len(gestartet) < anzahl and not shutdown.is_set():
                while angenommen and pool.frei:
                    d = angenommen.pop(0)
                    msg = json.loads(d.body)
                    gestartet.add(msg["id"])
                    pool._vergib((msg["task"], tuple(msg["args"]), msg["id"]))
                    geackt.append(msg["id"])
                    await b.ack(d)
                pool._ernte_tote_fresh()
                pool._ernte()
                await asyncio.sleep(0.005)

        fahrt = asyncio.create_task(fahre())
        await asyncio.sleep(0.8)
        laufend_bei_signal = len(pool.laufend)
        shutdown.set()
        await fahrt

        # Drain: die laufenden Kinder ausrechnen lassen.
        t0 = time.perf_counter()
        while pool.laufend and time.perf_counter() - t0 < 30:
            pool._ernte_tote_fresh()
            pool._ernte()
            await asyncio.sleep(0.005)
        drain_s = time.perf_counter() - t0
        await b.cancel_consume()
        fertig = len(pool.ok_values())
        korrekt = all(v["correct"] for v in pool.ok_values())
        pool.close()

        # Der Drain muss wirklich gedauert haben — sonst waren die Tasks
        # beim Signal schon fertig und der Punkt belegt nichts.
        check("I1", laufend_bei_signal == SLOTS and fertig == len(gestartet)
              and korrekt and drain_s > 0.5,
              f"Signal bei {laufend_bei_signal} rechnenden Kindern (von {SLOTS} Slots): "
              f"{fertig}/{len(gestartet)} angenommene Tasks in {drain_s:.2f}s Drain zu "
              f"Ende gerechnet, {anzahl - len(gestartet)} nicht angenommene zurueck an "
              f"den Broker")
    finally:
        await b.close()


async def _i2_redelivery(fs: Any) -> None:
    """Stirbt das Kind vor dem Ack, muss die Nachricht wiederkommen.

    Das ist die Zusage „at-least-once ueberlebt die Prozessgrenze": der
    Loop-Prozess darf erst ackken, wenn das Kind ein Ergebnis geliefert
    hat — nicht schon beim Zustellen.
    """
    q = f"{QUEUE}-i2"
    b = await _frischer_broker(q)
    try:
        await b.publish(q, json.dumps({"task": "proto.lc_long", "args": [3.0],
                                       "id": "i2-1"}).encode())
        pool = SlotPool(fs, 1, mode="fresh")
        zustellungen: list[Any] = []
        versuche = 0
        erfolg = False

        async def on_delivery(delivery: Any) -> None:
            nonlocal versuche, erfolg
            versuche += 1
            zustellungen.append(delivery)
            msg = json.loads(delivery.body)
            # Kurzer Task beim zweiten Versuch, damit der Test endet.
            args = tuple(msg["args"]) if versuche == 1 else (0.3,)
            tid = f"{msg['id']}-v{versuche}"
            pool._vergib((msg["task"], args, tid))

            if versuche == 1:
                await asyncio.sleep(0.4)
                pool.kill(tid)         # Kindtod mitten in der Rechnung
                await b.redeliver(delivery)  # kein Ack — die Nachricht kommt wieder
                return

            t0 = time.perf_counter()
            while tid not in pool.results and time.perf_counter() - t0 < 20:
                pool._ernte()
                await asyncio.sleep(0.005)
            erfolg = tid in pool.results and pool.results[tid].payload.get("correct")
            await b.ack(delivery)

        await b.consume(q, on_delivery, concurrency=1)
        t0 = time.perf_counter()
        while versuche < 2 and time.perf_counter() - t0 < 40:
            pool._ernte_tote_fresh()
            pool._ernte()
            await asyncio.sleep(0.01)
        while not erfolg and time.perf_counter() - t0 < 60:
            await asyncio.sleep(0.01)
        await b.cancel_consume()
        redelivered = bool(getattr(zustellungen[-1].handle, "redelivered", False)) \
            if zustellungen else False
        pool.close()

        check("I2", versuche == 2 and erfolg,
              f"Kind vor dem Ack getoetet -> {versuche} Zustellungen "
              f"(redelivered-Flag: {redelivered}), zweiter Versuch korrekt "
              f"— kein Verlust")
    finally:
        await b.close()


async def _i3_consumer_timeout(fs: Any) -> None:
    """Wie lange darf ein Rechenkind rechnen, bevor der Broker die
    Nachricht zurueckzieht?

    RabbitMQ zieht eine Zustellung zurueck, die laenger als
    ``consumer_timeout`` unbestaetigt bleibt — gemessen von der
    Zustellung bis zum Ack, unabhaengig davon, wie frei der
    Loop-Prozess ist. Fuer CPU-Tasks von Minuten ist das eine harte
    Obergrenze, die niemand aus dem Entwurf ablesen kann.

    Der Provokationsversuch (Timeout auf 8s senken, 20s rechnen) hat am
    2026-08-31 an RabbitMQ 4.3.5 **nicht** gegriffen — weder ueber
    ``application:set_env`` noch ueber ``conf.d``, weder mit Classic-
    noch mit Quorum-Queue. Der Test haelt deshalb fest, was er wirklich
    belegen kann: den geltenden Vorgabewert und einen Langlaeufer, der
    sauber durchgeht. Ob der Timeout bei laengerer Ueberschreitung
    greift, bleibt offen und gehoert in die Lasttest-Suite.
    """
    vorgabe = _rabbit_eval("application:get_env(rabbit, consumer_timeout).")

    q = f"{QUEUE}-i3"
    b = await _frischer_broker(q)
    ack_fehler = ""
    zustellungen = 0
    sekunden = float(os.environ.get("PROTO_I3_SEKUNDEN", "20"))
    try:
        await b.publish(q, json.dumps({"task": "proto.lc_long", "args": [sekunden],
                                       "id": "i3-1"}).encode())
        pool = SlotPool(fs, 1, mode="fresh")
        fertig = asyncio.Event()
        jitter: list[float] = []

        async def on_delivery(delivery: Any) -> None:
            nonlocal ack_fehler, zustellungen
            zustellungen += 1
            if zustellungen > 1:
                fertig.set()
                return
            msg = json.loads(delivery.body)
            pool._vergib((msg["task"], tuple(msg["args"]), "i3-1"))
            # Der Loop-Prozess bleibt waehrenddessen frei — das ist die
            # Zusage des Entwurfs und wird hier mitgemessen.
            t0 = letzte = time.perf_counter()
            while "i3-1" not in pool.results and time.perf_counter() - t0 < sekunden * 4:
                pool._ernte()
                await asyncio.sleep(0.02)
                jetzt = time.perf_counter()
                jitter.append((jetzt - letzte - 0.02) * 1000)
                letzte = jetzt
            try:
                await b.ack(delivery)
            except Exception as exc:
                ack_fehler = f"{type(exc).__name__}: {exc}"
            fertig.set()

        await b.consume(q, on_delivery, concurrency=1)
        try:
            await asyncio.wait_for(fertig.wait(), timeout=sekunden * 5)
        except TimeoutError:
            ack_fehler = ack_fehler or f"kein Abschluss in {sekunden * 5:.0f}s"
        erg = pool.results.get("i3-1")
        gerechnet = erg is not None and erg.payload.get("correct")
        cpu_s = erg.payload.get("cpu_s", 0.0) if erg is not None else 0.0
        await b.cancel_consume()
        pool.close()

        check("I3", bool(gerechnet) and not ack_fehler and zustellungen == 1,
              f"{cpu_s:.0f}s reine Rechenzeit bei consumer_timeout={vorgabe}: "
              f"{zustellungen} Zustellung, Ack "
              + (f"abgelehnt — {ack_fehler}" if ack_fehler else "angenommen")
              + "; Loop-Jitter waehrenddessen max "
              + (f"{max(jitter):.1f}ms" if jitter else "n/a"))
        note("Der Vorgabewert 1800000 ms = 30 Minuten ist damit die "
             "Obergrenze fuer eine einzelne Taskausfuehrung im CPU-Modus. Ein "
             "Versuch, sie durch Absenken auf 8s zu provozieren, griff an "
             "RabbitMQ 4.3.5 nicht (set_env, conf.d, Classic und Quorum je "
             "geprueft) — die Grenze ist damit dokumentiert, aber nicht "
             "nachgemessen. Restrisiko fuer die Lasttest-Suite.")
    finally:
        await b.close()


async def _i4_prefetch(fs: Any) -> None:
    """Der Loop-Prozess darf nicht mehr Nachrichten unbestaetigt halten,
    als er Rechenslots hat — sonst liegen Nachrichten bei ihm fest,
    waehrend ein anderer Worker frei ist."""
    q = f"{QUEUE}-i4"
    b = await _frischer_broker(q)
    try:
        gesamt = SLOTS * 5
        for i in range(gesamt):
            await b.publish(q, json.dumps({"task": "proto.lc_blas", "args": [400, 3],
                                           "id": f"i4-{i}"}).encode())

        pool = SlotPool(fs, SLOTS, mode="fresh")
        offen = 0
        max_offen = 0
        fertig = 0

        async def on_delivery(delivery: Any) -> None:
            nonlocal offen, max_offen, fertig
            offen += 1
            max_offen = max(max_offen, offen)
            msg = json.loads(delivery.body)
            tid = msg["id"]
            while not pool.frei:
                await asyncio.sleep(0.003)
            pool._vergib((msg["task"], tuple(msg["args"]), tid))
            t0 = time.perf_counter()
            while tid not in pool.results and time.perf_counter() - t0 < 30:
                await asyncio.sleep(0.003)
            await b.ack(delivery)
            offen -= 1
            fertig += 1

        await b.consume(q, on_delivery, concurrency=SLOTS)
        ernter = asyncio.create_task(_ernte_dauernd(pool))
        t0 = time.perf_counter()
        while fertig < gesamt and time.perf_counter() - t0 < 90:
            await asyncio.sleep(0.01)
        ernter.cancel()
        await b.cancel_consume()
        korrekt = sum(1 for v in pool.ok_values() if v["correct"])
        pool.close()

        check("I4", fertig == gesamt and korrekt == gesamt and max_offen <= SLOTS,
              f"{fertig}/{gesamt} Tasks, hoechstens {max_offen} Nachrichten "
              f"gleichzeitig unbestaetigt bei {SLOTS} Slots (prefetch={SLOTS})")
    finally:
        await b.close()


async def _ernte_dauernd(pool: SlotPool) -> None:
    while True:
        pool._ernte_tote_fresh()
        pool._ernte()
        await asyncio.sleep(0.003)


async def _i5_dlq(fs: Any) -> None:
    """Eine Exception im Rechenkind muss den Retry- und DLQ-Pfad
    unveraendert erreichen — der Offload darf ihn nicht brechen."""
    q = f"{QUEUE}-i5"
    b = await _frischer_broker(q)
    try:
        from swarmq.broker.rabbitmq import RabbitMQBroker

        admin = RabbitMQBroker(BROKER_URL)
        await admin.connect()
        await admin.declare_queue(q)
        try:
            vorher_dlq = await admin.dead_letter_depth(q)
        except Exception:
            vorher_dlq = -1

        await b.publish(q, json.dumps({"task": "proto.lc_fail", "args": ["im Kind"],
                                       "id": "i5-1"}).encode())
        pool = SlotPool(fs, 1, mode="fresh")
        max_retries = 2
        versuche = 0
        fehlertext = ""
        fertig = asyncio.Event()

        async def on_delivery(delivery: Any) -> None:
            nonlocal versuche, fehlertext
            versuche += 1
            msg = json.loads(delivery.body)
            tid = f"{msg['id']}-v{versuche}"
            pool._vergib((msg["task"], tuple(msg["args"]), tid))
            t0 = time.perf_counter()
            while tid not in pool.results and time.perf_counter() - t0 < 20:
                pool._ernte_tote_fresh()
                pool._ernte()
                await asyncio.sleep(0.005)
            erg = pool.results.get(tid)
            if erg is not None and erg.status == "error":
                fehlertext = str(erg.payload)
            if versuche <= max_retries:
                await b.retry_later(delivery, delay_ms=200,
                                    headers={"x-swarmq-retry-count": versuche})
            else:
                await b.dead_letter(delivery, headers={"x-swarmq-error": fehlertext[:200]})
                fertig.set()

        await b.consume(q, on_delivery, concurrency=1)
        try:
            await asyncio.wait_for(fertig.wait(), timeout=60)
        except TimeoutError:
            pass
        await b.cancel_consume()
        await asyncio.sleep(0.5)
        try:
            nachher_dlq = await admin.dead_letter_depth(q)
        except Exception:
            nachher_dlq = -1
        await admin.close()
        pool.close()

        typ_erhalten = fehlertext.startswith("ValueError:")
        check("I5", versuche == max_retries + 1 and typ_erhalten
              and (nachher_dlq < 0 or nachher_dlq > vorher_dlq),
              f"Exception im Kind: {versuche} Versuche (max_retries={max_retries}), "
              f"typerhaltend uebertragen ({fehlertext[:60]!r}), "
              f"DLQ-Tiefe {vorher_dlq} -> {nachher_dlq}")
    finally:
        await b.close()


# ---------------------------------------------------------------------------


async def main() -> int:
    print(f"Python {sys.version.split()[0]}  |  Kerne {os.cpu_count()}  |  "
          f"Slots {SLOTS}  |  OMP_NUM_THREADS={os.environ.get('OMP_NUM_THREADS')}  |  "
          f"Serien {SERIES}\n", flush=True)

    fs = mp.get_context("forkserver")
    fs.set_forkserver_preload(PRELOAD)

    if SERIES in ("all", "g"):
        await series_g(fs, MINUTES)
    if SERIES in ("all", "h"):
        await series_h(fs)
    if SERIES in ("all", "i"):
        await series_i(fs)

    failed = [e for e, ok, _ in _RESULTS if not ok]
    print("\n" + "=" * 72, flush=True)
    print(f"{len(_RESULTS) - len(failed)}/{len(_RESULTS)} Punkte bestaetigt"
          + (f" — offen: {', '.join(failed)}" if failed else ""), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
