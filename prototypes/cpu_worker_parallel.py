"""Parallel- und Dauerlauftest fuer den CPU-Worker-Modus — die beiden
Luecken, die ``cpu_worker_integration.py`` ausdruecklich offengelassen
hat: es lief immer nur EIN Rechenkind, und 200 Tasks in 3,6 Sekunden
sind kein Dauerbetrieb.

    docker run -d --name swarmq-proto-rabbit -p 5699:5672 \
        -e RABBITMQ_DEFAULT_USER=guest -e RABBITMQ_DEFAULT_PASS=guest rabbitmq:4
    docker run -d --name swarmq-proto-valkey -p 6399:6379 valkey/valkey:8

    SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5699/" \
    SWARMQ_BACKEND_URL="redis://localhost:6399/0" PROTO_MINUTES=5 \
    uv run --extra dishka --with numpy --with pillow \
        python -u prototypes/cpu_worker_parallel.py

Eigene Ports, damit kein laufender Stack beruehrt wird.

Geprueft wird:

  E-Serie — Parallelbetrieb (N Rechenkinder gleichzeitig):
    E1  N Slots rechnen wirklich parallel (Speedup gegen einen Slot)
    E2  Der Loop-Prozess bleibt unter Vollast bedienbar
    E3  Jedes Kind rechnet korrekt und ist ein eigener Prozess
    E4  N Kinder unter echtem Speicherdruck gleichzeitig resident
    E5  Ein sterbender Slot laesst die Geschwister unberuehrt
    E6  Kein Slot liegt bei ungleichen Laufzeiten brach
    E7  Ohne Thread-Begrenzung ueberbucht OpenBLAS die Kerne

  F-Serie — Dauerlauf (die Drift, die erst mit der Zeit auftritt):
    F1  Alle Tasks des Dauerlaufs liefern ein Ergebnis
    F2  Der RSS des Loop-Prozesses driftet nicht
    F3  Die Dateideskriptoren des Loop-Prozesses driften nicht
    F4  Der Durchsatz bricht ueber die Laufzeit nicht ein
    F5  Kein Zombie bleibt zurueck
    F6  Regelmaessiger Kindtod im Betrieb wird durchgehend verkraftet
    F7  Broker- und Backend-Verbindung ueberleben den Lauf im Leerlauf
"""

from __future__ import annotations

import os

# MUSS vor jedem numpy-Import und vor dem Start des Forkservers stehen:
# der Forkserver erbt diese Umgebung, und OpenBLAS liest sie beim Laden
# der Bibliothek. PROTO_PIN=0 schaltet die Begrenzung ab — so misst E7
# den Gegenversuch.
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
import statistics
import subprocess
import sys
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT))

BROKER_URL = os.environ.get("SWARMQ_BROKER_URL", "amqp://guest:guest@localhost:5699/")
BACKEND_URL = os.environ.get("SWARMQ_BACKEND_URL", "redis://localhost:6399/0")
QUEUE = "swarmq-cpu-proto-par"

PRELOAD = ["prototypes.cpu_parallel_tasks"]
SLOTS = int(os.environ.get("PROTO_SLOTS", "4"))
MINUTES = float(os.environ.get("PROTO_MINUTES", "5"))

# Rund 1,5s echte BLAS-Arbeit je Task. Bewusst so schwer gewaehlt: bei
# 60ms war der Task frueher fertig, als der Test ihn toeten konnte, und
# der Heartbeat kam nur ein Dutzend Mal dran. Beides misst dann nichts.
SCHWER = (1200, 20)

_RESULTS: list[tuple[str, bool, str]] = []


def check(eid: str, ok: bool, detail: str) -> None:
    _RESULTS.append((eid, ok, detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {eid}  {detail}", flush=True)


# ---------------------------------------------------------------------------
# Messgroessen des Loop-Prozesses
# ---------------------------------------------------------------------------


def rss_mb() -> float:
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0


def fd_count() -> int:
    return len(os.listdir("/proc/self/fd"))


def own_zombies() -> list[int]:
    """Zombie-Kinder dieses Prozesses.

    Ein nicht eingesammeltes Kind belegt einen Prozesstabelleneintrag —
    im Dauerbetrieb mit einem Kind je Task waere das der erste Weg, den
    Host zu erschoepfen.
    """
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
        except OSError, IndexError, ValueError:
            continue
    return out


# ---------------------------------------------------------------------------
# Kind-Entrypoint
# ---------------------------------------------------------------------------


class _QueueBackend:
    """Rueckkanal: die zwei Methoden, die Task.update_progress braucht."""

    def __init__(self, queue: Any) -> None:
        self._queue = queue

    async def set_progress(self, task_id: str, fields: dict[str, str], ttl_ms: int) -> None:
        self._queue.put(("progress", task_id, dict(fields), ttl_ms))

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        return None


def child_task(
    task_name: str,
    args: tuple[Any, ...],
    task_id: str,
    progress_q: Any,
    result_q: Any,
) -> None:
    """Ein Task, vollstaendig im Rechenkind — mit DI-REQUEST-Scope."""
    from prototypes import cpu_parallel_tasks  # noqa: F401  (Preload-Modul)
    from swarmq.dishka import _MISSING, TaskContext, get_registered_container, request_container
    from swarmq.task import Task

    cls = next(c for c in Task._registered_tasks if c.task_info.name == task_name)

    async def _run() -> None:
        inst = cls()
        inst._task_id = task_id
        inst._backend = _QueueBackend(progress_q)
        inst._task_start_time = time.monotonic()
        inst._result_ttl_ms = 86_400_000

        container = get_registered_container()
        scope = token = None
        if container is not _MISSING:
            scope = container(
                context={TaskContext: TaskContext(task_id=task_id, task_name=task_name, queue=QUEUE, retry_count=0)}
            )
            token = request_container.set(await scope.__aenter__())
        try:
            result_q.put(("ok", task_id, await inst.run(*args)))
        except Exception as exc:
            result_q.put(("error", task_id, f"{type(exc).__name__}: {exc}"))
        finally:
            if token is not None:
                request_container.reset(token)
            if scope is not None:
                await scope.__aexit__(None, None, None)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Der Pool: ein Loop-Prozess, der nie rechnet, und N Rechenslots
# ---------------------------------------------------------------------------


class ChildPool:
    """Haelt bis zu ``slots`` Rechenkinder am Leben.

    Bewusst mit GETEILTEN Queues statt einem Queue-Paar je Task: ein
    Queue-Paar pro Task waere im Dauerlauf ein Dateideskriptor-Paar pro
    Task — genau die Drift, die F3 messen soll. Der Entwurf braucht die
    geteilte Variante ohnehin, weil der Loop-Prozess einen einzigen
    Rueckkanal bedient.
    """

    def __init__(self, fs: Any, slots: int, *, keep_results: bool = True) -> None:
        self._fs = fs
        self.slots = slots
        # Im Dauerlauf werden Ergebnisse nur gezaehlt, nicht gesammelt: ein
        # echter Worker ackt die Nachricht und vergisst sie. Sammelte der
        # Test sie, maesse F2 seine eigene Buchfuehrung statt den Loop.
        self.keep_results = keep_results
        self.pq = fs.Queue()
        self.rq = fs.Queue()
        self.procs: dict[str, Any] = {}
        self.results: dict[str, tuple[Any, ...]] = {}
        self.ok_count = 0
        self.err_count = 0
        self.wrong_count = 0
        self.errors: list[str] = []
        # Bewusst gedeckelt: bei zehntausenden Kindern waere die Menge
        # selbst ein Leck im Loop-Prozess — und F2 misst genau dort.
        self.pids: set[int] = set()
        self.pids_bis = 5000
        self.killed: set[str] = set()
        self.completed = 0
        self.busy_s = 0.0
        self._started: dict[str, float] = {}
        self._kill_at: dict[str, float] = {}

    def _spawn(self, job: tuple[str, tuple[Any, ...], str]) -> None:
        name, args, tid = job
        p = self._fs.Process(target=child_task, args=(name, args, tid, self.pq, self.rq))
        p.start()
        self.procs[tid] = p
        self._started[tid] = time.perf_counter()

    def _drain(self) -> None:
        while True:
            try:
                item = self.rq.get_nowait()
            except Exception:
                break
            if item[0] == "ok":
                self.ok_count += 1
                if not item[2].get("correct"):
                    self.wrong_count += 1
            else:
                self.err_count += 1
                if len(self.errors) < 5:
                    self.errors.append(f"{item[1]}: {item[2]}")
            if self.keep_results:
                self.results[item[1]] = item
        # Fortschrittsmeldungen leeren: der Rueckkanal darf nicht volllaufen,
        # nur ihr Inhalt ist hier nicht Gegenstand der Pruefung.
        while True:
            try:
                self.pq.get_nowait()
            except Exception:
                break

    def kill(self, task_id: str) -> bool:
        p = self.procs.get(task_id)
        if p is not None and p.is_alive():
            p.kill()
            self.killed.add(task_id)
            return True
        return False

    async def run_all(
        self,
        jobs: Iterable[tuple[str, tuple[Any, ...], str]],
        *,
        kill_every: int | None = None,
        kill_after_s: float = 0.02,
        deadline: float | None = None,
    ) -> None:
        """Faehrt die Jobs durch die Slots. Rechnet selbst nie.

        ``jobs`` darf unendlich sein; dann begrenzt ``deadline`` den Lauf.
        Nach Ablauf werden keine neuen Jobs mehr gestartet, die laufenden
        aber zu Ende gefahren — sonst zaehlte der Abbruch als Verlust.
        """
        it = iter(jobs)
        erschoepft = False
        eingereicht = 0
        while True:
            if deadline is not None and time.perf_counter() > deadline:
                erschoepft = True
            while not erschoepft and len(self.procs) < self.slots:
                try:
                    job = next(it)
                except StopIteration:
                    erschoepft = True
                    break
                self._spawn(job)
                eingereicht += 1
                if kill_every and eingereicht % kill_every == 0:
                    self._kill_at[job[2]] = time.perf_counter() + kill_after_s

            if erschoepft and not self.procs:
                break

            now = time.perf_counter()
            for tid, dl in list(self._kill_at.items()):
                if now >= dl:
                    self.kill(tid)
                    del self._kill_at[tid]

            for tid, p in list(self.procs.items()):
                if not p.is_alive():
                    p.join()
                    self.busy_s += time.perf_counter() - self._started.pop(tid)
                    if p.pid and self.completed < self.pids_bis:
                        self.pids.add(p.pid)
                    del self.procs[tid]
                    self.completed += 1
            self._drain()
            await asyncio.sleep(0.002)
        self._drain()

    def ok_values(self) -> list[dict[str, Any]]:
        return [r[2] for r in self.results.values() if r[0] == "ok"]

    def close(self) -> None:
        for p in self.procs.values():
            if p.is_alive():
                p.kill()
            p.join()
        self.procs.clear()


async def heartbeat(stop: asyncio.Event, interval: float = 0.01) -> list[float]:
    """Misst, wie zuverlaessig der Loop-Prozess drankommt.

    Der Jitter ist die Zusage des Entwurfs: waehrend die Kinder rechnen,
    muss der Beobachter Broker, Health und Metriken bedienen koennen.
    """
    gaps: list[float] = []
    last = time.perf_counter()
    while not stop.is_set():
        await asyncio.sleep(interval)
        now = time.perf_counter()
        gaps.append((now - last - interval) * 1000)
        last = now
    return gaps


# ---------------------------------------------------------------------------
# E-Serie: Parallelbetrieb
# ---------------------------------------------------------------------------


async def series_e(fs: Any) -> None:
    print(f"\nE  Parallelbetrieb mit {SLOTS} Rechenslots", flush=True)

    # Forkserver vorwaermen: sein einmaliger Hochlauf (rund 150ms) wuerde
    # sonst dem sequenziellen Lauf angelastet und den Speedup schoenen.
    warm = ChildPool(fs, 1)
    await warm.run_all([("proto.par_blas", (200, 1), "warmup")])

    # --- E1/E2/E3: Speedup, Bedienbarkeit, Korrektheit in einem Lauf -------
    jobs_seq = [("proto.par_blas", SCHWER, f"e1-seq-{i}") for i in range(SLOTS)]
    seq = ChildPool(fs, 1)
    t0 = time.perf_counter()
    await seq.run_all(jobs_seq)
    seq_wall = time.perf_counter() - t0

    jobs_par = [("proto.par_blas", SCHWER, f"e1-par-{i}") for i in range(SLOTS)]
    par = ChildPool(fs, SLOTS)
    stop = asyncio.Event()
    hb = asyncio.create_task(heartbeat(stop))
    t0 = time.perf_counter()
    await par.run_all(jobs_par)
    par_wall = time.perf_counter() - t0
    stop.set()
    gaps = await hb

    speedup = seq_wall / par_wall if par_wall > 0 else 0.0
    check(
        "E1",
        speedup >= SLOTS * 0.6,
        f"{SLOTS} Tasks: sequenziell {seq_wall:.2f}s, parallel {par_wall:.2f}s "
        f"-> Speedup {speedup:.1f}x (von {SLOTS}x moeglich)",
    )

    check(
        "E2",
        len(gaps) > 20 and max(gaps) < 100,
        f"Loop-Prozess waehrend der Rechnung {len(gaps)}x drangekommen, "
        f"Jitter max {max(gaps):.1f}ms / median {statistics.median(gaps):.1f}ms",
    )

    vals = par.ok_values()
    pids = {v["pid"] for v in vals}
    check(
        "E3",
        len(vals) == SLOTS and all(v["correct"] for v in vals) and len(pids) == SLOTS,
        f"{len(vals)}/{SLOTS} korrekt in {len(pids)} verschiedenen Prozessen, "
        f"je {statistics.median([v['cpu_s'] for v in vals]) * 1000:.0f}ms BLAS "
        f"bei {vals[0]['threads'] if vals else '?'} Thread(s)",
    )

    # --- E4: echter Speicherdruck auf allen Slots gleichzeitig -------------
    rss_before = rss_mb()
    mem_jobs = [("proto.mem_pressure", (400, 1.5), f"e4-{i}") for i in range(SLOTS)]
    mem = ChildPool(fs, SLOTS)
    stop = asyncio.Event()
    hb = asyncio.create_task(heartbeat(stop))
    await mem.run_all(mem_jobs)
    stop.set()
    mem_gaps = await hb
    rss_after = rss_mb()
    mvals = mem.ok_values()
    ok = len(mvals) == SLOTS and all(v["correct"] for v in mvals) and (rss_after - rss_before) < 40
    check(
        "E4",
        ok,
        f"{len(mvals)}/{SLOTS} Kinder gleichzeitig mit je "
        f"{statistics.median([v['resident_mb'] for v in mvals]):.0f}MB resident "
        f"({SLOTS * 400}MB Druck); Loop-RSS {rss_before:.0f} -> {rss_after:.0f}MB, "
        f"Jitter max {max(mem_gaps):.1f}ms",
    )

    # --- E5: ein Slot stirbt, die Geschwister nicht ------------------------
    kill_jobs = [("proto.par_blas", SCHWER, f"e5-{i}") for i in range(SLOTS)]
    kp = ChildPool(fs, SLOTS)
    task = asyncio.create_task(kp.run_all(kill_jobs))
    await asyncio.sleep(0.4)
    victim = "e5-0"
    killed = kp.kill(victim)
    await task
    survivors = [v for tid, v in ((t, r[2]) for t, r in kp.results.items() if r[0] == "ok") if tid != victim]
    check(
        "E5",
        killed and victim not in kp.results and len(survivors) == SLOTS - 1 and all(v["correct"] for v in survivors),
        f"Slot '{victim}' getoetet (Ergebnis fehlt: {victim not in kp.results}); "
        f"{len(survivors)}/{SLOTS - 1} Geschwister rechneten korrekt zu Ende",
    )

    # --- E6: Slot-Auslastung bei ungleichen Laufzeiten ---------------------
    mixed = [("proto.mixed", (1 + (i * 7) % 12,), f"e6-{i}") for i in range(40)]
    mp_pool = ChildPool(fs, SLOTS)
    t0 = time.perf_counter()
    await mp_pool.run_all(mixed)
    wall = time.perf_counter() - t0
    util = mp_pool.busy_s / (wall * SLOTS) if wall > 0 else 0.0
    done = len(mp_pool.ok_values())
    check(
        "E6",
        done == 40 and util >= 0.80,
        f"40 Tasks ungleicher Laenge in {wall:.2f}s, Slot-Auslastung "
        f"{util * 100:.0f}% (Leerlauf {(1 - util) * 100:.0f}%)",
    )

    _pruefe_threadbegrenzung()


def _pruefe_threadbegrenzung() -> None:
    """E7: Was OpenBLAS ohne Begrenzung mit N Rechenkindern anstellt.

    Jedes Kind macht so viele Threads auf, wie der Host Kerne hat — bei N
    Slots also N-fache Ueberbuchung. Das ist die Auflage, die in den Plan
    gehoert: der CPU-Modus muss die Threadzahl je Kind setzen.
    """
    pinned = _probe(pin=True)
    unpinned = _probe(pin=False)
    if pinned is None or unpinned is None:
        check("E7", False, f"Sondierung fehlgeschlagen: pinned={pinned} unpinned={unpinned}")
        return
    ok = max(pinned["threads"]) == 1 and max(unpinned["threads"]) > 1
    check(
        "E7",
        ok,
        f"mit Begrenzung 1 Thread je Kind ({pinned['wall']:.2f}s), ohne "
        f"{max(unpinned['threads'])} je Kind = {sum(unpinned['threads'])} auf "
        f"{os.cpu_count()} Kernen ({unpinned['wall']:.2f}s)",
    )


def _probe(*, pin: bool) -> dict[str, Any] | None:
    """Startet dieses Skript als eigenen Prozess im Sondierungsmodus.

    Noetig, weil OpenBLAS die Threadzahl beim LADEN der Bibliothek liest —
    im Forkserver, also lange bevor ein Rechenkind existiert. Beide
    Varianten lassen sich deshalb nicht im selben Prozess messen.
    """
    env = dict(os.environ)
    env["PROTO_MODE"] = "probe"
    env["PROTO_PIN"] = "1" if pin else "0"
    if not pin:
        for var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            env.pop(var, None)
    proc = subprocess.run(
        [sys.executable, "-u", str(Path(__file__).resolve())],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )
    for line in proc.stdout.splitlines():
        if line.startswith("PROBE "):
            return json.loads(line[6:])
    print(f"      Sondierung ohne Ergebnis (pin={pin}): {proc.stderr[-300:]}", flush=True)
    return None


async def probe_mode() -> int:
    fs = mp.get_context("forkserver")
    fs.set_forkserver_preload(PRELOAD)
    pool = ChildPool(fs, SLOTS)
    jobs = [("proto.par_blas", SCHWER, f"probe-{i}") for i in range(SLOTS)]
    t0 = time.perf_counter()
    await pool.run_all(jobs)
    wall = time.perf_counter() - t0
    vals = pool.ok_values()
    print(
        "PROBE "
        + json.dumps(
            {
                "wall": wall,
                "threads": [v["threads"] for v in vals],
                "cpu_s": [v["cpu_s"] for v in vals],
            }
        ),
        flush=True,
    )
    return 0


# ---------------------------------------------------------------------------
# F-Serie: Dauerlauf
# ---------------------------------------------------------------------------


Sample = tuple[float, float, int, int]


async def _oeffne_infra() -> tuple[Any, Any]:
    """Broker und Backend vor dem Lauf oeffnen — sie bleiben im Leerlauf
    offen, damit F7 die Verbindungsalterung misst und nicht einen
    frischen Verbindungsaufbau."""
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

    backend = ValkeyBackend(BACKEND_URL)
    await backend.connect()
    broker = RabbitMQBroker(BROKER_URL)
    await broker.connect()
    await broker.declare_queue(QUEUE)
    await backend.set_progress("proto-f7-vorher", {"current": "1", "total": "1"}, ttl_ms=3_600_000)
    return backend, broker


async def _dauerlauf(fs: Any, seconds: float) -> tuple[ChildPool, list[Sample], float]:
    """Faehrt den Pool ueber die Laufzeit und protokolliert dabei die
    Groessen, die erst mit der Zeit driften."""

    def jobs() -> Iterator[tuple[str, tuple[Any, ...], str]]:
        """Unbegrenzt: die Laufzeit begrenzt den Lauf, nicht die Liste.

        Rund 40ms echter BLAS-Arbeit je Task — lang genug, dass der
        Prozesswechsel die Messung nicht dominiert.
        """
        i = 0
        while True:
            yield ("proto.par_blas", (400, 3), f"f-{i}")
            i += 1

    pool = ChildPool(fs, SLOTS, keep_results=False)
    samples: list[Sample] = []
    stop = asyncio.Event()

    async def sampler() -> None:
        t0 = time.perf_counter()
        while not stop.is_set():
            samples.append((time.perf_counter() - t0, rss_mb(), fd_count(), pool.completed))
            for _ in range(50):
                if stop.is_set():
                    break
                await asyncio.sleep(0.2)

    smp = asyncio.create_task(sampler())
    t0 = time.perf_counter()
    # Jeder 200. Task wird 20ms nach dem Start hart getoetet: der
    # Dauerbetrieb muss den Kindtod nicht einmal, sondern immer wieder
    # verkraften.
    await pool.run_all(jobs(), kill_every=200, deadline=t0 + seconds)
    wall = time.perf_counter() - t0
    stop.set()
    await smp
    pool.close()
    samples.append((wall, rss_mb(), fd_count(), pool.completed))
    return pool, samples, wall


def _bewerte_dauerlauf(pool: ChildPool, samples: list[Sample], wall: float) -> None:
    erwartet = pool.completed - len(pool.killed)
    check(
        "F1",
        pool.ok_count == erwartet and pool.wrong_count == 0 and pool.err_count == 0,
        f"{pool.ok_count}/{erwartet} Tasks mit korrektem Ergebnis in {wall:.0f}s "
        f"({len(pool.killed)} absichtlich getoetet, {len(pool.pids)} verschiedene PIDs"
        + (f", Fehler: {pool.errors}" if pool.errors else "")
        + ")",
    )

    # Die Steigung zaehlt, nicht nur der Endwert: ein Leck von 5 MB/min
    # bleibt in 5 Minuten unter jeder absoluten Schranke und fuellt in
    # einem Tag 7 GB.
    rss_first, rss_last = samples[0][1], samples[-1][1]
    rss_max = max(s[1] for s in samples)
    minuten = max(wall / 60, 1e-9)
    steigung = (rss_last - rss_first) / minuten
    check(
        "F2",
        steigung < 1.0 and (rss_max - rss_first) < 30,
        f"Loop-RSS {rss_first:.0f} -> {rss_last:.0f}MB (Maximum {rss_max:.0f}MB) "
        f"ueber {len(samples)} Proben, Steigung {steigung:+.2f} MB/min "
        f"= {steigung * 1440:+.0f} MB/Tag "
        f"[Buchfuehrung: {len(pool._started)} offen, {len(pool.results)} Ergebnisse, "
        f"{len(pool.pids)} PIDs gedeckelt]",
    )

    fd_first, fd_last = samples[0][2], samples[-1][2]
    fd_max = max(s[2] for s in samples)
    check(
        "F3",
        (fd_last - fd_first) <= 8 and (fd_max - fd_first) <= 16,
        f"Dateideskriptoren {fd_first} -> {fd_last} (Maximum {fd_max})",
    )

    def rate(seg: list[Sample]) -> float:
        dt = seg[-1][0] - seg[0][0]
        return (seg[-1][3] - seg[0][3]) / dt if dt > 0 else 0.0

    fifth = max(1, len(samples) // 5)
    r_early, r_late = rate(samples[: fifth + 1]), rate(samples[-(fifth + 1) :])
    ratio = r_late / r_early if r_early > 0 else 0.0
    check(
        "F4", ratio >= 0.85, f"Durchsatz erstes Fuenftel {r_early:.0f}/s, letztes {r_late:.0f}/s ({ratio * 100:.0f}%)"
    )

    zombies = own_zombies()
    check("F5", not zombies, f"{len(zombies)} Zombie-Kind(er) nach dem Lauf")

    check(
        "F6",
        len(pool.killed) >= 3 and pool.ok_count == erwartet,
        f"{len(pool.killed)}x Kind mitten im Task getoetet — der Pool hat jedes Mal weitergearbeitet",
    )


async def _pruefe_infra(fs: Any, backend: Any, broker: Any, wall: float) -> None:
    """F7: Sind die ueber den Lauf offen gehaltenen Verbindungen noch gut?"""
    raw = await backend.get_progress("proto-f7-vorher")
    await backend.set_progress("proto-f7-nachher", {"current": "2", "total": "2"}, ttl_ms=60_000)
    raw2 = await backend.get_progress("proto-f7-nachher")
    await broker.publish(QUEUE, b'{"task":"proto.par_blas","args":[400,1]}')

    got: asyncio.Queue[Any] = asyncio.Queue()

    async def on_delivery(delivery: Any) -> None:
        await got.put(delivery)
        await delivery.handle.ack()

    await broker.consume(QUEUE, on_delivery, concurrency=1)
    delivery = await asyncio.wait_for(got.get(), timeout=20)
    await broker.cancel_consume()
    msg = json.loads(delivery.body)

    after = ChildPool(fs, 1)
    await after.run_all([(msg["task"], tuple(msg["args"]), "f7")])
    vals = after.ok_values()
    check(
        "F7",
        raw is not None and raw2 is not None and len(vals) == 1 and vals[0]["correct"],
        f"nach {wall:.0f}s Leerlauf: Valkey lesbar und schreibbar, RabbitMQ publish -> consume -> Rechenkind korrekt",
    )


async def series_f(fs: Any, minutes: float) -> None:
    print(f"\nF  Dauerlauf ueber {minutes:.0f} Minuten mit {SLOTS} Slots", flush=True)

    backend = broker = None
    try:
        backend, broker = await _oeffne_infra()
        print("      Broker und Backend verbunden — bleiben ueber den Lauf offen", flush=True)
    except Exception as exc:
        print(f"      Kein Broker/Backend erreichbar ({type(exc).__name__}: {exc}) — F7 wird uebersprungen", flush=True)

    pool, samples, wall = await _dauerlauf(fs, minutes * 60)
    _bewerte_dauerlauf(pool, samples, wall)

    if backend is not None and broker is not None:
        try:
            await _pruefe_infra(fs, backend, broker, wall)
        except Exception as exc:
            check("F7", False, f"Verbindung nach dem Lauf unbrauchbar: {type(exc).__name__}: {exc}")
        finally:
            await broker.close()
            await backend.close()

    print("\n      Verlauf (s / RSS MB / FDs / fertig):", flush=True)
    step = max(1, len(samples) // 12)
    for s in samples[::step]:
        print(f"        {s[0]:7.0f}  {s[1]:7.1f}  {s[2]:5d}  {s[3]:7d}", flush=True)


# ---------------------------------------------------------------------------


async def main() -> int:
    if os.environ.get("PROTO_MODE") == "probe":
        return await probe_mode()

    pinned = os.environ.get("OMP_NUM_THREADS", "<nicht gesetzt>")
    print(
        f"Python {sys.version.split()[0]}  |  Kerne {os.cpu_count()}  |  Slots {SLOTS}  |  OMP_NUM_THREADS={pinned}\n",
        flush=True,
    )

    fs = mp.get_context("forkserver")
    fs.set_forkserver_preload(PRELOAD)

    await series_e(fs)
    await series_f(fs, MINUTES)

    failed = [e for e, ok, _ in _RESULTS if not ok]
    print("\n" + "=" * 72, flush=True)
    print(
        f"{len(_RESULTS) - len(failed)}/{len(_RESULTS)} Punkte bestaetigt"
        + (f" — offen: {', '.join(failed)}" if failed else ""),
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
