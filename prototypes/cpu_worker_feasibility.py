"""Machbarkeitstest fuer den CPU-Worker-Modus.

Prueft die Annahmen des Entwurfs empirisch, statt sie aus dem Code
abzuleiten. Jede Annahme ist ein eigener Testblock mit PASS/FAIL.

    uv run --extra dishka python prototypes/cpu_worker_feasibility.py

Geprueft wird:

  A1  forkserver+Preload startet Rechenkinder deutlich schneller als spawn
  A2  Der Event-Loop des Elternprozesses bleibt waehrend CPU-Arbeit bedienbar
  A3  Ein Kind ohne await-Punkte ist hart killbar (Hard-Timeout traegt)
  A4  Dishka-Injektion loest im Rechenkind auf — Signatur unveraendert
  A5  Der REQUEST-Scope traegt den TaskContext ueber die Prozessgrenze
  A6  update_progress laeuft ueber den Rueckkanal, waehrend das Kind rechnet
  A7  Exceptions kommen typerhaltend beim Elternprozess an (Retry/DLQ)
  A8  RSS nach Taskende erkennt einen Leak; ein frisches Kind ist ihn los
  A9  Der Forkserver erbt keinen Event-Loop-Zustand
  A10 APP-Scope lebt pro Kind — die dokumentierte Verhaltensaenderung
  A11 Der Forkserver haelt den Container KALT (keine geerbten Verbindungen)
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import statistics
import sys
import time
import traceback
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

PRELOAD = ["prototypes.cpu_feasibility_tasks"]

_RESULTS: list[tuple[str, bool, str]] = []


def check(aid: str, ok: bool, detail: str) -> None:
    _RESULTS.append((aid, ok, detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {aid}  {detail}")


# ---------------------------------------------------------------------------
# Kind-seitige Ausfuehrung: das, was der CPU-Worker im Rechenkind taete.
# ---------------------------------------------------------------------------


class _QueueBackend:
    """Backend-Ersatz im Rechenkind: statt selbst nach Valkey zu schreiben,
    schiebt er die Fortschrittsmeldung ueber den Rueckkanal zum Elternprozess.

    Erfuellt genau die zwei Methoden, die Task.update_progress aufruft — die
    Task-Signatur bleibt dadurch unveraendert."""

    def __init__(self, queue: Any) -> None:
        self._queue = queue

    async def set_progress(self, task_id: str, fields: dict[str, str], ttl_ms: int) -> None:
        self._queue.put(("progress", task_id, fields["current"], fields["total"], os.getpid()))

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        return None


def child_run_task(
    task_name: str,
    args: tuple[Any, ...],
    task_id: str,
    progress_q: Any,
    result_q: Any,
    probe_app_scope_cold: bool = False,
) -> None:
    """Fuehrt einen Task im Rechenkind aus — inkl. DI-REQUEST-Scope,
    Progress-Wiring und Fehlerweitergabe."""
    from prototypes import cpu_feasibility_tasks as tasks
    from swarmq.dishka import _MISSING, TaskContext, get_registered_container, request_container
    from swarmq.task import Task

    # A11: Ist der APP-Scope beim Fork bereits materialisiert gewesen?
    # instances == 0 heisst: der Forkserver hat den Container nur registriert,
    # nie aufgeloest — es kann also keine geerbte Verbindung geben.
    if probe_app_scope_cold:
        result_q.put(("app_scope_cold", tasks.Database.instances == 0, tasks.Database.instances))
        return

    cls = next(c for c in Task._registered_tasks if c.task_info.name == task_name)

    async def _run() -> None:
        instance = cls()
        instance._task_id = task_id
        instance._backend = _QueueBackend(progress_q)
        instance._task_start_time = time.monotonic()
        instance._result_ttl_ms = 86_400_000

        container = get_registered_container()
        scope = None
        token = None
        if container is not _MISSING:
            scope = container(
                context={TaskContext: TaskContext(task_id=task_id, task_name=task_name, queue="cpu", retry_count=0)}
            )
            req = await scope.__aenter__()
            token = request_container.set(req)
        try:
            value = await instance.run(*args)
            result_q.put(("ok", value, os.getpid(), tasks.rss_mb()))
        except Exception as exc:
            result_q.put(("error", type(exc).__name__, str(exc), traceback.format_exc()[-300:]))
        finally:
            if token is not None:
                request_container.reset(token)
            if scope is not None:
                await scope.__aexit__(None, None, None)

    asyncio.run(_run())


def child_noop() -> None:
    import prototypes.cpu_feasibility_tasks  # noqa: F401


def child_loop_probe(result_q: Any) -> None:
    """A9: Erbt das Kind einen laufenden Event-Loop aus dem Elternprozess?"""
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        running = loop.is_running()
        closed = loop.is_closed()
    except Exception as exc:  # kein Loop vorhanden -> ebenfalls sauber
        result_q.put(("loop", True, f"kein Loop im Kind ({type(exc).__name__})"))
        return
    result_q.put(("loop", not running, f"running={running} closed={closed}"))


def child_leak_series(result_q: Any, rounds: int) -> None:
    """Fuehrt LeakTask mehrfach im SELBEN Kind aus und meldet RSS nach jedem
    Durchgang — belegt, dass die Messung nach Taskende den Leak sieht."""
    from prototypes import cpu_feasibility_tasks as tasks

    async def _run() -> None:
        rss = [tasks.rss_mb()]
        for _ in range(rounds):
            await tasks.LeakTask().run(40)
            rss.append(tasks.rss_mb())
        result_q.put(("leak", rss, os.getpid()))

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Elternprozess
# ---------------------------------------------------------------------------


def bench_start(ctx: Any, method: str, n: int = 10) -> float:
    p = ctx.Process(target=child_noop)
    p.start()
    p.join()  # Warmup
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        p = ctx.Process(target=child_noop)
        p.start()
        p.join()
        ts.append((time.perf_counter() - t0) * 1000)
    return statistics.median(ts)


async def main() -> int:  # noqa: PLR0915 - linearer Testablauf, bewusst eine Funktion
    print(f"Python {sys.version.split()[0]}  |  CPU-Kerne: {os.cpu_count()}\n")

    try:
        import dishka  # noqa: F401
    except ImportError:
        print("FEHLER: dishka fehlt. Aufruf: uv run --extra dishka python prototypes/cpu_worker_feasibility.py")
        return 2

    fs = mp.get_context("forkserver")
    fs.set_forkserver_preload(PRELOAD)
    sp = mp.get_context("spawn")

    # -- A1 -----------------------------------------------------------------
    print("A1  Startkosten eines Rechenkindes")
    t0 = time.perf_counter()
    warm = fs.Process(target=child_noop)
    warm.start()
    warm.join()
    boot_ms = (time.perf_counter() - t0) * 1000
    fs_ms = bench_start(fs, "forkserver")
    sp_ms = bench_start(sp, "spawn")
    print(f"      Forkserver-Hochlauf einmalig: {boot_ms:.0f}ms")
    check("A1", fs_ms < sp_ms / 3, f"forkserver {fs_ms:.1f}ms vs spawn {sp_ms:.1f}ms (Faktor {sp_ms / fs_ms:.1f}x)")

    # -- A11 ----------------------------------------------------------------
    print("\nA11 Container im Forkserver kalt (keine geerbten Verbindungen)")
    rq = fs.Queue()
    p = fs.Process(target=child_run_task, args=("", (), "", None, rq, True))
    p.start()
    _unused, cold, count = rq.get()
    p.join()
    check("A11", cold, f"Database-Instanzen im frischen Kind vor Scope-Oeffnung: {count}")

    # -- A9 -----------------------------------------------------------------
    print("\nA9  Kein geerbter Event-Loop-Zustand (Fork aus laufendem Loop)")
    rq = fs.Queue()
    p = fs.Process(target=child_loop_probe, args=(rq,))
    p.start()
    while p.is_alive():
        await asyncio.sleep(0.002)  # Elternprozess hat hier einen LAUFENDEN Loop
    p.join()
    _unused, ok, detail = rq.get()
    check("A9", ok, detail)

    # -- A2, A4, A5, A6 -----------------------------------------------------
    print("\nA2/A4/A5/A6  DI, Progress und freier Loop im laufenden Betrieb")
    pq, rq = fs.Queue(), fs.Queue()
    ticks = 0

    async def heartbeat() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    hb = asyncio.create_task(heartbeat())
    progress_seen: list[tuple[str, str, int]] = []
    t0 = time.perf_counter()
    p = fs.Process(target=child_run_task, args=("feasibility.image", (4,), "task-abc", pq, rq))
    p.start()
    while p.is_alive():
        while not pq.empty():
            _kind, _tid, cur, tot, cpid = pq.get()
            progress_seen.append((cur, tot, cpid))
        await asyncio.sleep(0.005)
    p.join()
    while not pq.empty():
        _kind, _tid, cur, tot, cpid = pq.get()
        progress_seen.append((cur, tot, cpid))
    dur_ms = (time.perf_counter() - t0) * 1000
    hb.cancel()

    status, value, child_pid, _rss = rq.get()
    check("A2", ticks > dur_ms / 20, f"{dur_ms:.0f}ms CPU-Arbeit im Kind -> Heartbeat schlug {ticks}x")
    check(
        "A4",
        status == "ok" and "db(" in str(value.get("db", "")),
        f"FromDishka aufgeloest im Kind: {value.get('db') if status == 'ok' else value}",
    )
    check(
        "A5",
        status == "ok" and value.get("session_task_id") == "task-abc",
        f"TaskContext im REQUEST-Scope: {value.get('session_task_id') if status == 'ok' else 'n/a'}",
    )
    check(
        "A6",
        len(progress_seen) > 0 and all(pid == child_pid for _, _, pid in progress_seen),
        f"{len(progress_seen)} Fortschrittsmeldungen aus PID {child_pid} "
        f"waehrend der Rechnung: {[c for c, _, _ in progress_seen]}",
    )

    # -- A10 ----------------------------------------------------------------
    print("\nA10 APP-Scope-Lebensdauer je Rechenkind")
    nos = []
    for _ in range(2):
        # Queues MUESSEN in Variablen gehalten werden: ein inline erzeugtes
        # mp.Queue() im args-Tupel wird im Elternprozess sofort eingesammelt,
        # bevor das Kind seine Semaphore rekonstruieren kann (FileNotFoundError).
        pq2, rq2 = fs.Queue(), fs.Queue()
        p = fs.Process(target=child_run_task, args=("feasibility.image", (1,), "t", pq2, rq2))
        p.start()
        st, val, _cpid, _crss = rq2.get()  # vor join() leeren, sonst Deadlock
        p.join()
        nos.append(val.get("db_instance_no") if st == "ok" else -1)
    check("A10", nos == [1, 1], f"Jedes Kind baut sein eigenes APP-Singleton (instance_no je Kind: {nos})")

    # -- A3 -----------------------------------------------------------------
    print("\nA3  Hard-Timeout: blockierendes Kind ohne await-Punkte")
    pq3, rq3 = fs.Queue(), fs.Queue()
    p = fs.Process(target=child_run_task, args=("feasibility.hang", (), "t", pq3, rq3))
    p.start()
    await asyncio.sleep(0.3)
    alive_before = p.is_alive()
    t0 = time.perf_counter()
    p.kill()
    p.join(timeout=5)
    kill_ms = (time.perf_counter() - t0) * 1000
    check(
        "A3",
        alive_before and not p.is_alive() and p.exitcode == -9,
        f"in {kill_ms:.1f}ms beendet, exitcode={p.exitcode}",
    )

    # -- A7 -----------------------------------------------------------------
    print("\nA7  Exception-Weitergabe ueber die Prozessgrenze")
    pq4, rq = fs.Queue(), fs.Queue()
    p = fs.Process(target=child_run_task, args=("feasibility.fail", (), "t", pq4, rq))
    p.start()
    payload = rq.get()
    p.join()
    check("A7", payload[0] == "error" and payload[1] == "ValueError", f"{payload[1]}: {payload[2]}")

    # -- A8 -----------------------------------------------------------------
    print("\nA8  Leak-Erkennung per RSS nach Taskende")
    rq = fs.Queue()
    p = fs.Process(target=child_leak_series, args=(rq, 3))
    p.start()
    _unused, rss_series, _leak_pid = rq.get()
    p.join()
    growth = rss_series[-1] - rss_series[0]
    rq2 = fs.Queue()
    p = fs.Process(target=child_leak_series, args=(rq2, 0))
    p.start()
    _unused, fresh_series, _pid = rq2.get()
    p.join()
    check(
        "A8",
        growth > 100 and fresh_series[0] < rss_series[-1] - 100,
        f"RSS im selben Kind {[f'{r:.0f}' for r in rss_series]}MB (+{growth:.0f}MB); "
        f"frisches Kind startet bei {fresh_series[0]:.0f}MB",
    )

    # -- Zusammenfassung ----------------------------------------------------
    failed = [a for a, ok, _ in _RESULTS if not ok]
    print("\n" + "=" * 72)
    print(
        f"{len(_RESULTS) - len(failed)}/{len(_RESULTS)} Annahmen bestaetigt"
        + (f" — offen: {', '.join(failed)}" if failed else "")
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
