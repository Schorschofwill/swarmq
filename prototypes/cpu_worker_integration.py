"""Integrationstest fuer den CPU-Worker-Modus — die Risiken, die der
reine Machbarkeitstest (``cpu_worker_feasibility.py``) offenlassen musste.

    docker run -d --name swarmq-proto-rabbit -p 5699:5672 \
        -e RABBITMQ_DEFAULT_USER=guest -e RABBITMQ_DEFAULT_PASS=guest rabbitmq:4
    docker run -d --name swarmq-proto-valkey -p 6399:6379 valkey/valkey:8

    SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5699/" \
    SWARMQ_BACKEND_URL="redis://localhost:6399/0" PROTO_ROUNDS=200 \
    uv run --extra dishka --with numpy --with pillow \
        python -u prototypes/cpu_worker_integration.py

Eigene Ports, damit weder ein laufender loadsuite-Stack noch ein anderer
lokaler Broker beruehrt wird.

Geprueft wird:

  B-Serie — C-Extensions (die fork-Gefahr):
    B1  Der Forkserver bleibt threadfrei, obwohl numpy/Pillow vorgeladen sind
    B2  NumPy/BLAS rechnet im Rechenkind korrekt (Fork mit geladener libopenblas)
    B3  Pillow verarbeitet im Rechenkind korrekt
    B4  Ein NumPy-Leak waechst im selben Kind und ist im frischen Kind weg
    B5  Ein Kind, das in C-Code (BLAS) haengt, ist trotzdem hart killbar

  C-Serie — echte Infrastruktur:
    C1  update_progress landet ueber den Rueckkanal in echtem Valkey
    C2  Cancel ueber das Valkey-Flag beendet ein rechnendes Kind
    C3  Ende-zu-Ende ueber echtes RabbitMQ: publish -> consume -> Rechenkind

  D-Serie — Dauerlauf:
    D1  Viele Tasks in Folge: RSS des Elternprozesses bleibt flach
    D2  Forkserver-Tod mitten im Betrieb — erholt sich das System?
    D3  Kind-Tod mitten im Task — sauberer Fehlerpfad statt Haenger
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BROKER_URL = os.environ.get("SWARMQ_BROKER_URL", "amqp://guest:guest@localhost:5672/")
BACKEND_URL = os.environ.get("SWARMQ_BACKEND_URL", "redis://localhost:6379/0")
QUEUE = "swarmq-cpu-proto"

PRELOAD = ["prototypes.cpu_integration_tasks"]

_RESULTS: list[tuple[str, bool, str]] = []


def check(bid: str, ok: bool, detail: str) -> None:
    _RESULTS.append((bid, ok, detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {bid}  {detail}", flush=True)


# ---------------------------------------------------------------------------
# Kind-Entrypoints
# ---------------------------------------------------------------------------


class _QueueBackend:
    """Rueckkanal-Adapter: erfuellt die zwei Methoden, die
    Task.update_progress aufruft, und schiebt sie zum Elternprozess."""

    def __init__(self, queue: Any) -> None:
        self._queue = queue

    async def set_progress(self, task_id: str, fields: dict[str, str], ttl_ms: int) -> None:
        self._queue.put(("progress", task_id, dict(fields), ttl_ms))

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        return None


def child_thread_probe(result_q: Any) -> None:
    """B1: Wie viele Threads hatte der Forkserver zum Fork-Zeitpunkt?

    Das Kind liest die Thread-Zahl seines ELTERN (= Forkserver) aus /proc.
    Ein Forkserver mit >1 Thread waere die klassische fork-Falle: ein von
    einem fremden Thread gehaltener Lock bleibt im Kind ewig gesperrt.
    """
    ppid = os.getppid()
    threads = -1
    try:
        with open(f"/proc/{ppid}/status") as fh:
            for line in fh:
                if line.startswith("Threads:"):
                    threads = int(line.split()[1])
    except OSError:
        pass
    import numpy  # bereits vorgeladen  # noqa: F401

    result_q.put(("threads", threads, ppid))


def child_task(
    task_name: str,
    args: tuple[Any, ...],
    task_id: str,
    progress_q: Any,
    result_q: Any,
) -> None:
    """Fuehrt einen Task im Rechenkind aus — mit DI-REQUEST-Scope."""
    import traceback

    from prototypes import cpu_integration_tasks as tasks
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
                context={
                    TaskContext: TaskContext(
                        task_id=task_id, task_name=task_name, queue=QUEUE, retry_count=0
                    )
                }
            )
            token = request_container.set(await scope.__aenter__())
        try:
            value = await inst.run(*args)
            result_q.put(("ok", value, os.getpid(), tasks.rss_mb()))
        except Exception as exc:
            result_q.put(("error", type(exc).__name__, str(exc), traceback.format_exc()[-200:]))
        finally:
            if token is not None:
                request_container.reset(token)
            if scope is not None:
                await scope.__aexit__(None, None, None)

    asyncio.run(_run())


def child_leak_series(result_q: Any, rounds: int) -> None:
    from prototypes import cpu_integration_tasks as tasks

    async def _run() -> None:
        rss = [tasks.rss_mb()]
        for _ in range(rounds):
            await tasks.NumpyLeakTask().run(40)
            rss.append(tasks.rss_mb())
        result_q.put(("leak", rss, os.getpid()))

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Hilfsmittel im Elternprozess
# ---------------------------------------------------------------------------


async def run_child(fs: Any, task_name: str, args: tuple[Any, ...], task_id: str) -> tuple[Any, list[dict[str, str]]]:
    """Startet ein Rechenkind, haelt den Loop frei und sammelt Fortschritt."""
    pq, rq = fs.Queue(), fs.Queue()
    progress: list[dict[str, str]] = []
    p = fs.Process(target=child_task, args=(task_name, args, task_id, pq, rq))
    p.start()
    while p.is_alive():
        while not pq.empty():
            progress.append(pq.get()[2])
        await asyncio.sleep(0.005)
    while not pq.empty():
        progress.append(pq.get()[2])
    payload = rq.get() if not rq.empty() else ("error", "NoResult", "Kind lieferte nichts", "")
    p.join()
    return payload, progress


def rss_mb() -> float:
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0


# ---------------------------------------------------------------------------
# B-Serie: C-Extensions
# ---------------------------------------------------------------------------


async def series_b(fs: Any) -> None:
    print("\nB  C-Extensions (numpy / Pillow) ueber die fork-Grenze", flush=True)

    rq = fs.Queue()
    p = fs.Process(target=child_thread_probe, args=(rq,))
    p.start()
    _tag, threads, fs_pid = rq.get()
    p.join()
    check("B1", threads == 1,
          f"Forkserver (PID {fs_pid}) hatte {threads} Thread(s) beim Fork "
          f"— numpy/Pillow vorgeladen")

    payload, _ = await run_child(fs, "proto.numpy", (600,), "b2")
    ok = payload[0] == "ok" and payload[1].get("correct") is True
    check("B2", ok,
          f"BLAS im Kind: {payload[1].get('detail') if ok else payload[1:]}")

    payload, _ = await run_child(fs, "proto.pillow", (1400,), "b3")
    ok = payload[0] == "ok" and payload[1].get("correct") is True
    check("B3", ok,
          f"Pillow im Kind: {payload[1].get('detail') if ok else payload[1:]}")

    rq = fs.Queue()
    p = fs.Process(target=child_leak_series, args=(rq, 3))
    p.start()
    _tag, series, _pid = rq.get()
    p.join()
    rq2 = fs.Queue()
    p = fs.Process(target=child_leak_series, args=(rq2, 0))
    p.start()
    _tag, fresh, _pid = rq2.get()
    p.join()
    growth = series[-1] - series[0]
    check("B4", growth > 100 and fresh[0] < series[-1] - 100,
          f"NumPy-Leak im selben Kind {[f'{r:.0f}' for r in series]}MB (+{growth:.0f}MB); "
          f"frisches Kind bei {fresh[0]:.0f}MB")

    # B5: haengen IN C-Code (grosse SVD), nicht in Python-Bytecode
    pq, rq = fs.Queue(), fs.Queue()
    p = fs.Process(target=child_task, args=("proto.blas_hang", (2200,), "b5", pq, rq))
    p.start()
    await asyncio.sleep(1.2)  # sicher mitten im BLAS-Aufruf
    alive = p.is_alive()
    t0 = time.perf_counter()
    p.kill()
    p.join(timeout=5)
    kill_ms = (time.perf_counter() - t0) * 1000
    check("B5", alive and not p.is_alive() and p.exitcode == -9,
          f"Kind mitten im BLAS-Aufruf in {kill_ms:.1f}ms beendet, exitcode={p.exitcode}")


# ---------------------------------------------------------------------------
# C-Serie: echte Infrastruktur
# ---------------------------------------------------------------------------


async def series_c(fs: Any) -> None:
    print("\nC  Echtes Valkey und echtes RabbitMQ", flush=True)
    from swarmq.backend.valkey import ValkeyBackend

    backend = ValkeyBackend(BACKEND_URL)
    await backend.connect()
    try:
        # C1: Rueckkanal -> Elternprozess -> echtes Valkey
        task_id = f"proto-c1-{os.getpid()}"
        payload, progress = await run_child(fs, "proto.numpy_progress", (3,), task_id)
        for fields in progress:
            await backend.set_progress(task_id, fields, ttl_ms=60_000)
        raw = await backend.get_progress(task_id)
        ok = payload[0] == "ok" and raw is not None and b"current" in raw
        check("C1", ok,
              f"{len(progress)} Meldung(en) aus dem Kind in Valkey geschrieben; "
              f"gelesen: current={raw.get(b'current') if raw else None} "
              f"total={raw.get(b'total') if raw else None}")

        # C2: Cancel ueber echtes Valkey-Flag beendet ein rechnendes Kind
        cancel_id = f"proto-c2-{os.getpid()}"
        pq, rq = fs.Queue(), fs.Queue()
        p = fs.Process(target=child_task, args=("proto.blas_hang", (2200,), cancel_id, pq, rq))
        p.start()
        await asyncio.sleep(0.4)
        await backend.set_cancel_flag(cancel_id, ttl_ms=60_000)
        t0 = time.perf_counter()
        killed = False
        while time.perf_counter() - t0 < 5:  # Elternprozess pollt wie der Worker
            if await backend.is_cancelled(cancel_id):
                p.kill()
                p.join(timeout=5)
                killed = True
                break
            await asyncio.sleep(0.05)
        latency_ms = (time.perf_counter() - t0) * 1000
        await backend.clear_cancel_flag(cancel_id)
        check("C2", killed and p.exitcode == -9,
              f"Cancel-Flag erkannt und Kind beendet nach {latency_ms:.0f}ms, "
              f"exitcode={p.exitcode}")

        # C3: Ende-zu-Ende ueber echtes RabbitMQ
        from swarmq.broker.rabbitmq import RabbitMQBroker

        broker = RabbitMQBroker(BROKER_URL)
        await broker.connect()
        try:
            await broker.declare_queue(QUEUE)
            body = b'{"task":"proto.numpy","args":[400]}'
            await broker.publish(QUEUE, body, headers={"x-proto": "c3"})

            got: asyncio.Queue[Any] = asyncio.Queue()

            async def on_delivery(delivery: Any) -> None:
                await got.put(delivery)
                await delivery.handle.ack()

            await broker.consume(QUEUE, on_delivery, concurrency=1)
            delivery = await asyncio.wait_for(got.get(), timeout=15)
            import json

            msg = json.loads(delivery.body)
            payload, _ = await run_child(fs, msg["task"], tuple(msg["args"]), "c3")
            await broker.cancel_consume()
            ok = payload[0] == "ok" and payload[1].get("correct") is True
            check("C3", ok,
                  f"publish -> consume -> Rechenkind (PID {payload[2] if ok else '?'}): "
                  f"{payload[1].get('detail') if ok else payload[1:]}")
        finally:
            await broker.close()
    finally:
        await backend.close()


# ---------------------------------------------------------------------------
# D-Serie: Dauerlauf und Prozesstod
# ---------------------------------------------------------------------------


async def series_d(fs: Any, rounds: int) -> None:
    print(f"\nD  Dauerlauf ueber {rounds} Tasks inkl. Prozesstod", flush=True)

    rss_start = rss_mb()
    ok_count = 0
    pids: set[int] = set()
    t0 = time.perf_counter()
    for i in range(rounds):
        payload, _ = await run_child(fs, "proto.numpy", (250,), f"d1-{i}")
        if payload[0] == "ok":
            ok_count += 1
            pids.add(payload[2])
    dur = time.perf_counter() - t0
    rss_end = rss_mb()
    check("D1", ok_count == rounds and (rss_end - rss_start) < 40,
          f"{ok_count}/{rounds} Tasks in {dur:.1f}s, {len(pids)} verschiedene Kinder; "
          f"Eltern-RSS {rss_start:.0f} -> {rss_end:.0f}MB")

    # D2: Forkserver toeten und pruefen, ob weitergearbeitet werden kann
    fs_pid = None
    try:
        from multiprocessing import forkserver as _fsmod

        fs_pid = _fsmod._forkserver._forkserver_pid
    except Exception:
        pass
    killed_ok = False
    if fs_pid:
        os.kill(fs_pid, 9)
        await asyncio.sleep(0.3)
        try:
            payload, _ = await run_child(fs, "proto.numpy", (250,), "d2")
            killed_ok = payload[0] == "ok"
        except Exception as exc:
            killed_ok = False
            print(f"      Forkserver-Neuaufbau schlug fehl: {type(exc).__name__}: {exc}", flush=True)
    check("D2", killed_ok,
          f"Forkserver (PID {fs_pid}) getoetet — naechster Task "
          f"{'lief durch' if killed_ok else 'schlug fehl'}")

    # D3: Kind mitten im Task toeten -> sauberer Fehlerpfad, kein Haenger
    pq, rq = fs.Queue(), fs.Queue()
    p = fs.Process(target=child_task, args=("proto.blas_hang", (2000,), "d3", pq, rq))
    p.start()
    await asyncio.sleep(0.5)
    p.kill()
    t0 = time.perf_counter()
    p.join(timeout=5)
    joined_ms = (time.perf_counter() - t0) * 1000
    no_result = rq.empty()
    payload, _ = await run_child(fs, "proto.numpy", (250,), "d3-nach")
    check("D3", not p.is_alive() and no_result and payload[0] == "ok",
          f"Kind getoetet (join {joined_ms:.1f}ms, kein Ergebnis in der Queue: {no_result}); "
          f"Folge-Task {'ok' if payload[0] == 'ok' else 'fehlgeschlagen'}")


# ---------------------------------------------------------------------------


async def main() -> int:
    print(f"Python {sys.version.split()[0]}  |  Broker {BROKER_URL}  |  Backend {BACKEND_URL}\n",
          flush=True)
    rounds = int(os.environ.get("PROTO_ROUNDS", "30"))

    fs = mp.get_context("forkserver")
    fs.set_forkserver_preload(PRELOAD)

    await series_b(fs)
    try:
        await series_c(fs)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        check("C*", False, f"Infrastruktur-Serie abgebrochen: {type(exc).__name__}: {exc}")
    await series_d(fs, rounds)

    failed = [b for b, ok, _ in _RESULTS if not ok]
    print("\n" + "=" * 72, flush=True)
    print(f"{len(_RESULTS) - len(failed)}/{len(_RESULTS)} Punkte bestaetigt"
          + (f" — offen: {', '.join(failed)}" if failed else ""), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
