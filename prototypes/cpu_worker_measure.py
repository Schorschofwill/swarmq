"""Messnachweis der I-Serie gegen die Implementierung des CPU-Modus (Plan U7, Verification Contract).

Faehrt die fuenf I-Messpunkte aus ``docs/cpu-worker-machbarkeit.md`` plus die
Startlatenz (M0) gegen ``SwarmQ(pool="cpu")`` am echten Broker und druckt eine
Markdown-Tabelle. Manuell auszufuehren::

    SWARMQ_TEST_RABBITMQ_PORT=5699 SWARMQ_TEST_VALKEY_PORT=6399 \\
        uv run --extra test python -m prototypes.cpu_worker_measure

Broker/Backend kommen aus ``SWARMQ_BROKER_URL``/``SWARMQ_BACKEND_URL`` oder den
Test-Ports. Nichts davon ist ein Test: das Skript misst und berichtet.
"""

from __future__ import annotations

import asyncio
import os
import signal
import statistics
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

os.environ.setdefault("CPU_MEASURE_QUEUE", f"cpu-measure-{uuid.uuid4().hex[:8]}")

from prototypes import cpu_measure_tasks as tasks
from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.worker.compute import pool as poolmod

_POOLS: list[poolmod.SlotPool] = []
_orig_pool_start = poolmod.SlotPool.start


async def _capturing_start(self: poolmod.SlotPool) -> None:
    _POOLS.append(self)
    await _orig_pool_start(self)


poolmod.SlotPool.start = _capturing_start  # type: ignore[method-assign]  # Diagnosezugriff auf den Pool

QUEUE = tasks.QUEUE
BROKER = os.environ.get(
    "SWARMQ_BROKER_URL", f"amqp://guest:guest@localhost:{os.environ.get('SWARMQ_TEST_RABBITMQ_PORT', '5672')}/"
)
BACKEND = os.environ.get(
    "SWARMQ_BACKEND_URL", f"valkey://localhost:{os.environ.get('SWARMQ_TEST_VALKEY_PORT', '6379')}/0"
)


class Recorder:
    """Signal-Mitschrift: Startzeiten, In-Flight-Zaehler, Retries, DLQ."""

    def __init__(self, app: SwarmQ) -> None:
        self.started: dict[str, float] = {}
        self.finished: set[str] = set()
        self.max_in_flight = 0
        self.retrying = 0
        self.failed = 0
        self.dlq = 0

        @app.on_signal(Signal.TASK_STARTED)
        async def _started(task_id: str, **_: Any) -> None:
            self.started[task_id] = time.time()
            self.max_in_flight = max(self.max_in_flight, len(self.started) - len(self.finished))

        @app.on_signal(Signal.TASK_COMPLETED)
        async def _ok(task_id: str, **_: Any) -> None:
            self.finished.add(task_id)

        @app.on_signal(Signal.TASK_FAILED)
        async def _failed(task_id: str, **_: Any) -> None:
            self.failed += 1
            self.finished.add(task_id)

        @app.on_signal(Signal.TASK_RETRYING)
        async def _retry(task_id: str, **_: Any) -> None:
            self.retrying += 1
            self.finished.add(task_id)

        @app.on_signal(Signal.TASK_DLQ)
        async def _dlq(task_id: str, **_: Any) -> None:
            self.dlq += 1


def _app(concurrency: int, idle: float = 2.0, **cfg: Any) -> SwarmQ:
    return SwarmQ(
        RabbitMQBroker(BROKER),
        ValkeyBackend(BACKEND),
        queues=[QUEUE],
        pool="cpu",
        concurrency=concurrency,
        burst_mode=True,
        burst_idle_timeout=idle,
        **cfg,
    )


async def _results(tids: list[str]) -> list[Any]:
    reader = SwarmQ(RabbitMQBroker(BROKER), ValkeyBackend(BACKEND), queues=[QUEUE])
    out: list[Any] = []
    async with reader:
        for t in tids:
            try:
                out.append(await reader.get_result(t, timeout=10.0))
            except Exception as exc:  # TaskError (Fehlerergebnis) oder ResultNotReadyError (nie bearbeitet)
                out.append(exc)
    return out


async def m0_start_latency(n: int = 40) -> str:
    app = _app(concurrency=2)
    rec = Recorder(app)
    async with app:
        await app.broker.declare_queue(QUEUE)
        tids = [await app.schedule("measure_echo", i) for i in range(n)]
        await asyncio.wait_for(app.start_worker(), timeout=120)
    results = await _results(tids)
    lat = sorted((r["run_at"] - rec.started[t]) * 1000 for t, r in zip(tids, results, strict=True))
    pids = {r["pid"] for r in results}
    spawns = sorted(s for s, _ in _POOLS[-1].recent_spawn_ms)
    readies = sorted(r for _, r in _POOLS[-1].recent_spawn_ms)
    return (
        f"| M0 | Startlatenz Dispatch -> ``run`` im frischen Kind, n={n}: median {statistics.median(lat):.1f} ms, "
        f"p95 {lat[int(len(lat) * 0.95) - 1]:.1f} ms, max {lat[-1]:.1f} ms; davon process.start() median "
        f"{statistics.median(spawns):.1f} ms, Spawn bis Ready median {statistics.median(readies):.1f} ms; "
        f"{len(pids)} verschiedene Kind-PIDs |"
    )


async def i1_shutdown_with_computing_children() -> str:
    app = _app(concurrency=4, idle=30.0, shutdown_timeout=10)
    rec = Recorder(app)
    async with app:
        await app.broker.declare_queue(QUEUE)
        tids = [await app.schedule("measure_sleep", 2.0) for _ in range(8)]
        worker = asyncio.create_task(app.start_worker())
        while len(rec.started) < 4:
            await asyncio.sleep(0.02)
        t0 = time.monotonic()
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(worker, timeout=60)
        drain_s = time.monotonic() - t0
    first = await _results(tids)
    done_first = sum(1 for r in first if not isinstance(r, Exception))
    # Rest per zweitem Burst-Lauf abholen: die vier nicht angenommenen liegen noch in der Queue.
    app2 = _app(concurrency=4, idle=3.0)
    async with app2:
        await asyncio.wait_for(app2.start_worker(), timeout=60)
    second = await _results(tids)
    done_total = sum(1 for r in second if not isinstance(r, Exception))
    return (
        f"| I1 | SIGTERM bei 4 rechnenden Kindern (8 geplant, 4 Slots): Drain {drain_s:.2f} s, "
        f"{done_first}/8 im ersten Lauf fertig, {done_total}/8 nach dem zweiten Lauf, "
        f"Retries {rec.retrying}, Fehler {rec.failed} |"
    )


async def i2_kill_before_ack() -> str:
    tmp = Path(tempfile.mkdtemp(prefix="swarmq-i2-"))
    pid_file = tmp / "child.pid"
    app = _app(concurrency=1, idle=3.0)
    rec = Recorder(app)
    async with app:
        await app.broker.declare_queue(QUEUE)
        tid = await app.schedule("measure_kill_me", str(pid_file), str(tmp))
        worker = asyncio.create_task(app.start_worker())
        while not pid_file.exists():
            await asyncio.sleep(0.02)
        os.kill(int(pid_file.read_text()), signal.SIGKILL)
        await asyncio.wait_for(worker, timeout=60)
    result = (await _results([tid]))[0]
    attempts = len(list(tmp.glob("attempt-*")))
    return (
        f"| I2 | Kind vor dem Ack per SIGKILL getoetet: {attempts} Versuche, Retry-Signale {rec.retrying}, "
        f"Ergebnis {result!r} — kein Verlust |"
    )


async def i3_long_compute_loop_jitter(seconds: float = 20.0) -> str:
    app = _app(concurrency=1, idle=3.0)
    rec = Recorder(app)
    async with app:
        await app.broker.declare_queue(QUEUE)
        tid = await app.schedule("measure_compute", seconds)
        worker = asyncio.create_task(app.start_worker())
        while not rec.started:  # Jitter erst messen, wenn das Kind rechnet (nicht den Forkserver-Start)
            await asyncio.sleep(0.01)
        samples: list[float] = []
        while not rec.finished and not worker.done():
            before = time.monotonic()
            await asyncio.sleep(0.01)
            samples.append((time.monotonic() - before - 0.01) * 1000)
        await asyncio.wait_for(worker, timeout=60)
    result = (await _results([tid]))[0]
    samples.sort()
    return (
        f"| I3 | {seconds:.0f} s reine Rechenzeit im Kind: {result} Iterationen; Loop-Jitter waehrenddessen "
        f"median {statistics.median(samples):.2f} ms, p99 {samples[int(len(samples) * 0.99) - 1]:.2f} ms, "
        f"max {samples[-1]:.2f} ms |"
    )


async def i4_prefetch_equals_slots(slots: int = 4, n: int = 24) -> str:
    app = _app(concurrency=slots, idle=3.0)
    rec = Recorder(app)
    async with app:
        await app.broker.declare_queue(QUEUE)
        tids = [await app.schedule("measure_sleep", 0.3) for _ in range(n)]
        await asyncio.wait_for(app.start_worker(), timeout=120)
    ok = sum(1 for r in await _results(tids) if not isinstance(r, Exception))
    return (
        f"| I4 | {n} Tasks auf {slots} Slots: hoechstens {rec.max_in_flight} gleichzeitig angenommen, "
        f"{ok}/{n} Ergebnisse |"
    )


async def i5_typed_exception_retries_to_dlq() -> str:
    app = _app(concurrency=1, idle=3.0)
    rec = Recorder(app)
    async with app:
        await app.broker.declare_queue(QUEUE)
        tid = await app.schedule("measure_fail")
        await asyncio.wait_for(app.start_worker(), timeout=60)
    result = (await _results([tid]))[0]
    return (
        f"| I5 | ValueError im Kind mit max_retries=2: {rec.failed} Fehlversuche, {rec.retrying} Retries, "
        f"DLQ-Signale {rec.dlq}, Fehlertext {str(result)!r} |"
    )


async def main() -> None:
    import sys

    only = set(sys.argv[1:])
    steps = {
        "m0": m0_start_latency,
        "i1": i1_shutdown_with_computing_children,
        "i2": i2_kill_before_ack,
        "i3": i3_long_compute_loop_jitter,
        "i4": i4_prefetch_equals_slots,
        "i5": i5_typed_exception_retries_to_dlq,
    }
    rows = [await fn() for name, fn in steps.items() if not only or name in only]
    print(f"\nMessnachweis I-Serie gegen swarmq {_version()} — Queue {QUEUE}\n")
    print("| | Befund |\n|---|---|")
    print("\n".join(rows))


def _version() -> str:
    from importlib.metadata import version

    return version("swarmq")


if __name__ == "__main__":
    asyncio.run(main())
