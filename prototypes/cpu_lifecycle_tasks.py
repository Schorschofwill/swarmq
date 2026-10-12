"""Task-Modul fuer die Lebenszyklus-Serien G/H/I des CPU-Worker-Modus.

Bewusst unabhaengig von ``cpu_integration_tasks``: dessen Import ruft
``setup_dishka`` mit einem eigenen Container auf, und zwei Aufrufe
ersetzen einander (letzter gewinnt, mit WARNING). Dieses Modul bringt
seinen eigenen Container mit — inklusive eines APP-Providers, der eine
*echte* Valkey-Verbindung oeffnet. Genau die ist der Gegenstand der
H-Serie: sie ist teuer genug, dass sich Wiederverwendung lohnt, und
zaehlbar genug, dass sich die Vervielfachung belegen laesst.

Wie in ``cpu_parallel_tasks`` gilt: **beim Import wird nichts gerechnet
und keine Verbindung geoeffnet** (Auflagen B1 und 3). Der Provider ist
eine Fabrik, die der Forkserver registriert, aber nie aufloest.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import AsyncIterable
from typing import Any, ClassVar

import numpy as np

from swarmq.task import Task, TaskInfo

try:
    from dishka import FromDishka, Provider, Scope, make_async_container, provide

    from swarmq.dishka import SwarmQProvider, setup_dishka

    _HAVE_DISHKA = True
except ImportError:  # pragma: no cover
    _HAVE_DISHKA = False

# Der Provider liest die URL beim Aufloesen, nicht beim Import.
BACKEND_URL = os.environ.get("SWARMQ_BACKEND_URL", "redis://localhost:6399/0")

# Client-Name, unter dem die Rechenkinder am Valkey erscheinen. Der
# Zaehltest filtert ``CLIENT LIST`` darauf, damit fremde Clients die
# Messung nicht verfaelschen.
CLIENT_NAME = "swarmq-proto-kind"


def rss_mb() -> float:
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return -1.0


def own_threads() -> int:
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("Threads:"):
                return int(line.split()[1])
    return -1


class AppPool:
    """APP-scoped Ressource mit echter Verbindung — das teure Objekt.

    Sie ist der Grund, warum der Entwurf DI-Tasks ueberhaupt in einem
    wiederverwendeten Kind laufen laesst: ein frisches Kind je Task
    zahlt diesen Aufbau jedes Mal neu.
    """

    def __init__(self, client: Any, build_ms: float) -> None:
        self.client = client
        self.build_ms = build_ms
        self.built_in_pid = os.getpid()
        self.ident = uuid.uuid4().hex[:8]

    async def ping(self) -> bool:
        return bool(await self.client.ping())


class RequestMarker:
    """REQUEST-scoped Ressource — je Taskausfuehrung eine neue Identitaet.

    Der Nachweis, dass ein *wiederverwendetes* Kind die Scopes nicht
    verschmiert: ``AppPool.ident`` bleibt ueber die Tasks eines Kindes
    gleich, ``RequestMarker.ident`` muss sich jedes Mal aendern.
    """

    def __init__(self) -> None:
        self.ident = uuid.uuid4().hex[:8]


if _HAVE_DISHKA:

    class LifecycleProvider(Provider):
        @provide(scope=Scope.APP)
        async def app_pool(self) -> AsyncIterable[AppPool]:
            import valkey.asyncio

            t0 = time.perf_counter()
            client = valkey.asyncio.Valkey.from_url(BACKEND_URL, client_name=CLIENT_NAME, decode_responses=True)
            # Erst der erste Befehl baut die Verbindung wirklich auf —
            # ohne ihn maesse die Amortisation nur einen Konstruktor.
            await client.ping()
            pool = AppPool(client, (time.perf_counter() - t0) * 1000)
            try:
                yield pool
            finally:
                await client.aclose()

        @provide(scope=Scope.REQUEST)
        def request_marker(self) -> RequestMarker:
            return RequestMarker()

    setup_dishka(make_async_container(LifecycleProvider(), SwarmQProvider()))


class DiPoolTask(Task):
    """Loest die teure APP-Ressource auf und belegt beide Scope-Identitaeten."""

    task_info = TaskInfo(name="proto.di_pool", queues=["cpu"])

    # WICHTIG: kein ``FromDishka[X] | None = None``. Diese Form sieht
    # wie eine optionale Injektion aus, wird von dishka aber gar nicht
    # als Abhaengigkeit erkannt — der Parameter bleibt still ``None``,
    # und ``validate_registered_tasks`` schlaegt nicht an, weil
    # ``_dishka_injected_keys`` leer bleibt. Nachgemessen am
    # 2026-08-31; ``cpu_integration_tasks.NumpyTask`` hat genau diese
    # Signatur und bekommt dort nie ein ``db`` injiziert.
    async def run(
        self,
        n: int,
        pool: FromDishka[AppPool],  # type: ignore[valid-type]
        marker: FromDishka[RequestMarker],  # type: ignore[valid-type]
    ) -> dict[str, Any]:
        a = np.eye(n, dtype=np.float64) * 2.0
        b = np.eye(n, dtype=np.float64) * 3.0
        c = a @ b
        alive = await pool.ping()
        return {
            "correct": abs(float(np.trace(c)) - 6.0 * n) < 1e-6 and alive,
            "pid": os.getpid(),
            "app_ident": pool.ident,
            "app_build_ms": pool.build_ms,
            "req_ident": marker.ident,
        }


class BlasTask(Task):
    """Rechenlast mit analytisch bekanntem Ergebnis, ohne DI."""

    task_info = TaskInfo(name="proto.lc_blas", queues=["cpu"])

    async def run(self, n: int = 400, reps: int = 3) -> dict[str, Any]:
        a = np.eye(n, dtype=np.float64) * 2.0
        b = np.eye(n, dtype=np.float64) * 3.0
        t0 = time.perf_counter()
        c = a
        for _ in range(reps):
            c = a @ b
        return {
            "correct": abs(float(np.trace(c)) - 6.0 * n) < 1e-6,
            "cpu_s": time.perf_counter() - t0,
            "threads": own_threads(),
            "pid": os.getpid(),
        }


class MemoryTask(Task):
    """Belegt echten Speicher und haelt ihn, bis die Geschwister da sind.

    ``np.ones`` fasst jede Seite an — der Speicher ist wirklich resident
    und nicht nur reserviert.
    """

    task_info = TaskInfo(name="proto.lc_mem", queues=["cpu"])

    async def run(self, mb: int = 400, hold_s: float = 0.4) -> dict[str, Any]:
        buf = np.ones(mb * 1024 * 1024 // 8, dtype=np.float64)
        resident = rss_mb()
        await asyncio.sleep(hold_s)
        checksum = float(buf[0] + buf[-1])
        return {
            "correct": abs(checksum - 2.0) < 1e-9 and resident > mb * 0.8,
            "resident_mb": resident,
            "pid": os.getpid(),
        }


class LeakTask(Task):
    """Behaelt Puffer ueber das Taskende hinaus — im selben Prozess.

    Im wiederverwendeten Kind waechst der RSS damit von Task zu Task.
    Das ist der Ausloeser, den die Recycling-Regel treffen soll.
    """

    task_info = TaskInfo(name="proto.lc_leak", queues=["cpu"])

    _leaked: ClassVar[list[Any]] = []

    async def run(self, mb: int = 40) -> dict[str, Any]:
        LeakTask._leaked.append(np.ones(mb * 1024 * 1024 // 8, dtype=np.float64))
        return {
            "correct": True,
            "pid": os.getpid(),
            "held_mb": sum(a.nbytes for a in LeakTask._leaked) / 1024 / 1024,
        }


class LongBlasTask(Task):
    """Laeuft Sekunden in C-Code — fuer den ``consumer_timeout``-Test.

    Waehrend dieser Task rechnet, fuehrt das Kind keinen Python-Bytecode
    aus. Der Loop-Prozess muss trotzdem den Broker bedienen, sonst kappt
    RabbitMQ den Kanal.
    """

    task_info = TaskInfo(name="proto.lc_long", queues=["cpu"])

    async def run(self, seconds: float = 15.0) -> dict[str, Any]:
        rng = np.random.default_rng(0)
        m = rng.random((700, 700))
        t0 = time.perf_counter()
        runden = 0
        while time.perf_counter() - t0 < seconds:
            np.linalg.svd(m, compute_uv=False)
            runden += 1
        return {
            "correct": runden > 0,
            "cpu_s": time.perf_counter() - t0,
            "runden": runden,
            "pid": os.getpid(),
        }


class FailTask(Task):
    """Wirft — fuer den Retry- und DLQ-Pfad ueber die Prozessgrenze."""

    task_info = TaskInfo(name="proto.lc_fail", queues=["cpu"])

    async def run(self, msg: str = "absichtlich") -> dict[str, Any]:
        raise ValueError(msg)
