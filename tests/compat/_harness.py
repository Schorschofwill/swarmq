"""Mischbetrieb-Suite (Q2): Fixtures und Helfer für Worker aus swarmq 0.11.x, 0.12.x und dem Arbeitsstand.

Umgebung: ein isoliertes venv mit ``swarmq==0.11.1`` von PyPI, Abhängigkeiten
exakt wie im Lockfile des Tags (``constraints-0.11.1.txt``). Seit MR 8 ist
0.11.1 die Basis: Der Rollout geht erst auf 0.11.1, und nur 0.11.1 kann die
von 0.12 angelegten Queues deklarieren (Plan Abschnitt 1, Nachtrag
2026-10-10). Ein zweites venv mit ``swarmq==0.11.0``
(``constraints-0.11.0.txt``) prüft genau diese Rollback-Grenze. Der Plan nennt
``uv run --no-project --isolated --with ... --constraints``; uv 0.11 kennt für
``uv run`` aber keine Constraints, und das ``--isolated``-Umfeld ist nach dem
Aufruf weg. Deshalb ``uv venv`` + ``uv pip install -c`` in ein Sitzungs-
Verzeichnis; dessen ``sys.executable`` startet die Worker direkt (kein
verschachteltes ``uv run``, das PDEATHSIG und ``crash_worker`` stören würde).

Zweite Basis seit der Fehlersuche Runde 2 (Plan 2026-10-14-0748, KTD19): ein
venv mit ``swarmq==0.12.0`` (``constraints-0.12.0.txt``); die 0.12.x-Fixes
laufen in beiden Richtungen neben 0.12.0 (R27).

Dritte Basis seit der Fehlersuche Runde 4 (Plan 2026-10-17-2103, U1, R5): ein
venv mit ``swarmq==0.12.3`` (``constraints-0.12.3.txt``), der letzten
Version vor 0.12.4; Retry-Übergabe, Leader-Wechsel und DLQ-Replay laufen
neben 0.12.0 und 0.12.3.

Vierte Basis seit Plan 2026-10-19-1500 (U0): ein venv mit ``swarmq==0.13.1``
(``constraints-0.13.1.txt``), der letzten Version vor 0.14.0. Die Helfer für
Producer, Retry-Übergabe und Shovel-Replay teilen sich die Suiten der 0.12-
und der 0.13-Reihe.

Opt-in über den Marker ``compat`` (braucht Netz zu PyPI beim ersten Lauf).
"""

from __future__ import annotations

import importlib.metadata
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import aio_pika
import pytest
import valkey.asyncio as aio_valkey

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from swarmq.keyspace import KeySpace
from tests.support.queues import delete_queue_family
from tests.support.services import flush_valkey_db, valkey_db_url
from tests.support.wait import wait_until
from tests.support.worker_subprocess import kill_subprocess, spawn_worker, wait_for_worker_started, worker_children

REPO_ROOT = Path(__file__).resolve().parents[2]
_KEYS = KeySpace(None)
CONSTRAINTS = Path(__file__).with_name("constraints-0.11.1.txt")
OLD_VERSION = "0.11.1"
#: Rollback-Grenze (MR 8): 0.11.0 kann von 0.12 angelegte Queues nicht deklarieren.
ROLLBACK_LIMIT_VERSION = "0.11.0"
ROLLBACK_LIMIT_CONSTRAINTS = Path(__file__).with_name("constraints-0.11.0.txt")
#: Basis der 0.12.x-Reihe (KTD19): Rolling Deploy und Rollback neben 0.12.0.
BASE_012_VERSION = "0.12.0"
BASE_012_CONSTRAINTS = Path(__file__).with_name("constraints-0.12.0.txt")
#: Letzte Version vor 0.12.4 (Plan 2026-10-17-2103, U1): Basis für den Rollout von 0.12.4.
BASE_0123_VERSION = "0.12.3"
BASE_0123_CONSTRAINTS = Path(__file__).with_name("constraints-0.12.3.txt")
#: Letzte Version vor 0.14.0 (Plan 2026-10-19-1500, U0): Basis für den Rollout von 0.14.
BASE_0131_VERSION = "0.13.1"
BASE_0131_CONSTRAINTS = Path(__file__).with_name("constraints-0.13.1.txt")
#: Cron-Task und Fixture-Modul für den Leader-Wechsel.
CRON_TASK = "compat_cron"
CRON_MODULE = "tests.compat._compat_cron_tasks"

# Schnelle Leader-Übergabe, damit ein Wechsel Sekunden statt der Standard-TTL dauert.
SCHEDULER_ENV = {
    "SWARMQ_SCHEDULER_POLL_INTERVAL": "0.1",
    "SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL": "0.2",
    "SWARMQ_SCHEDULER_LOCK_TTL": "1.0",
}

_PROBE = (
    "import importlib.metadata as m, sys, swarmq; "
    "print(m.version('swarmq')); print(swarmq.__file__); print(sys.executable)"
)


def _run(cmd: list[str], **kwargs: object) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=False, **kwargs)  # type: ignore[call-overload]
    if result.returncode != 0:
        pytest.fail(f"{' '.join(cmd)} failed (rc={result.returncode}): {result.stderr}", pytrace=False)
    return str(result.stdout)


def _pypi_python(tmp_path_factory: pytest.TempPathFactory, version: str, constraints: Path) -> str:
    """Interpreter eines venv mit ``swarmq==version`` von PyPI, echt geprüft."""
    venv = tmp_path_factory.mktemp(f"swarmq-{version}") / "venv"
    _run(["uv", "venv", "-q", "--python", sys.executable, str(venv)])
    _run(
        [
            "uv",
            "pip",
            "install",
            "-q",
            "--python",
            str(venv / "bin" / "python"),
            f"swarmq=={version}",
            "-c",
            str(constraints),
        ]
    )
    found, location, executable = _run([str(venv / "bin" / "python"), "-c", _PROBE], cwd=str(REPO_ROOT)).split()
    assert found == version, f"compat venv runs swarmq {found}, expected {version}"
    assert not Path(location).resolve().is_relative_to(REPO_ROOT / "src"), f"compat venv imports the repo: {location}"
    return executable


@pytest.fixture(scope="session")
def python_011(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Interpreter eines venv mit swarmq 0.11.1 (``OLD_VERSION``) von PyPI."""
    return _pypi_python(tmp_path_factory, OLD_VERSION, CONSTRAINTS)


@pytest.fixture(scope="session")
def python_0110(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Interpreter eines venv mit swarmq 0.11.0 von PyPI (Rollback-Grenze, MR 8)."""
    return _pypi_python(tmp_path_factory, ROLLBACK_LIMIT_VERSION, ROLLBACK_LIMIT_CONSTRAINTS)


@pytest.fixture(scope="session")
def python_0120(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Interpreter eines venv mit swarmq 0.12.0 von PyPI (Basis der 0.12.x-Reihe, KTD19)."""
    return _pypi_python(tmp_path_factory, BASE_012_VERSION, BASE_012_CONSTRAINTS)


@pytest.fixture(scope="session")
def python_0123(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Interpreter eines venv mit swarmq 0.12.3 von PyPI (Basis für 0.12.4, Plan 2026-10-17-2103)."""
    return _pypi_python(tmp_path_factory, BASE_0123_VERSION, BASE_0123_CONSTRAINTS)


@pytest.fixture(scope="session")
def python_0131(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Interpreter eines venv mit swarmq 0.13.1 von PyPI (Basis für 0.14, Plan 2026-10-19-1500)."""
    return _pypi_python(tmp_path_factory, BASE_0131_VERSION, BASE_0131_CONSTRAINTS)


@pytest.fixture(scope="session")
def python_repo() -> str:
    """Interpreter des Arbeitsstands; muss swarmq aus ``<repo>/src`` laden."""
    _, location, executable = _run([sys.executable, "-c", _PROBE], cwd=str(REPO_ROOT)).split()
    assert Path(location).resolve().is_relative_to(REPO_ROOT / "src"), f"test env imports {location}, not the repo"
    return executable


@pytest.fixture
def queue() -> str:
    return f"compat-{uuid.uuid4().hex[:10]}"


@dataclass(frozen=True)
class Base:
    """Eine alte Basis des Mischbetriebs: Interpreter ihres venv und Version."""

    python: str
    version: str

    @property
    def runtime(self) -> str:
        return f"pypi-{self.version}"


@pytest.fixture
async def app(rabbitmq_url: str, valkey_url: str, queue: str) -> AsyncIterator[SwarmQ]:
    """App aus dem Arbeitsstand (Producer und Ergebnisse lesen); der zuerst
    gestartete Worker legt die Queue-Familie an."""
    async with SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue]) as swarm:
        yield swarm


@pytest.fixture(autouse=True)
async def _clean_state(rabbitmq_url: str, queue: str) -> AsyncIterator[None]:
    """Valkey vor und nach jedem Test leeren: Leader-Lock und Crons früherer Tests
    dürfen den Leaderwechsel nicht stören. Die Queue-Familie danach löschen,
    samt der Delay-Stufen, die 0.12-Worker beim ersten Retry anlegen (MR 10)."""
    await flush_valkey_db(valkey_db_url())
    yield
    await delete_queue_family(rabbitmq_url, queue)
    await flush_valkey_db(valkey_db_url())


@dataclass
class CompatWorker:
    """Ein ``swarmq worker``-Prozess samt mitgelesener Ausgabe."""

    proc: subprocess.Popen[str]
    runtime: str
    lines: list[str] = field(default_factory=list)

    def drain(self) -> None:
        """Ausgabe im Hintergrund lesen, sonst blockiert der Worker an der vollen Pipe."""

        def _pump() -> None:
            assert self.proc.stdout is not None
            for line in self.proc.stdout:
                self.lines.append(line)

        threading.Thread(target=_pump, daemon=True).start()

    def child_pids(self) -> set[int]:
        return {child.pid for child in worker_children(self.proc)}

    def stop(self) -> None:
        kill_subprocess(self.proc)


@pytest.fixture
def start_worker(rabbitmq_url: str, valkey_url: str, queue: str) -> Iterator[object]:
    """Fabrik: ``start_worker(python, expect, module)`` startet einen Worker und
    wartet auf ``worker_started``; der Teardown beendet alle geordnet."""
    workers: list[CompatWorker] = []

    def _start(
        python: str, expect: str, module: str = "tests.compat._compat_tasks", extra_args: list[str] | None = None
    ) -> CompatWorker:
        proc = spawn_worker(
            queue,
            fixture_module=module,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
            env_extra={"COMPAT_EXPECT_SWARMQ": expect, **SCHEDULER_ENV},
            extra_args=extra_args,
            python=python,
        )
        worker = CompatWorker(proc, "repo" if expect == "repo" else f"pypi-{expect}")
        workers.append(worker)
        wait_for_worker_started(proc)
        worker.drain()
        return worker

    yield _start
    for worker in workers:
        worker.stop()


async def consumer_count(rabbitmq_url: str, queue: str) -> int:
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        count = declared.declaration_result.consumer_count
        assert count is not None
        return count
    finally:
        await connection.close()


async def message_count(rabbitmq_url: str, queue: str) -> int:
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        count = declared.declaration_result.message_count
        assert count is not None
        return count
    finally:
        await connection.close()


async def recorded_runs(valkey_url: str, queue: str) -> list[tuple[str, str, int, str]]:
    """Alle Ausführungen als ``(name, task_id, attempt, runtime)``."""
    client = aio_valkey.Valkey.from_url(valkey_url)
    try:
        raw = await client.lrange(f"compat:runs:{queue}", 0, -1)
    finally:
        await client.aclose()
    runs = []
    for item in raw:
        name, task_id, attempt, runtime = item.decode().split("|")
        runs.append((name, task_id, int(attempt), runtime))
    return runs


async def wait_runs(
    valkey_url: str, queue: str, name: str, count: int, timeout: float = 20.0
) -> list[tuple[str, str, int, str]]:
    """Wartet, bis ``name`` mindestens ``count``-mal aufgezeichnet ist."""
    runs: list[tuple[str, str, int, str]] = []

    async def enough() -> bool:
        nonlocal runs
        runs = [run for run in await recorded_runs(valkey_url, queue) if run[0] == name]
        return len(runs) >= count

    assert await wait_until(enough, timeout=timeout), f"expected {count} runs of {name}, got {runs}"
    return runs


async def scheduler_leader(valkey_url: str) -> tuple[int, str] | None:
    """``(pid, version)`` des Leaders aus ``sq:{sched}:leader_version``."""
    client = aio_valkey.Valkey.from_url(valkey_url)
    try:
        raw = await client.get(_KEYS.sched_leader_version())
    finally:
        await client.aclose()
    if raw is None:
        return None
    owner, _, version = raw.decode().rpartition("|")
    return int(owner.split(":")[1]), version


async def wait_leader(valkey_url: str, worker: CompatWorker, timeout: float = 20.0) -> None:
    """Leader ist ein Kind von ``worker`` (PID im Owner); die gemeldete Version
    passt zur Laufzeit des Workers (``pypi-<version>`` bzw. Arbeitsstand)."""
    expected = (
        importlib.metadata.version("swarmq") if worker.runtime == "repo" else worker.runtime.removeprefix("pypi-")
    )

    async def is_leader() -> bool:
        leader = await scheduler_leader(valkey_url)
        return leader is not None and leader[1] == expected and leader[0] in worker.child_pids()

    assert await wait_until(is_leader, timeout=timeout), (
        f"{worker.runtime} never became leader (current: {await scheduler_leader(valkey_url)})"
    )


async def force_cron_fire(valkey_url: str, cron: str, timeout: float = 20.0) -> None:
    """Fire-Index des Crons auf "fällig" setzen, sobald der Leader ihn angelegt hat."""
    client = aio_valkey.Valkey.from_url(valkey_url)
    try:

        async def adopted() -> bool:
            return await client.zscore(_KEYS.sched_zset(), cron) is not None

        assert await wait_until(adopted, timeout=timeout), "leader never adopted the cron"
        await client.zadd(_KEYS.sched_zset(), {cron: int(time.time() * 1000) - 1000})
    finally:
        await client.aclose()


# Läuft im venv der Basis: nur die öffentliche API, die 0.12.0 kennt.
OLD_PRODUCER = """
import asyncio, importlib.metadata, json, os.path, sys
import swarmq
from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client

async def main(broker_url, backend_url, queue, count):
    out = {"version": importlib.metadata.version("swarmq"), "location": os.path.realpath(swarmq.__file__), "results": {}}
    async with SwarmQ(RabbitMQBroker(broker_url), ValkeyBackend(backend_url), queues=[queue]) as app:
        producer = Client(app.broker, app.backend)
        task_ids = [await producer.schedule("compat_flaky", queue=queue) for _ in range(count)]
        for task_id in task_ids:
            out["results"][task_id] = await app.get_result(task_id, timeout=30)
    print(json.dumps(out))

asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])))
"""

#: Wartezeit auf Ereignisse im Mischbetrieb (Consumer, Ergebnisse, Delay-Stufe).
EVENT_TIMEOUT = 20.0


async def wait_consumers(rabbitmq_url: str, queue: str, count: int) -> None:
    async def reached() -> bool:
        return await consumer_count(rabbitmq_url, queue) == count

    assert await wait_until(reached, timeout=EVENT_TIMEOUT), f"consumer count on {queue} never reached {count}"


def assert_two_attempts_each(runs: list[tuple[str, str, int, str]], task_ids: list[str], runtime: str) -> None:
    attempts = Counter(task_id for _, task_id, _, _ in runs)
    assert attempts == dict.fromkeys(task_ids, 2), "every message runs exactly twice (fail, retry)"
    assert {run_runtime for *_, run_runtime in runs} == {runtime}


async def wait_deferred_in_tier(rabbitmq_url: str, tier: str) -> None:
    async def deferred_in_tier() -> bool:
        try:
            return await message_count(rabbitmq_url, tier) == 1
        except aio_pika.exceptions.ChannelNotFoundEntity:
            return False

    assert await wait_until(deferred_in_tier, timeout=EVENT_TIMEOUT), "retry never reached the delay tier"


async def allow_poison(valkey_url: str, queue: str) -> None:
    """``compat_poison`` gelingt ab jetzt (``compat:flag:<queue>``)."""
    client = aio_valkey.Valkey.from_url(valkey_url)
    try:
        await client.set(f"compat:flag:{queue}", "1")
    finally:
        await client.aclose()


async def shovel(rabbitmq_url: str, source: str, target: str) -> None:
    """Eine Nachricht mit allen Headern von *source* nach *target* legen --
    wie "Move messages" in der RabbitMQ-UI bzw. ein Shovel."""
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        message = await (await channel.get_queue(source)).get(no_ack=True)
        assert message is not None, f"no message in {source}"
        await channel.default_exchange.publish(
            aio_pika.Message(message.body, headers=dict(message.headers or {})), routing_key=target
        )
    finally:
        await connection.close()


async def result_is(app: SwarmQ, task_id: str, expected: str) -> bool:
    try:
        result: object = await app.get_result(task_id, timeout=1)
    except TaskError:  # Fehlerergebnis des Dead-Letterns, noch nicht ueberschrieben
        return False
    return result == expected
