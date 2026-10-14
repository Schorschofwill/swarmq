"""Mischbetrieb-Suite (Q2): Fixtures und Helfer für Worker aus swarmq 0.11.x, 0.12.0 und dem Arbeitsstand.

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

Opt-in über den Marker ``compat`` (braucht Netz zu PyPI beim ersten Lauf).
"""

from __future__ import annotations

import subprocess
import sys
import threading
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import aio_pika
import pytest
import valkey.asyncio as aio_valkey

from tests.support.queues import delete_queue_family
from tests.support.services import flush_valkey_db, valkey_db_url
from tests.support.worker_subprocess import kill_subprocess, spawn_worker, wait_for_worker_started, worker_children

REPO_ROOT = Path(__file__).resolve().parents[2]
CONSTRAINTS = Path(__file__).with_name("constraints-0.11.1.txt")
OLD_VERSION = "0.11.1"
#: Rollback-Grenze (MR 8): 0.11.0 kann von 0.12 angelegte Queues nicht deklarieren.
ROLLBACK_LIMIT_VERSION = "0.11.0"
ROLLBACK_LIMIT_CONSTRAINTS = Path(__file__).with_name("constraints-0.11.0.txt")
#: Basis der 0.12.x-Reihe (KTD19): Rolling Deploy und Rollback neben 0.12.0.
BASE_012_VERSION = "0.12.0"
BASE_012_CONSTRAINTS = Path(__file__).with_name("constraints-0.12.0.txt")

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
def python_repo() -> str:
    """Interpreter des Arbeitsstands; muss swarmq aus ``<repo>/src`` laden."""
    _, location, executable = _run([sys.executable, "-c", _PROBE], cwd=str(REPO_ROOT)).split()
    assert Path(location).resolve().is_relative_to(REPO_ROOT / "src"), f"test env imports {location}, not the repo"
    return executable


@pytest.fixture
def queue() -> str:
    return f"compat-{uuid.uuid4().hex[:10]}"


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

    def _start(python: str, expect: str, module: str = "tests.compat._compat_tasks") -> CompatWorker:
        proc = spawn_worker(
            queue,
            fixture_module=module,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
            env_extra={"COMPAT_EXPECT_SWARMQ": expect, **SCHEDULER_ENV},
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
