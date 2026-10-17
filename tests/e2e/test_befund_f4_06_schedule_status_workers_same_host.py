"""Nachweis F4-06: ``swarmq schedule status`` verschweigt Worker mehrerer
Supervisoren eines Hosts.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-06; Verdacht
R4B-03; Gegenprüfung ``6-gegenpruefung-B.md``; Probe U15 6c,
reproduziert in U17).

Der Scheduler-Loop meldet seine Version unter ``config.worker_identity``
(``<SWARMQ_WORKER_ID oder Hostname>:<Slot>``) in
``sq:{sched}:worker_version:<worker_identity>``. Zwei Supervisoren auf einem
Host ohne eigenes ``SWARMQ_WORKER_ID`` haben dieselben Slots und überschreiben
sich gegenseitig: ``WORKERS`` zeigt je Slot nur den zuletzt Schreibenden. Im
Mischbetrieb (Rolling Deploy) kann so eine App unter der Mindestversion des
Leaders unsichtbar bleiben.

Erwartet: ``cli.md:377-380`` („``WORKERS`` lists every worker whose report is
live … Every version shown here should be at least the minimum leader
version“); gleiche Regel im Kontrollkanal, der Supervisoren eines Hosts über
``:p<pid>`` trennt (``cli.md:247-249``). ``SWARMQ_WORKER_ID`` ist nicht als
Pflichtwert je Supervisor dokumentiert (``configuration.md:139, 224-242``).

Zwei Supervisoren mit ``--processes 1`` ohne ``SWARMQ_WORKER_ID`` in einem
eigenen Namensraum, Meldetakt 2 s; ``schedule status --format json`` als
Unterprozess.

Testliste:
# [x] zwei Supervisoren, je ein Kind, kein SWARMQ_WORKER_ID -> zwei Einträge unter workers
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.support.services import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    VALKEY_HOST,
    VALKEY_PORT,
    rabbitmq_amqp_url,
    valkey_db_url,
)
from tests.support.wait import wait_until
from tests.support.worker_subprocess import (
    kill_subprocess,
    port_open,
    spawn_worker,
    wait_for_worker_started,
)

_BROKER_URL = rabbitmq_amqp_url()
_BACKEND_URL = valkey_db_url()
_FIXTURE_MODULE = "tests.e2e._shutdown_fixture_tasks"
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SEEN_INTERVAL_S = 2

pytestmark = [
    pytest.mark.timeout(90),
    pytest.mark.skipif(
        not (port_open(RABBITMQ_HOST, RABBITMQ_PORT) and port_open(VALKEY_HOST, VALKEY_PORT)),
        reason="Docker services (RabbitMQ + Valkey) not reachable",
    ),
]


@pytest.fixture
def namespace() -> str:
    """Eigener Namensraum: Versionsmeldungen anderer e2e-Worker zählen nicht mit."""
    return f"f406-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def supervisors(namespace: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[list[subprocess.Popen[str]]]:
    monkeypatch.delenv("SWARMQ_WORKER_ID", raising=False)  # spawn_worker erbt die Umgebung
    queue = f"f406-{uuid.uuid4().hex[:8]}"
    env = {
        "SWARMQ_NAMESPACE": namespace,
        "SWARMQ_SCHEDULER_SEEN_INTERVAL_S": str(_SEEN_INTERVAL_S),
    }
    procs: list[subprocess.Popen[str]] = []
    try:
        for _ in range(2):
            proc = spawn_worker(
                queue,
                fixture_module=_FIXTURE_MODULE,
                broker_url=_BROKER_URL,
                backend_url=_BACKEND_URL,
                concurrency=1,
                env_extra=env,
                extra_args=["--processes", "1"],
            )
            procs.append(proc)
        for proc in procs:
            wait_for_worker_started(proc, timeout=60)
        yield procs
    finally:
        for proc in procs:
            kill_subprocess(proc)


def _workers(namespace: str) -> list[dict[str, Any]]:
    env = {**os.environ, "SWARMQ_BACKEND_URL": _BACKEND_URL, "SWARMQ_NAMESPACE": namespace}
    result = subprocess.run(
        [sys.executable, "-m", "swarmq.cli", "schedule", "status", "--format", "json"],
        cwd=str(_REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    return list(json.loads(result.stdout)["workers"])


@pytest.mark.befund("F4-06")
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="F4-06: offen - Versionsmeldungen zweier Supervisoren eines Hosts ueberschreiben sich",
)
async def test_schedule_status_lists_the_workers_of_two_supervisors_on_one_host(
    supervisors: list[subprocess.Popen[str]], namespace: str
) -> None:
    assert all(proc.poll() is None for proc in supervisors)
    seen: list[dict[str, Any]] = []

    def two_reported() -> bool:
        seen[:] = _workers(namespace)
        return len(seen) >= 2

    # Drei Meldetakte nach dem Start: beide Kinder haben gemeldet.
    await wait_until(two_reported, timeout=3 * _SEEN_INTERVAL_S + 6, interval=1.0)

    assert len(seen) == 2, f"zwei lebende Worker, WORKERS zeigt {seen} (cli.md: 'every worker whose report is live')"
