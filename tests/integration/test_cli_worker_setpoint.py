"""Kontrollkanal am echten Backend (Plan 2026-09-16, U6/U7).

Startet ``swarmq worker --max-processes`` als Subprozess gegen RabbitMQ und
Valkey, schreibt einen Sollwert in ``sq:control:setpoint:<gruppe>`` und liest
die Rueckmeldung unter ``sq:control:status:<gruppe>:<supervisor>``.

Gewartet wird auf Anzeichen in der Rueckmeldung, nie eine feste Zeitspanne
(docs/solutions/logic-errors/scheduler-cron-sync-clobber-test-flake-2026-06-01.md).

Test-Liste:
- Ein zur Laufzeit geschriebener Sollwert wirkt innerhalb eines Abtastintervalls (U6).
- Ein entfernter Sollwert stellt die Startargumente wieder her (AE9).
- Nach dem Ende des Supervisors verschwindet seine Rueckmeldung von selbst (U7).
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

from swarmq.backend.valkey.control import ValkeySetpointStore
from swarmq.contracts.control import Setpoint
from swarmq.process_manager.setpoint_store import DEFAULT_SAMPLE_INTERVAL_S
from tests.support.cli_process import terminate_worker_process
from tests.support.services import missing_services, rabbitmq_amqp_url, valkey_db_url

REPO_ROOT = Path(__file__).resolve().parents[2]

pytestmark = [
    pytest.mark.integration,
    pytest.mark.timeout(150),
    pytest.mark.skipif(bool(missing_services()), reason="RabbitMQ/Valkey nicht erreichbar"),
]

_WORKER_ID = "ctl-itest"
_GRACE_S = 2


def _spawn(queue: str, group: str) -> subprocess.Popen[str]:
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = rabbitmq_amqp_url()
    env["SWARMQ_BACKEND_URL"] = valkey_db_url()
    env["CLI_TEST_QUEUE"] = queue
    env["PYTHONUNBUFFERED"] = "1"
    env["SWARMQ_WORKER_ID"] = _WORKER_ID  # macht die Supervisor-Kennung vorhersagbar
    env["SWARMQ_SHUTDOWN_TIMEOUT"] = str(_GRACE_S)  # kurze Gnadenfrist -> kurzes Verfallsdatum
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "swarmq.cli",
            "worker",
            "--module",
            "tests.integration._cli_fixture_tasks",
            "--queues",
            queue,
            "--concurrency",
            "2",
            "--processes",
            "1",
            "--max-processes",
            "4",
            "--group",
            group,
            "--log-format",
            "json",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )


def _drain(proc: subprocess.Popen[str], sink: list[str]) -> threading.Thread:
    """stdout leeren, damit der Supervisor nicht an einer vollen Pipe haengt."""

    def run() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            sink.append(line)  # noqa: PERF402 - jede Zeile muss sofort sichtbar sein, waehrend der Test parallel auf sie wartet

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


async def _with_store(fn: Callable[[ValkeySetpointStore], object]) -> object:
    store = ValkeySetpointStore(valkey_db_url())
    await store.connect()
    try:
        return await fn(store)  # type: ignore[misc]
    finally:
        await store.close()


def _read_status(group: str, supervisor: str) -> dict | None:
    return asyncio.run(_with_store(lambda s: s.read_status(group, supervisor)))  # type: ignore[return-value]


def _wait_for_status(
    group: str, supervisor: str, predicate: Callable[[dict | None], bool], *, timeout_s: float, output: list[str]
) -> dict | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        status = _read_status(group, supervisor)
        if predicate(status):
            return status
        time.sleep(0.2)
    pytest.fail(
        f"status did not reach the expected state within {timeout_s}s; last={status!r}\n" + "".join(output[-40:])
    )


def test_runtime_setpoint_applies_reverts_and_the_status_expires_after_exit() -> None:
    queue = f"ctl_{uuid.uuid4().hex[:8]}"
    group = f"grp-{uuid.uuid4().hex[:8]}"
    proc = _spawn(queue, group)
    supervisor = f"{_WORKER_ID}:p{proc.pid}"
    output: list[str] = []
    _drain(proc, output)
    try:
        started = _wait_for_status(
            group,
            supervisor,
            lambda s: s is not None and s["slots"]["occupied"] == 1,
            timeout_s=40,
            output=output,
        )
        assert started is not None
        assert started["effective"] == {"min": 1, "max": 1}
        assert started["source"] == "startup"
        assert started["store"] == "absent"
        assert started["setpoint"] is None
        assert started["headroom"] == 4
        assert started["group"] == group

        # U6: Sollwert 3..3 wirkt innerhalb eines Abtastintervalls (plus Startzeit der Kinder).
        asyncio.run(_with_store(lambda s: s.write_setpoint(group, Setpoint(3, 3))))
        written_at = time.monotonic()
        applied = _wait_for_status(
            group,
            supervisor,
            lambda s: s is not None and s["effective"] == {"min": 3, "max": 3} and s["slots"]["occupied"] == 3,
            timeout_s=30,
            output=output,
        )
        assert applied is not None
        assert applied["source"] == "setpoint"
        assert applied["store"] == "present"
        assert time.monotonic() - written_at < 3 * DEFAULT_SAMPLE_INTERVAL_S

        # AE9: entfernter Sollwert -> Startargumente; die zwei ueberzaehligen Worker enden sanft.
        asyncio.run(_with_store(lambda s: s.clear_setpoint(group)))
        reverted = _wait_for_status(
            group,
            supervisor,
            lambda s: s is not None and s["source"] == "startup" and s["slots"]["occupied"] == 1,
            timeout_s=40,
            output=output,
        )
        assert reverted is not None
        assert reverted["effective"] == {"min": 1, "max": 1}
        assert reverted["store"] == "absent"

        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=40)
        assert rc == 0, "".join(output[-40:])

        # U7: die Rueckmeldung verfaellt von selbst -- Gnadenfrist + Terminierungsfrist + ein Intervall.
        _wait_for_status(
            group,
            supervisor,
            lambda s: s is None,
            timeout_s=_GRACE_S + 5.0 + DEFAULT_SAMPLE_INTERVAL_S + 15.0,
            output=output,
        )
    finally:
        terminate_worker_process(proc)
