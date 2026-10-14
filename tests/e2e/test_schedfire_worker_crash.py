"""E2E: SIGKILL eines Workers mitten in einem Cron-Fire (FS-03, E-046, Plan U5).

Worker A gewinnt den In-Flight-Claim eines Fires (``x-swarmq-schedfire``) und
wird mitten im Lauf per SIGKILL beendet (Kind und Supervisor). Der Broker
stellt die Nachricht Worker B neu zu; der Claim gehoert noch dem toten A.
Frueher bestaetigte B sie als Duplikat -- das Fire war still verloren. Jetzt
stellt B sie zurueck, bis der Claim abgelaufen ist, und fuehrt sie genau
einmal aus.

``scheduler_inflight_ttl`` und ``execution_guard_ttl_s`` sind auf wenige
Sekunden skaliert. Die Nachricht wird wie vom Leader veroeffentlicht
(Cron-Fire-Header), damit der Test nicht am Minutentakt des Crons haengt.

Test-Liste:
- [x] SIGKILL mitten im Fire -> nach Claim-Ablauf genau ein vollstaendiger
      Lauf auf B, Ergebnis gespeichert, Completed-Marker gesetzt, kein
      weiterer Lauf
"""

from __future__ import annotations

import signal
import subprocess
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.serialization import encode_json
from swarmq.task import Task
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
    crash_worker,
    ensure_fixture_registered,
    kill_subprocess,
    port_open,
    spawn_worker,
    wait_for_worker_started,
)

_BROKER_URL = rabbitmq_amqp_url()
_BACKEND_URL = valkey_db_url()
_FIXTURE_MODULE = "tests.e2e._shutdown_fixture_tasks"

# Claim lebt 4 s, Running-Marker des Ausfuehrungsschutzes 2 s, Zurueck-
# stellungen 0,5 s ... 2 s: B stellt mehrfach zurueck, bevor der Claim ablaeuft.
_ENV = {
    "SWARMQ_SCHEDULER_INFLIGHT_TTL": "4",
    "SWARMQ_EXECUTION_GUARD_TTL_S": "2",
    "SWARMQ_BACKEND_DEFER_BASE_DELAY": "0.5",
    "SWARMQ_BACKEND_DEFER_MAX_DELAY": "2",
}

pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.skipif(
        not (port_open(RABBITMQ_HOST, RABBITMQ_PORT) and port_open(VALKEY_HOST, VALKEY_PORT)),
        reason="Docker services (RabbitMQ + Valkey) not reachable",
    ),
]


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


def _spawn(queue: str) -> subprocess.Popen[str]:
    return spawn_worker(
        queue,
        fixture_module=_FIXTURE_MODULE,
        broker_url=_BROKER_URL,
        backend_url=_BACKEND_URL,
        concurrency=2,
        env_extra=_ENV,
    )


async def _exec_count(backend: ValkeyBackend, task_id: str) -> int:
    raw = await backend.client.get(f"exec_count:{task_id}")
    return int(raw) if raw is not None else 0


@pytest.mark.entscheidung("E-046")
@pytest.mark.usefixtures("_cleanup_tasks")
async def test_sigkill_mid_cron_fire_runs_the_fire_exactly_once_after_the_claim_expired() -> None:
    queue = f"e2e-schedfire-crash-{uuid.uuid4().hex[:10]}"
    ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)
    fire_key = f"shutdown_sleep:{uuid.uuid4().hex[:8]}:1700000000000"
    task_id = uuid.uuid4().hex

    proc_a = _spawn(queue)
    proc_b: subprocess.Popen[str] | None = None
    try:
        wait_for_worker_started(proc_a)
        backend = ValkeyBackend(_BACKEND_URL)
        app = SwarmQ(RabbitMQBroker(_BROKER_URL), backend, queues=[queue])
        async with app:
            body = encode_json(
                {
                    "task": {"id": task_id, "queue": queue, "name": "shutdown_sleep"},
                    "args": [3.0],
                    "kwargs": {},
                }
            )
            await app.broker.publish(queue, body, headers={"x-swarmq-schedfire": fire_key})

            async def _started() -> bool:
                return await _exec_count(backend, task_id) >= 1

            assert await wait_until(_started, timeout=10), f"exec_count:{task_id} did not reach 1 within 10s"

            # Harter Absturz mitten im Lauf: kein Ack, keine Claim-Freigabe.
            crash_worker(proc_a)
            assert proc_a.returncode == -signal.SIGKILL

            proc_b = _spawn(queue)
            wait_for_worker_started(proc_b)

            [result] = await app.get_results([task_id], timeout=30)
            assert result == "slept:3.0"
            assert await backend.schedules.is_fire_completed(fire_key) is True
            # Ein Lauf auf A (abgebrochen), genau einer auf B.
            assert await _exec_count(backend, task_id) == 2

            proc_b.send_signal(signal.SIGTERM)
            assert proc_b.wait(timeout=30) == 0
        # Nach dem geordneten Ende von B laeuft nichts mehr nach.
        assert await _exec_count_after_close(task_id) == 2
    finally:
        kill_subprocess(proc_a)
        if proc_b is not None:
            kill_subprocess(proc_b)


async def _exec_count_after_close(task_id: str) -> int:
    backend = ValkeyBackend(_BACKEND_URL)
    await backend.connect()
    try:
        return await _exec_count(backend, task_id)
    finally:
        await backend.close()
