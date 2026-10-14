"""Mischbetrieb 0.12.0 / Arbeitsstand (Plan 2026-10-14-0748, U2, R27, KTD19).

Zweite Basis neben 0.11.1: swarmq 0.12.0 von PyPI (isoliertes venv,
``constraints-0.12.0.txt``). Die Fixes der 0.12.x-Reihe müssen in beiden
Richtungen neben 0.12.0-Workern und -Producern laufen (Rolling Deploy,
Rollback). Fixes mit wandernden Daten (Header, Lua, Definitionen) ergänzen
hier ihre eigenen Fälle (U5, U11, U13, U16).

Test-Liste:
- [x] 0.12.0-Producer reiht ein, Worker aus dem Arbeitsstand führt aus (mit
      Retry); das Ergebnis ist über den 0.12.0-Client und den neuen Client lesbar
- [x] Umgekehrt: Producer aus dem Arbeitsstand, 0.12.0-Worker führt aus (mit
      Retry); Ergebnis über den neuen Client lesbar
- [x] Rollback: der Arbeitsstand stellt einen Retry in ``{queue}.DQ.15s`` zurück
      und stoppt; 0.12.0 allein führt ihn nach Ablauf aus, nichts doppelt
"""

from __future__ import annotations

import asyncio
import collections
import json
import subprocess
import tempfile
from collections.abc import AsyncIterator, Callable

import aio_pika
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from tests.compat._harness import (
    BASE_012_VERSION,
    CompatWorker,
    consumer_count,
    message_count,
    recorded_runs,
)
from tests.support.wait import wait_until

pytestmark = [pytest.mark.compat, pytest.mark.timeout(120)]

StartWorker = Callable[..., CompatWorker]

_MESSAGES = 5
_EVENT_TIMEOUT = 20.0
_OLD_RUNTIME = f"pypi-{BASE_012_VERSION}"

# Läuft im 0.12.0-venv: nur die öffentliche API, die 0.12.0 kennt.
_OLD_PRODUCER = """
import asyncio, importlib.metadata, json, sys
from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client

async def main(broker_url, backend_url, queue, count):
    out = {"version": importlib.metadata.version("swarmq"), "results": {}}
    async with SwarmQ(RabbitMQBroker(broker_url), ValkeyBackend(backend_url), queues=[queue]) as app:
        producer = Client(app.broker, app.backend)
        task_ids = [await producer.schedule("compat_flaky", queue=queue) for _ in range(count)]
        for task_id in task_ids:
            out["results"][task_id] = await app.get_result(task_id, timeout=30)
    print(json.dumps(out))

asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])))
"""


@pytest.fixture
async def app(rabbitmq_url: str, valkey_url: str, queue: str) -> AsyncIterator[SwarmQ]:
    """App aus dem Arbeitsstand (Producer und Ergebnisse lesen); der zuerst
    gestartete Worker legt die Queue-Familie an."""
    async with SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue]) as swarm:
        yield swarm


async def _wait_consumers(rabbitmq_url: str, queue: str, count: int) -> None:
    async def reached() -> bool:
        return await consumer_count(rabbitmq_url, queue) == count

    assert await wait_until(reached, timeout=_EVENT_TIMEOUT), f"consumer count on {queue} never reached {count}"


def _assert_two_attempts_each(runs: list[tuple[str, str, int, str]], task_ids: list[str], runtime: str) -> None:
    attempts = collections.Counter(task_id for _, task_id, _, _ in runs)
    assert attempts == dict.fromkeys(task_ids, 2), "every message runs exactly twice (fail, retry)"
    assert {run_runtime for *_, run_runtime in runs} == {runtime}


async def test_old_producer_new_worker(
    start_worker: StartWorker,
    python_0120: str,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    start_worker(python_repo, "repo")
    await _wait_consumers(rabbitmq_url, queue, 1)

    with tempfile.TemporaryDirectory() as cwd:  # nie das Repo im Pfad des 0.12.0-venv
        result = await asyncio.to_thread(
            subprocess.run,
            [python_0120, "-c", _OLD_PRODUCER, rabbitmq_url, valkey_url, queue, str(_MESSAGES)],
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
            cwd=cwd,
        )
    assert result.returncode == 0, result.stderr[-2000:]
    old = json.loads(result.stdout.strip().splitlines()[-1])
    assert old["version"] == BASE_012_VERSION
    task_ids = list(old["results"])
    assert len(task_ids) == _MESSAGES
    assert set(old["results"].values()) == {"repo"}, "the 0.12.0 client reads the new worker's results"
    for task_id in task_ids:
        assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT) == "repo"

    _assert_two_attempts_each(await recorded_runs(valkey_url, queue), task_ids, "repo")
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


async def test_new_producer_old_worker(
    start_worker: StartWorker,
    python_0120: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    start_worker(python_0120, BASE_012_VERSION)
    await _wait_consumers(rabbitmq_url, queue, 1)

    producer = Client(app.broker, app.backend)
    task_ids = [await producer.schedule("compat_flaky", queue=queue) for _ in range(_MESSAGES)]
    results = {task_id: await app.get_result(task_id, timeout=_EVENT_TIMEOUT) for task_id in task_ids}

    assert set(results.values()) == {_OLD_RUNTIME}
    _assert_two_attempts_each(await recorded_runs(valkey_url, queue), task_ids, _OLD_RUNTIME)
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


async def test_rollback_runs_a_deferred_retry_on_0_12_0(
    start_worker: StartWorker,
    python_0120: str,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Rollback: eine vom Arbeitsstand in eine Delay-Stufe zurückgestellte
    Nachricht läuft nach Ablauf auf 0.12.0, ohne Fehler und ohne Doppellauf."""
    tier = f"{queue}.DQ.15s"

    async def deferred_in_tier() -> bool:
        try:
            return await message_count(rabbitmq_url, tier) == 1
        except aio_pika.exceptions.ChannelNotFoundEntity:
            return False

    new = start_worker(python_repo, "repo")
    await _wait_consumers(rabbitmq_url, queue, 1)
    task_id = await Client(app.broker, app.backend).schedule("compat_slow_retry", queue=queue)
    assert await wait_until(deferred_in_tier, timeout=_EVENT_TIMEOUT), "retry never reached the delay tier"
    new.stop()
    await _wait_consumers(rabbitmq_url, queue, 0)
    start_worker(python_0120, BASE_012_VERSION)

    assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT + 10) == _OLD_RUNTIME

    runs = [(tid, attempt, runtime) for _, tid, attempt, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, 1, "repo"), (task_id, 2, _OLD_RUNTIME)]
    assert await message_count(rabbitmq_url, tier) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0
