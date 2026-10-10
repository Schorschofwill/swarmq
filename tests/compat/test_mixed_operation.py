"""Mischbetrieb 0.11.1 / 0.12 (Plan 0.12.0, Q2): Grundszenarien, Basislinie gegen den Arbeitsstand.

Seit MR 8 ist 0.11.1 die Basis (Rollout erst auf 0.11.1, Plan Abschnitt 1).
Worker aus swarmq 0.11.1 (PyPI, isoliertes venv) und aus dem Arbeitsstand
teilen sich Queue, Valkey und Scheduler-Leader. Erwartet wird neues oder
unverändertes altes Verhalten; Verlust oder Doppelausführung nur, wo
Abschnitt 7 des Plans es nennt (heute: nirgends in diesen Szenarien).

Die späteren MRs ergänzen weitere Szenarien (Oneshot, Lock-Konflikt,
unbekannter Task, Replay in-flight, Rollback, Namensraum, zwei Apps).

Test-Liste:
- [x] 0.11- und 0.12-Worker auf derselben Queue mit Retry: jede Nachricht genau
      zwei Versuche, Ergebnis über den 0.12-Client lesbar, beide Versionen arbeiten
- [x] Cron-Fire mit Leaderwechsel 0.11 -> 0.12: je erzwungenem Fire genau ein Lauf
- [x] Cron-Fire mit Leaderwechsel 0.12 -> 0.11: dito
- [x] DLQ-Replay über die 0.12-CLI an einen 0.11-Worker: der Task läuft erneut und gelingt
"""

from __future__ import annotations

import collections
import importlib.metadata
import json
import os
import subprocess
import time
from collections.abc import AsyncIterator, Callable

import pytest
import valkey.asyncio as aio_valkey

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from swarmq.keyspace import KeySpace
from tests.compat._harness import (
    OLD_VERSION,
    REPO_ROOT,
    CompatWorker,
    consumer_count,
    message_count,
    recorded_runs,
)
from tests.support.wait import wait_until

pytestmark = [pytest.mark.compat, pytest.mark.timeout(90)]

_KEYS = KeySpace(None)
_CRON = "compat_cron"
_CRON_MODULE = "tests.compat._compat_cron_tasks"
_EVENT_TIMEOUT = 20.0

StartWorker = Callable[..., CompatWorker]


@pytest.fixture
async def app(rabbitmq_url: str, valkey_url: str, queue: str) -> AsyncIterator[SwarmQ]:
    """App aus dem Arbeitsstand (Ergebnisse lesen); deklariert die Queue-Familie vorab."""
    swarm = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue])
    async with swarm:
        await swarm.broker.declare_queue(queue)
        yield swarm


@pytest.fixture
def producer(app: SwarmQ) -> Client:
    """Producer ohne Registry: die Tasks leben nur in den Worker-Prozessen."""
    return Client(app.broker, app.backend)


async def _wait_consumers(rabbitmq_url: str, queue: str, count: int) -> None:
    async def ready() -> bool:
        return await consumer_count(rabbitmq_url, queue) >= count

    assert await wait_until(ready, timeout=_EVENT_TIMEOUT), f"{count} consumers never attached to {queue}"


async def _wait_runs(valkey_url: str, queue: str, name: str, count: int) -> list[tuple[str, str, int, str]]:
    runs: list[tuple[str, str, int, str]] = []

    async def enough() -> bool:
        nonlocal runs
        runs = [run for run in await recorded_runs(valkey_url, queue) if run[0] == name]
        return len(runs) >= count

    assert await wait_until(enough, timeout=_EVENT_TIMEOUT), f"expected {count} runs of {name}, got {runs}"
    return runs


# ---------------------------------------------------------------------------
# Retry über beide Versionen
# ---------------------------------------------------------------------------


async def test_retry_between_old_and_new_worker_on_one_queue(
    start_worker: StartWorker,
    python_011: str,
    python_repo: str,
    app: SwarmQ,
    producer: Client,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    start_worker(python_011, OLD_VERSION)
    start_worker(python_repo, "repo")
    await _wait_consumers(rabbitmq_url, queue, 2)

    task_ids = [await producer.schedule("compat_flaky", queue=queue) for _ in range(20)]
    results = {task_id: await app.get_result(task_id, timeout=_EVENT_TIMEOUT) for task_id in task_ids}

    runs = await recorded_runs(valkey_url, queue)
    attempts = collections.Counter(task_id for _, task_id, _, _ in runs)
    assert attempts == dict.fromkeys(task_ids, 2), "every message runs exactly twice (fail, retry)"
    assert set(results.values()) <= {"repo", f"pypi-{OLD_VERSION}"}
    assert {runtime for *_, runtime in runs} == {"repo", f"pypi-{OLD_VERSION}"}, "both versions consumed"
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


# ---------------------------------------------------------------------------
# Cron-Fire mit Leaderwechsel
# ---------------------------------------------------------------------------


async def _leader(valkey_url: str) -> tuple[int, str] | None:
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


async def _wait_leader(valkey_url: str, worker: CompatWorker) -> None:
    """Leader ist ein Kind von ``worker`` (PID im Owner); die gemeldete Version
    passt zur Laufzeit des Workers (0.11.1 bzw. Arbeitsstand)."""
    expected = importlib.metadata.version("swarmq") if worker.runtime == "repo" else OLD_VERSION

    async def is_leader() -> bool:
        leader = await _leader(valkey_url)
        return leader is not None and leader[1] == expected and leader[0] in worker.child_pids()

    assert await wait_until(is_leader, timeout=_EVENT_TIMEOUT), (
        f"{worker.runtime} never became leader (current: {await _leader(valkey_url)})"
    )


async def _force_fire(valkey_url: str) -> None:
    """Fire-Index des Crons auf "fällig" setzen, sobald der Leader ihn angelegt hat."""
    client = aio_valkey.Valkey.from_url(valkey_url)
    try:

        async def adopted() -> bool:
            return await client.zscore(_KEYS.sched_zset(), _CRON) is not None

        assert await wait_until(adopted, timeout=_EVENT_TIMEOUT), "leader never adopted the cron"
        await client.zadd(_KEYS.sched_zset(), {_CRON: int(time.time() * 1000) - 1000})
    finally:
        await client.aclose()


@pytest.mark.parametrize(("first", "second"), [("old", "new"), ("new", "old")])
async def test_cron_fire_survives_leader_change(
    start_worker: StartWorker,
    python_011: str,
    python_repo: str,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
    first: str,
    second: str,
) -> None:
    spec = {"old": (python_011, OLD_VERSION), "new": (python_repo, "repo")}

    leader = start_worker(*spec[first], module=_CRON_MODULE)
    await _wait_leader(valkey_url, leader)
    follower = start_worker(*spec[second], module=_CRON_MODULE)
    await _wait_consumers(rabbitmq_url, queue, 2)

    await _force_fire(valkey_url)
    await _wait_runs(valkey_url, queue, _CRON, 1)

    leader.stop()
    await _wait_leader(valkey_url, follower)
    await _force_fire(valkey_url)
    runs = await _wait_runs(valkey_url, queue, _CRON, 2)

    follower.stop()
    runs = [run for run in await recorded_runs(valkey_url, queue) if run[0] == _CRON]
    assert len(runs) == 2, f"each forced fire runs exactly once: {runs}"
    assert len({task_id for _, task_id, _, _ in runs}) == 2
    assert await message_count(rabbitmq_url, queue) == 0, "no duplicate fire left in the queue"


# ---------------------------------------------------------------------------
# DLQ-Replay über die 0.12-CLI an einen 0.11-Worker
# ---------------------------------------------------------------------------


async def test_dlq_replay_by_new_cli_runs_on_old_worker(
    start_worker: StartWorker,
    python_011: str,
    python_repo: str,
    app: SwarmQ,
    producer: Client,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    start_worker(python_011, OLD_VERSION)
    await _wait_consumers(rabbitmq_url, queue, 1)

    task_id = await producer.schedule("compat_poison", queue=queue)

    async def dead_lettered() -> bool:
        return await message_count(rabbitmq_url, f"{queue}.XQ") == 1

    assert await wait_until(dead_lettered, timeout=_EVENT_TIMEOUT), "poison message never reached the DLQ"

    client = aio_valkey.Valkey.from_url(valkey_url)
    try:
        await client.set(f"compat:flag:{queue}", "1")
    finally:
        await client.aclose()

    env = {**os.environ, "SWARMQ_BROKER_URL": rabbitmq_url, "SWARMQ_BACKEND_URL": valkey_url}
    cli = subprocess.run(
        [python_repo, "-m", "swarmq.cli", "dlq", "retry", "--all", "--queue", queue, "--format", "json"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert cli.returncode == 0, f"dlq retry failed: {cli.stdout!r} {cli.stderr!r}"
    assert json.loads(cli.stdout)

    runs = await _wait_runs(valkey_url, queue, "compat_poison", 2)
    assert [(tid, attempt, runtime) for _, tid, attempt, runtime in runs] == [
        (task_id, 1, f"pypi-{OLD_VERSION}"),
        (task_id, 2, f"pypi-{OLD_VERSION}"),
    ]
    assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT) == f"pypi-{OLD_VERSION}"
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0
