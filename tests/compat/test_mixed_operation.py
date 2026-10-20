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
      zwei Versuche, Ergebnis über den 0.12-Client lesbar, beide Versionen arbeiten;
      in beiden Startreihenfolgen, die erste Version legt die Queue an (Review MR 8, P2-6)
- [x] Cron-Fire mit Leaderwechsel 0.11 -> 0.12: je erzwungenem Fire genau ein Lauf
- [x] Cron-Fire mit Leaderwechsel 0.12 -> 0.11: dito
- [x] DLQ-Replay über die 0.12-CLI an einen 0.11-Worker: der Task läuft erneut und gelingt
"""

from __future__ import annotations

import asyncio
import collections
import json
import os
import subprocess
from collections.abc import AsyncIterator, Callable

import pytest
import valkey.asyncio as aio_valkey

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from tests.compat._harness import (
    OLD_VERSION,
    REPO_ROOT,
    CompatWorker,
    consumer_count,
    force_cron_fire,
    message_count,
    recorded_runs,
    wait_leader,
    wait_runs,
)
from tests.support.wait import wait_until

pytestmark = [pytest.mark.compat, pytest.mark.timeout(90)]

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
async def undeclared_app(rabbitmq_url: str, valkey_url: str, queue: str) -> AsyncIterator[SwarmQ]:
    """App aus dem Arbeitsstand ohne Vorab-Deklaration: der zuerst gestartete
    Worker legt die Queue-Familie an."""
    swarm = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue])
    async with swarm:
        yield swarm


@pytest.fixture
def producer(app: SwarmQ) -> Client:
    """Producer ohne Registry: die Tasks leben nur in den Worker-Prozessen."""
    return Client(app.broker, app.backend)


async def _wait_consumers(rabbitmq_url: str, queue: str, count: int) -> None:
    """Einzige lokale Fassung eines ``_harness``-Helfers (Plan Testlücken Runde 4, X-6):
    wartet auf *mindestens* ``count`` Consumer. ``wait_consumers`` aus ``_harness``
    verlangt die genaue Zahl. Im Retry-Test läuft ``_wait_consumers(..., 1)`` auch
    nach dem Start des zweiten Workers, wenn schon zwei Consumer anhängen können;
    ``==`` wäre dort nicht deterministisch."""

    async def ready() -> bool:
        return await consumer_count(rabbitmq_url, queue) >= count

    assert await wait_until(ready, timeout=_EVENT_TIMEOUT), f"{count} consumers never attached to {queue}"


# ---------------------------------------------------------------------------
# Retry über beide Versionen
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("first", ["old", "new"], ids=["old-creates", "new-creates"])
async def test_retry_between_old_and_new_worker_on_one_queue(
    first: str,
    start_worker: StartWorker,
    python_011: str,
    python_repo: str,
    undeclared_app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Review MR 8, P2-6: die zuerst gestartete Version legt die Queue an
    (0.11.1: Argumente aus 0.11; 0.12: at-least-once), die zweite übernimmt sie."""
    old, new = (python_011, OLD_VERSION), (python_repo, "repo")
    for python, expect in (old, new) if first == "old" else (new, old):
        start_worker(python, expect)
        await _wait_consumers(rabbitmq_url, queue, 1)
    await _wait_consumers(rabbitmq_url, queue, 2)
    app = undeclared_app
    producer = Client(app.broker, app.backend)

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
    await wait_leader(valkey_url, leader, timeout=_EVENT_TIMEOUT)
    follower = start_worker(*spec[second], module=_CRON_MODULE)
    await _wait_consumers(rabbitmq_url, queue, 2)

    await force_cron_fire(valkey_url, _CRON, timeout=_EVENT_TIMEOUT)
    await wait_runs(valkey_url, queue, _CRON, 1, timeout=_EVENT_TIMEOUT)

    leader.stop()
    await wait_leader(valkey_url, follower, timeout=_EVENT_TIMEOUT)
    await force_cron_fire(valkey_url, _CRON, timeout=_EVENT_TIMEOUT)
    runs = await wait_runs(valkey_url, queue, _CRON, 2, timeout=_EVENT_TIMEOUT)

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
    cli = await asyncio.to_thread(
        subprocess.run,
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

    runs = await wait_runs(valkey_url, queue, "compat_poison", 2, timeout=_EVENT_TIMEOUT)
    assert [(tid, attempt, runtime) for _, tid, attempt, runtime in runs] == [
        (task_id, 1, f"pypi-{OLD_VERSION}"),
        (task_id, 2, f"pypi-{OLD_VERSION}"),
    ]
    assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT) == f"pypi-{OLD_VERSION}"
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0
