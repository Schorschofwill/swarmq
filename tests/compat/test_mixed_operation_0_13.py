"""Mischbetrieb 0.13.1 / Arbeitsstand (Plan 2026-10-19-1500, U0).

Basis für den Rollout von 0.14: swarmq 0.13.1 von PyPI (isoliertes venv,
``constraints-0.13.1.txt``). 0.14 ändert kein Format, keinen Header und kein
Lua; Worker, Producer und Leader beider Versionen müssen deshalb in beiden
Rollen nebeneinander laufen (Rolling Deploy, Rollback).

Test-Liste:
- [x] 0.13.1-Producer reiht ein, Worker aus dem Arbeitsstand führt aus (mit
      Retry); das Ergebnis ist über beide Clients lesbar
- [x] Umgekehrt: Producer aus dem Arbeitsstand, 0.13.1-Worker führt aus (mit Retry)
- [x] Rolling Deploy: beide Worker auf einer Queue mit Retry, in beiden
      Startreihenfolgen; je Nachricht genau zwei Versuche, beide Laufzeiten arbeiten
- [x] Retry-Übergabe über die Delay-Stufe ``{queue}.DQ.15s``: Upgrade
      (0.13.1 stellt zurück, der Arbeitsstand führt aus) und Rollback (umgekehrt)
- [x] DLQ-Replay per Shovel in beiden Richtungen: eine DLQ-Kopie der einen
      Version läuft nach dem Replay auf der anderen
- [x] Cron-Fire mit Leader-Wechsel 0.13.1 <-> Arbeitsstand in beiden
      Richtungen: je erzwungenem Fire genau ein Lauf
- [x] ``unique_for``-Claim über beide Versionen: ein zweiter Aufruf mit
      denselben Argumenten aus der anderen Version bekommt die bestehende
      Task-ID zurück, der Task läuft genau einmal
"""

from __future__ import annotations

import asyncio
import collections
import json
import subprocess
import tempfile
import uuid
from collections.abc import Callable
from pathlib import PurePath

import pytest

from swarmq.app import SwarmQ
from swarmq.client import Client
from swarmq.serialization import encode_json
from tests.compat._harness import (
    BASE_0131_VERSION,
    CRON_MODULE,
    CRON_TASK,
    EVENT_TIMEOUT,
    OLD_PRODUCER,
    REPO_ROOT,
    Base,
    CompatWorker,
    allow_poison,
    assert_two_attempts_each,
    force_cron_fire,
    message_count,
    recorded_runs,
    result_is,
    shovel,
    wait_consumers,
    wait_deferred_in_tier,
    wait_leader,
    wait_runs,
)
from tests.support.wait import wait_until

pytestmark = [pytest.mark.compat, pytest.mark.timeout(120)]

StartWorker = Callable[..., CompatWorker]

_MESSAGES = 5
_ROLLING_MESSAGES = 20
_AT_DELIVERY_LIMIT = 19  # delivery_limit 20 (Default) -> Gate ab 19
_UNIQUE_TASK = "compat_unique"
_UNIQUE_FOR_S = 600


@pytest.fixture
def base(python_0131: str) -> Base:
    """0.13.1 als alte Seite des Mischbetriebs."""
    return Base(python_0131, BASE_0131_VERSION)


async def _run_in_base(base: Base, script: str, *args: str) -> str:
    """*script* im venv der Basis ausführen; liefert die letzte Ausgabezeile."""
    with tempfile.TemporaryDirectory() as cwd:  # nie das Repo im Pfad des Basis-venv
        result = await asyncio.to_thread(
            subprocess.run,
            [base.python, "-c", script, *args],
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
            cwd=cwd,
        )
    assert result.returncode == 0, result.stderr[-2000:]
    return result.stdout.strip().splitlines()[-1]


async def test_old_producer_new_worker(
    start_worker: StartWorker,
    base: Base,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    start_worker(python_repo, "repo")
    await wait_consumers(rabbitmq_url, queue, 1)

    old = json.loads(await _run_in_base(base, OLD_PRODUCER, rabbitmq_url, valkey_url, queue, str(_MESSAGES)))

    assert old["version"] == base.version
    location = PurePath(old["location"])
    assert not location.is_relative_to(REPO_ROOT / "src"), f"{base.version} producer imports the repo: {location}"
    task_ids = list(old["results"])
    assert len(task_ids) == _MESSAGES
    assert set(old["results"].values()) == {"repo"}, f"the {base.version} client reads the new worker's results"
    for task_id in task_ids:
        assert await app.get_result(task_id, timeout=EVENT_TIMEOUT) == "repo"
    assert_two_attempts_each(await recorded_runs(valkey_url, queue), task_ids, "repo")
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


async def test_new_producer_old_worker(
    start_worker: StartWorker,
    base: Base,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    start_worker(base.python, base.version)
    await wait_consumers(rabbitmq_url, queue, 1)

    producer = Client(app.broker, app.backend)
    task_ids = [await producer.schedule("compat_flaky", queue=queue) for _ in range(_MESSAGES)]
    results = {task_id: await app.get_result(task_id, timeout=EVENT_TIMEOUT) for task_id in task_ids}

    assert set(results.values()) == {base.runtime}
    assert_two_attempts_each(await recorded_runs(valkey_url, queue), task_ids, base.runtime)
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


@pytest.mark.parametrize("first", ["old", "new"], ids=["old-creates", "new-creates"])
async def test_retry_between_0_13_1_and_new_worker_on_one_queue(
    first: str,
    start_worker: StartWorker,
    base: Base,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Rolling Deploy: die zuerst gestartete Version legt die Queue an, die
    zweite übernimmt sie; Retries wandern zwischen beiden Laufzeiten."""
    old, new = (base.python, base.version), (python_repo, "repo")
    for count, (python, expect) in enumerate((old, new) if first == "old" else (new, old), start=1):
        start_worker(python, expect)
        await wait_consumers(rabbitmq_url, queue, count)
    producer = Client(app.broker, app.backend)

    task_ids = [await producer.schedule("compat_flaky", queue=queue) for _ in range(_ROLLING_MESSAGES)]
    results = {task_id: await app.get_result(task_id, timeout=EVENT_TIMEOUT) for task_id in task_ids}

    runs = await recorded_runs(valkey_url, queue)
    attempts = collections.Counter(task_id for _, task_id, _, _ in runs)
    assert attempts == dict.fromkeys(task_ids, 2), "every message runs exactly twice (fail, retry)"
    assert set(results.values()) <= {"repo", base.runtime}
    assert {runtime for *_, runtime in runs} == {"repo", base.runtime}, "both versions consumed"
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


@pytest.mark.parametrize("deferring", ["old", "new"], ids=["upgrade", "rollback"])
async def test_deferred_retry_moves_between_0_13_1_and_new(
    deferring: str,
    start_worker: StartWorker,
    base: Base,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Eine von der einen Version in ``{queue}.DQ.15s`` zurückgestellte
    Nachricht läuft nach Ablauf auf der anderen, ohne Fehler und ohne Doppellauf."""
    tier = f"{queue}.DQ.15s"
    old, new = (base.python, base.version, base.runtime), (python_repo, "repo", "repo")
    (first_python, first_expect, first_runtime), (second_python, second_expect, second_runtime) = (
        (old, new) if deferring == "old" else (new, old)
    )

    first = start_worker(first_python, first_expect)
    await wait_consumers(rabbitmq_url, queue, 1)
    task_id = await Client(app.broker, app.backend).schedule("compat_slow_retry", queue=queue)
    await wait_deferred_in_tier(rabbitmq_url, tier)
    first.stop()
    await wait_consumers(rabbitmq_url, queue, 0)
    start_worker(second_python, second_expect)

    assert await app.get_result(task_id, timeout=EVENT_TIMEOUT + 10) == second_runtime
    runs = [(tid, attempt, runtime) for _, tid, attempt, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, 1, first_runtime), (task_id, 2, second_runtime)]
    assert await message_count(rabbitmq_url, tier) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


@pytest.mark.parametrize("dead_lettering", ["old", "new"], ids=["old-dead-letters", "new-dead-letters"])
async def test_shovel_replay_of_a_dlq_copy_between_0_13_1_and_new(
    dead_lettering: str,
    start_worker: StartWorker,
    base: Base,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Eine DLQ-Kopie (am Zustelllimit) der einen Version läuft nach einem
    Replay per Shovel/"Move messages" auf einem Worker der anderen."""
    old, new = (base.python, base.version, base.runtime), (python_repo, "repo", "repo")
    (first_python, first_expect, _), (second_python, second_expect, second_runtime) = (
        (old, new) if dead_lettering == "old" else (new, old)
    )

    first = start_worker(first_python, first_expect)
    await wait_consumers(rabbitmq_url, queue, 1)
    await allow_poison(valkey_url, queue)
    task_id = uuid.uuid4().hex
    body = encode_json({"task": {"id": task_id, "queue": queue, "name": "compat_poison"}, "args": [], "kwargs": {}})
    counters = {"x-delivery-count": _AT_DELIVERY_LIMIT, "x-acquired-count": _AT_DELIVERY_LIMIT}
    await app.broker.publish(queue, body, headers=counters)

    async def dead_lettered() -> bool:
        return await message_count(rabbitmq_url, f"{queue}.XQ") == 1

    assert await wait_until(dead_lettered, timeout=EVENT_TIMEOUT), f"{first_expect} never dead-lettered the message"
    assert await recorded_runs(valkey_url, queue) == [], "at the delivery limit: dead-lettered before running"
    first.stop()
    await wait_consumers(rabbitmq_url, queue, 0)
    await shovel(rabbitmq_url, f"{queue}.XQ", queue)
    start_worker(second_python, second_expect)

    async def replay_done() -> bool:
        # Das Fehlerergebnis des Dead-Letterns steht schon; erst der Lauf
        # nach dem Replay überschreibt es.
        return await message_count(rabbitmq_url, queue) == 0 and bool(await recorded_runs(valkey_url, queue))

    assert await wait_until(replay_done, timeout=EVENT_TIMEOUT), f"{second_expect} never ran the replayed copy"
    runs = [(tid, runtime) for _, tid, _, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, second_runtime)]
    assert await wait_until(lambda: result_is(app, task_id, second_runtime), timeout=EVENT_TIMEOUT)
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


@pytest.mark.parametrize(("first", "second"), [("old", "new"), ("new", "old")], ids=["old-leads", "new-leads"])
async def test_cron_fire_survives_leader_change_with_0_13_1(
    first: str,
    second: str,
    start_worker: StartWorker,
    base: Base,
    python_repo: str,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    spec = {"old": (base.python, base.version), "new": (python_repo, "repo")}

    leader = start_worker(*spec[first], module=CRON_MODULE)
    await wait_leader(valkey_url, leader)
    follower = start_worker(*spec[second], module=CRON_MODULE)
    await wait_consumers(rabbitmq_url, queue, 2)

    await force_cron_fire(valkey_url, CRON_TASK)
    await wait_runs(valkey_url, queue, CRON_TASK, 1)

    leader.stop()
    await wait_leader(valkey_url, follower)
    await force_cron_fire(valkey_url, CRON_TASK)
    await wait_runs(valkey_url, queue, CRON_TASK, 2)

    follower.stop()
    runs = [run for run in await recorded_runs(valkey_url, queue) if run[0] == CRON_TASK]
    assert len(runs) == 2, f"each forced fire runs exactly once: {runs}"
    assert len({task_id for _, task_id, _, _ in runs}) == 2
    assert runs[-1][3] == follower.runtime, "after the leader stopped, only the follower consumes"
    assert await message_count(rabbitmq_url, queue) == 0, "no duplicate fire left in the queue"


# Läuft im venv der Basis: ``schedule`` mit ``unique_for``, gibt die Task-ID aus.
_OLD_UNIQUE_PRODUCER = """
import asyncio, json, sys
from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client

async def main(broker_url, backend_url, queue, unique_for):
    async with SwarmQ(RabbitMQBroker(broker_url), ValkeyBackend(backend_url), queues=[queue]) as app:
        producer = Client(app.broker, app.backend)
        task_id = await producer.schedule("compat_unique", queue=queue, unique_for=unique_for)
    print(json.dumps({"task_id": task_id}))

asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])))
"""


@pytest.mark.parametrize("claiming", ["old", "new"], ids=["old-claims", "new-claims"])
async def test_unique_for_claim_is_shared_with_0_13_1(
    claiming: str,
    start_worker: StartWorker,
    base: Base,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Der ``unique_for``-Claim der einen Version gilt für die andere: Solange
    der Task aussteht, bekommt ein zweiter Aufruf aus der anderen Version die
    bestehende Task-ID zurück, und der Task läuft genau einmal. Der Worker der
    anderen Version gibt den Claim nach dem Erfolg frei; ein neuer Aufruf der
    planenden Version bekommt danach eine neue Task-ID."""

    async def schedule_old() -> str:
        line = await _run_in_base(base, _OLD_UNIQUE_PRODUCER, rabbitmq_url, valkey_url, queue, str(_UNIQUE_FOR_S))
        task_id: str = json.loads(line)["task_id"]
        return task_id

    async def schedule_new() -> str:
        return await Client(app.broker, app.backend).schedule(_UNIQUE_TASK, queue=queue, unique_for=_UNIQUE_FOR_S)

    old, new = (base.python, base.version), (python_repo, "repo")
    claimer, runner = (old, new) if claiming == "old" else (new, old)
    first, second = (schedule_old, schedule_new) if claiming == "old" else (schedule_new, schedule_old)
    # Die planende Version legt die Queue-Familie an und stoppt: Der Task
    # steht aus, solange beide Aufrufe laufen.
    start_worker(*claimer).stop()
    await wait_consumers(rabbitmq_url, queue, 0)

    task_id = await first()
    assert await second() == task_id, "the other version returns the existing task_id while the claim holds"
    assert await message_count(rabbitmq_url, queue) == 1, "the duplicate was never published"
    worker = start_worker(*runner)

    assert await app.get_result(task_id, timeout=EVENT_TIMEOUT) == worker.runtime
    runs = [(tid, runtime) for _, tid, _, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, worker.runtime)]
    rescheduled = await first()
    assert rescheduled != task_id, "the other version's worker released the claim after the success"
    assert await app.get_result(rescheduled, timeout=EVENT_TIMEOUT) == worker.runtime
    assert not [line for line in worker.lines if "Traceback" in line]
