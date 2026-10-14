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
- [x] Rolling Deploy (Review U2, P2): 0.12.0- und Arbeitsstands-Worker
      gleichzeitig auf einer Queue mit Retry, in beiden Startreihenfolgen (wer die
      Queue anlegt); je Nachricht genau zwei Versuche, beide Laufzeiten arbeiten
- [x] Upgrade (Review U2, P2): 0.12.0 stellt einen Retry in ``{queue}.DQ.15s``
      zurück und stoppt; der Arbeitsstand deklariert neu und führt Versuch 2 aus
- [x] Der 0.12.0-Producer läuft wirklich aus dem venv, nicht aus ``REPO_ROOT/src``
      (Review U2, P3)
"""

from __future__ import annotations

import asyncio
import collections
import json
import subprocess
import tempfile
from collections.abc import AsyncIterator, Callable
from pathlib import PurePath

import aio_pika
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from tests.compat._harness import (
    BASE_012_VERSION,
    REPO_ROOT,
    CompatWorker,
    consumer_count,
    message_count,
    recorded_runs,
)
from tests.support.wait import wait_until

pytestmark = [pytest.mark.compat, pytest.mark.timeout(120)]

StartWorker = Callable[..., CompatWorker]

_MESSAGES = 5
_ROLLING_MESSAGES = 20
_EVENT_TIMEOUT = 20.0
_OLD_RUNTIME = f"pypi-{BASE_012_VERSION}"

# Läuft im 0.12.0-venv: nur die öffentliche API, die 0.12.0 kennt.
_OLD_PRODUCER = """
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
    location = PurePath(old["location"])  # im Subprozess bereits realpath
    assert not location.is_relative_to(REPO_ROOT / "src"), f"0.12.0 producer imports the repo: {location}"
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


@pytest.mark.parametrize("first", ["old", "new"], ids=["old-creates", "new-creates"])
async def test_retry_between_0_12_0_and_new_worker_on_one_queue(
    first: str,
    start_worker: StartWorker,
    python_0120: str,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Rolling Deploy: die zuerst gestartete Version legt die Queue an, die
    zweite übernimmt sie; Retries wandern zwischen beiden Laufzeiten."""
    old, new = (python_0120, BASE_012_VERSION), (python_repo, "repo")
    for count, (python, expect) in enumerate((old, new) if first == "old" else (new, old), start=1):
        start_worker(python, expect)
        await _wait_consumers(rabbitmq_url, queue, count)
    producer = Client(app.broker, app.backend)

    task_ids = [await producer.schedule("compat_flaky", queue=queue) for _ in range(_ROLLING_MESSAGES)]
    results = {task_id: await app.get_result(task_id, timeout=_EVENT_TIMEOUT) for task_id in task_ids}

    runs = await recorded_runs(valkey_url, queue)
    attempts = collections.Counter(task_id for _, task_id, _, _ in runs)
    assert attempts == dict.fromkeys(task_ids, 2), "every message runs exactly twice (fail, retry)"
    assert set(results.values()) <= {"repo", _OLD_RUNTIME}
    assert {runtime for *_, runtime in runs} == {"repo", _OLD_RUNTIME}, "both versions consumed"
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


async def _wait_deferred_in_tier(rabbitmq_url: str, tier: str) -> None:
    async def deferred_in_tier() -> bool:
        try:
            return await message_count(rabbitmq_url, tier) == 1
        except aio_pika.exceptions.ChannelNotFoundEntity:
            return False

    assert await wait_until(deferred_in_tier, timeout=_EVENT_TIMEOUT), "retry never reached the delay tier"


async def test_upgrade_runs_a_deferred_retry_of_0_12_0_on_new_worker(
    start_worker: StartWorker,
    python_0120: str,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Upgrade: eine von 0.12.0 in eine Delay-Stufe zurückgestellte Nachricht
    läuft nach Ablauf auf dem Arbeitsstand, der die Queue-Familie neu deklariert."""
    tier = f"{queue}.DQ.15s"

    old = start_worker(python_0120, BASE_012_VERSION)
    await _wait_consumers(rabbitmq_url, queue, 1)
    task_id = await Client(app.broker, app.backend).schedule("compat_slow_retry", queue=queue)
    await _wait_deferred_in_tier(rabbitmq_url, tier)
    old.stop()
    await _wait_consumers(rabbitmq_url, queue, 0)
    start_worker(python_repo, "repo")

    assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT + 10) == "repo"

    runs = [(tid, attempt, runtime) for _, tid, attempt, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, 1, _OLD_RUNTIME), (task_id, 2, "repo")]
    assert await message_count(rabbitmq_url, tier) == 0
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

    new = start_worker(python_repo, "repo")
    await _wait_consumers(rabbitmq_url, queue, 1)
    task_id = await Client(app.broker, app.backend).schedule("compat_slow_retry", queue=queue)
    await _wait_deferred_in_tier(rabbitmq_url, tier)
    new.stop()
    await _wait_consumers(rabbitmq_url, queue, 0)
    start_worker(python_0120, BASE_012_VERSION)

    assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT + 10) == _OLD_RUNTIME

    runs = [(tid, attempt, runtime) for _, tid, attempt, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, 1, "repo"), (task_id, 2, _OLD_RUNTIME)]
    assert await message_count(rabbitmq_url, tier) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


# ---------------------------------------------------------------------------
# U5 (FS-03, KTD5/KTD19): Schedfire-Zurueckstellung im Mischbetrieb.
# Eine zurueckgestellte Kopie traegt ``x-swarmq-schedfire-defer-count``;
# 0.12.0 kennt den Header nicht und verhaelt sich wie bisher (fremder Claim:
# Duplikat-Ack, freier Claim: Lauf), ohne Fehler.
# ---------------------------------------------------------------------------


async def _publish_schedfire_copy(app: SwarmQ, queue: str, fire_key: str, defer_count: int) -> str:
    """Eine Schedfire-Kopie, wie sie der Arbeitsstand zurueckstellt."""
    import uuid

    from swarmq.headers import HEADER_SCHEDFIRE_DEFER_COUNT
    from swarmq.serialization import encode_json

    task_id = uuid.uuid4().hex
    body = encode_json({"task": {"id": task_id, "queue": queue, "name": "compat_poison"}, "args": [], "kwargs": {}})
    headers = {"x-swarmq-schedfire": fire_key, HEADER_SCHEDFIRE_DEFER_COUNT: defer_count}
    await app.broker.publish(queue, body, headers=headers)
    return task_id


async def _allow_poison(valkey_url: str, queue: str) -> None:
    """``compat_poison`` gelingt ab jetzt (``compat:flag:<queue>``)."""
    import valkey.asyncio as aio_valkey

    client = aio_valkey.Valkey.from_url(valkey_url)
    try:
        await client.set(f"compat:flag:{queue}", "1")
    finally:
        await client.aclose()


async def test_deferred_schedfire_copy_with_foreign_claim_is_acked_by_0_12_0(
    start_worker: StartWorker,
    python_0120: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Eine zurueckgestellte Kopie erreicht einen 0.12.0-Worker, der Claim ist
    noch fremd: 0.12.0 bestaetigt sie als Duplikat wie bisher -- ohne Fehler,
    ohne Lauf, ohne DLQ (Verlust nur mit 0.12.0 als Empfaenger, dokumentiert)."""
    old = start_worker(python_0120, BASE_012_VERSION)
    await _wait_consumers(rabbitmq_url, queue, 1)
    await _allow_poison(valkey_url, queue)
    fire_key = f"compat-cron:{queue}:1700000000000"
    assert await app.backend.schedules.claim_fire(fire_key, 60_000, owner="W1") is True

    await _publish_schedfire_copy(app, queue, fire_key, defer_count=2)

    async def acked_as_duplicate() -> bool:
        return any("schedfire_inflight_duplicate_acked" in line for line in old.lines)

    assert await wait_until(acked_as_duplicate, timeout=_EVENT_TIMEOUT), "0.12.0 never acked the copy"
    assert await recorded_runs(valkey_url, queue) == []
    assert not [line for line in old.lines if "Traceback" in line]
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


async def test_rollback_runs_a_deferred_schedfire_copy_on_0_12_0_once_the_claim_expired(
    start_worker: StartWorker,
    python_0120: str,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rollback mit Kopien in der Delay-Queue: der Arbeitsstand stellt eine
    Neuzustellung bei fremdem Claim zurueck (``{queue}.DQ.15s``) und stoppt;
    nach Ablauf des Claims fuehrt 0.12.0 die Kopie genau einmal aus."""
    # Zurueckstellung 2 mit Basis 6 s: 9,6 ... 14,4 s, also immer DQ.15s.
    monkeypatch.setenv("SWARMQ_BACKEND_DEFER_BASE_DELAY", "6")
    monkeypatch.setenv("SWARMQ_BACKEND_DEFER_MAX_DELAY", "60")
    tier = f"{queue}.DQ.15s"
    new = start_worker(python_repo, "repo")
    await _wait_consumers(rabbitmq_url, queue, 1)
    await _allow_poison(valkey_url, queue)
    fire_key = f"compat-cron:{queue}:1700000000000"
    assert await app.backend.schedules.claim_fire(fire_key, 60_000, owner="W1") is True

    task_id = await _publish_schedfire_copy(app, queue, fire_key, defer_count=1)
    await _wait_deferred_in_tier(rabbitmq_url, tier)
    assert await recorded_runs(valkey_url, queue) == []
    new.stop()
    await _wait_consumers(rabbitmq_url, queue, 0)
    await app.backend.schedules.clear_schedfire_inflight(fire_key)  # Claim abgelaufen
    start_worker(python_0120, BASE_012_VERSION)

    assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT + 10) == _OLD_RUNTIME

    runs = [(tid, runtime) for _, tid, _, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, _OLD_RUNTIME)]
    assert await app.backend.schedules.is_fire_completed(fire_key) is True
    assert await message_count(rabbitmq_url, tier) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0
