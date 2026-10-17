"""Mischbetrieb 0.12.x / Arbeitsstand (Plan 2026-10-14-0748, U2, R27, KTD19; Plan 2026-10-17-2103, U1).

Zweite Basis neben 0.11.1: swarmq 0.12.0 von PyPI (isoliertes venv,
``constraints-0.12.0.txt``). Die Fixes der 0.12.x-Reihe müssen in beiden
Richtungen neben 0.12.0-Workern und -Producern laufen (Rolling Deploy,
Rollback). Fixes mit wandernden Daten (Header, Lua, Definitionen) ergänzen
hier ihre eigenen Fälle (U5, U11, U13, U16).

Seit Plan 2026-10-17-2103 (U1, R5) laufen Retry-Übergabe, Rollback, Upgrade,
DLQ-Replay und Leader-Wechsel über die Fixture ``base`` gegen beide Basen der
Reihe, 0.12.0 und 0.12.3 (letzte Version vor 0.12.4). Die Fälle zu
Schedfire-Zurückstellung (U5) und Opt-out (U13) bleiben bei 0.12.0: nur
0.12.0 kennt Header und Opt-out nicht; den Header gibt es seit 0.12.1, den
Opt-out seit 0.12.2 (beides in 0.12.3 enthalten).

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
- [x] U16 (FS-10): eine DLQ-Kopie des Arbeitsstands (am Zustelllimit) läuft nach
      einem Replay per Shovel/"Move messages" auf einem 0.12.0-Worker
- [x] U13 (FS-14): ``schedule(delay=, unique_for=None)`` bei gehaltenem Flag,
      Matrix Producer x Leader (0.12.0 und Arbeitsstand): nur mit beiden aus dem
      Arbeitsstand laeuft der Oneshot; in den Mischfaellen verwirft der Leader
      ihn wie bisher als Duplikat (dokumentierte Rollout-Grenze), ohne Fehler
- [x] Plan 2026-10-17-2103, U1: Basis 0.12.3 neben 0.12.0 für Producer/Worker,
      Rolling Deploy, Upgrade, Rollback und Shovel-Replay
- [x] Plan 2026-10-17-2103, U1: Cron-Fire mit Leader-Wechsel 0.12.x <-> Arbeitsstand
      in beiden Richtungen: je erzwungenem Fire genau ein Lauf
"""

from __future__ import annotations

import asyncio
import collections
import json
import subprocess
import tempfile
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import PurePath

import aio_pika
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from tests.compat._harness import (
    BASE_012_VERSION,
    BASE_0123_VERSION,
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

pytestmark = [pytest.mark.compat, pytest.mark.timeout(120)]

StartWorker = Callable[..., CompatWorker]

_MESSAGES = 5
_ROLLING_MESSAGES = 20
_EVENT_TIMEOUT = 20.0
_OLD_RUNTIME = f"pypi-{BASE_012_VERSION}"
_CRON = "compat_cron"
_CRON_MODULE = "tests.compat._compat_cron_tasks"

#: Basis-Version -> Fixture ihres venv-Interpreters.
_BASES = {BASE_012_VERSION: "python_0120", BASE_0123_VERSION: "python_0123"}


@dataclass(frozen=True)
class Base:
    """Eine alte Basis der 0.12-Reihe: Interpreter ihres venv und Version."""

    python: str
    version: str

    @property
    def runtime(self) -> str:
        return f"pypi-{self.version}"


@pytest.fixture(params=list(_BASES))
def base(request: pytest.FixtureRequest) -> Base:
    """0.12.0 und 0.12.3 als alte Seite des Mischbetriebs (Plan 2026-10-17-2103, U1)."""
    version: str = request.param
    python: str = request.getfixturevalue(_BASES[version])
    return Base(python, version)


# Läuft im venv der Basis: nur die öffentliche API, die 0.12.0 kennt.
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
    base: Base,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    start_worker(python_repo, "repo")
    await _wait_consumers(rabbitmq_url, queue, 1)

    with tempfile.TemporaryDirectory() as cwd:  # nie das Repo im Pfad des Basis-venv
        result = await asyncio.to_thread(
            subprocess.run,
            [base.python, "-c", _OLD_PRODUCER, rabbitmq_url, valkey_url, queue, str(_MESSAGES)],
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
            cwd=cwd,
        )
    assert result.returncode == 0, result.stderr[-2000:]
    old = json.loads(result.stdout.strip().splitlines()[-1])
    assert old["version"] == base.version
    location = PurePath(old["location"])  # im Subprozess bereits realpath
    assert not location.is_relative_to(REPO_ROOT / "src"), f"{base.version} producer imports the repo: {location}"
    task_ids = list(old["results"])
    assert len(task_ids) == _MESSAGES
    assert set(old["results"].values()) == {"repo"}, f"the {base.version} client reads the new worker's results"
    for task_id in task_ids:
        assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT) == "repo"

    _assert_two_attempts_each(await recorded_runs(valkey_url, queue), task_ids, "repo")
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
    await _wait_consumers(rabbitmq_url, queue, 1)

    producer = Client(app.broker, app.backend)
    task_ids = [await producer.schedule("compat_flaky", queue=queue) for _ in range(_MESSAGES)]
    results = {task_id: await app.get_result(task_id, timeout=_EVENT_TIMEOUT) for task_id in task_ids}

    assert set(results.values()) == {base.runtime}
    _assert_two_attempts_each(await recorded_runs(valkey_url, queue), task_ids, base.runtime)
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


@pytest.mark.parametrize("first", ["old", "new"], ids=["old-creates", "new-creates"])
async def test_retry_between_0_12_x_and_new_worker_on_one_queue(
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
        await _wait_consumers(rabbitmq_url, queue, count)
    producer = Client(app.broker, app.backend)

    task_ids = [await producer.schedule("compat_flaky", queue=queue) for _ in range(_ROLLING_MESSAGES)]
    results = {task_id: await app.get_result(task_id, timeout=_EVENT_TIMEOUT) for task_id in task_ids}

    runs = await recorded_runs(valkey_url, queue)
    attempts = collections.Counter(task_id for _, task_id, _, _ in runs)
    assert attempts == dict.fromkeys(task_ids, 2), "every message runs exactly twice (fail, retry)"
    assert set(results.values()) <= {"repo", base.runtime}
    assert {runtime for *_, runtime in runs} == {"repo", base.runtime}, "both versions consumed"
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


async def _wait_deferred_in_tier(rabbitmq_url: str, tier: str) -> None:
    async def deferred_in_tier() -> bool:
        try:
            return await message_count(rabbitmq_url, tier) == 1
        except aio_pika.exceptions.ChannelNotFoundEntity:
            return False

    assert await wait_until(deferred_in_tier, timeout=_EVENT_TIMEOUT), "retry never reached the delay tier"


async def test_upgrade_runs_a_deferred_retry_of_0_12_x_on_new_worker(
    start_worker: StartWorker,
    base: Base,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Upgrade: eine von der Basis in eine Delay-Stufe zurückgestellte Nachricht
    läuft nach Ablauf auf dem Arbeitsstand, der die Queue-Familie neu deklariert."""
    tier = f"{queue}.DQ.15s"

    old = start_worker(base.python, base.version)
    await _wait_consumers(rabbitmq_url, queue, 1)
    task_id = await Client(app.broker, app.backend).schedule("compat_slow_retry", queue=queue)
    await _wait_deferred_in_tier(rabbitmq_url, tier)
    old.stop()
    await _wait_consumers(rabbitmq_url, queue, 0)
    start_worker(python_repo, "repo")

    assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT + 10) == "repo"

    runs = [(tid, attempt, runtime) for _, tid, attempt, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, 1, base.runtime), (task_id, 2, "repo")]
    assert await message_count(rabbitmq_url, tier) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


async def test_rollback_runs_a_deferred_retry_on_0_12_x(
    start_worker: StartWorker,
    base: Base,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    """Rollback: eine vom Arbeitsstand in eine Delay-Stufe zurückgestellte
    Nachricht läuft nach Ablauf auf der Basis, ohne Fehler und ohne Doppellauf."""
    tier = f"{queue}.DQ.15s"

    new = start_worker(python_repo, "repo")
    await _wait_consumers(rabbitmq_url, queue, 1)
    task_id = await Client(app.broker, app.backend).schedule("compat_slow_retry", queue=queue)
    await _wait_deferred_in_tier(rabbitmq_url, tier)
    new.stop()
    await _wait_consumers(rabbitmq_url, queue, 0)
    start_worker(base.python, base.version)

    assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT + 10) == base.runtime

    runs = [(tid, attempt, runtime) for _, tid, attempt, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, 1, "repo"), (task_id, 2, base.runtime)]
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


# ---------------------------------------------------------------------------
# U16 (FS-10, KTD13/KTD19): DLQ-Kopien des Arbeitsstands tragen keine
# Broker-Zustellzaehler. Ein Replay per Shovel/"Move messages" einer solchen
# Kopie laeuft auch auf einem Worker der Basis 0.12.0 bzw. 0.12.3 (dessen
# Delivery-Count-Gate sieht keinen Zaehler).
# ---------------------------------------------------------------------------

_AT_DELIVERY_LIMIT = 19  # delivery_limit 20 (Default) -> Gate ab 19


async def _shovel(rabbitmq_url: str, source: str, target: str) -> None:
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


async def _result_is(app: SwarmQ, task_id: str, expected: str) -> bool:
    from swarmq.exceptions import TaskError

    try:
        result: object = await app.get_result(task_id, timeout=1)
    except TaskError:  # Fehlerergebnis des Dead-Letterns, noch nicht ueberschrieben
        return False
    return result == expected


async def test_shovel_replay_of_a_new_dlq_copy_runs_on_0_12_x(
    start_worker: StartWorker,
    base: Base,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    import uuid

    from swarmq.serialization import encode_json

    new = start_worker(python_repo, "repo")
    await _wait_consumers(rabbitmq_url, queue, 1)
    await _allow_poison(valkey_url, queue)
    task_id = uuid.uuid4().hex
    body = encode_json({"task": {"id": task_id, "queue": queue, "name": "compat_poison"}, "args": [], "kwargs": {}})
    counters = {"x-delivery-count": _AT_DELIVERY_LIMIT, "x-acquired-count": _AT_DELIVERY_LIMIT}
    await app.broker.publish(queue, body, headers=counters)

    async def dead_lettered() -> bool:
        return await message_count(rabbitmq_url, f"{queue}.XQ") == 1

    assert await wait_until(dead_lettered, timeout=_EVENT_TIMEOUT), "the new worker never dead-lettered the message"
    assert await recorded_runs(valkey_url, queue) == [], "at the delivery limit: dead-lettered before running"
    new.stop()
    await _wait_consumers(rabbitmq_url, queue, 0)
    await _shovel(rabbitmq_url, f"{queue}.XQ", queue)
    start_worker(base.python, base.version)

    async def replay_done() -> bool:
        # Das Fehlerergebnis des Dead-Letterns steht schon; erst der Lauf auf
        # der Basis ueberschreibt es.
        return await message_count(rabbitmq_url, queue) == 0 and bool(await recorded_runs(valkey_url, queue))

    assert await wait_until(replay_done, timeout=_EVENT_TIMEOUT), f"{base.version} never ran the replayed copy"
    runs = [(tid, runtime) for _, tid, _, runtime in await recorded_runs(valkey_url, queue)]
    assert runs == [(task_id, base.runtime)]
    assert await wait_until(lambda: _result_is(app, task_id, base.runtime), timeout=_EVENT_TIMEOUT)
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


# ---------------------------------------------------------------------------
# U13 (FS-14, KTD10/KTD19): Der Opt-out ``unique_for=None`` eines verzoegerten
# Oneshots reist als ``source="cli_unique"`` ohne Hash. 0.12.0 kennt den Wert
# (gleiches ``Literal``), faellt als Leader aber auf seine TaskInfo zurueck;
# ein 0.12.0-Producer schreibt weiter ``source="cli"``. Wirksam ist der
# Opt-out also erst mit Producer und Leader aus dem Arbeitsstand.
# ---------------------------------------------------------------------------

_UNIQUE_TASK = "compat_unique"

# Laeuft im 0.12.0-venv: plant einen verzoegerten Oneshot mit Opt-out ein.
_OLD_OPT_OUT_PRODUCER = """
import asyncio, json, sys
from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client

async def main(broker_url, backend_url, queue):
    async with SwarmQ(RabbitMQBroker(broker_url), ValkeyBackend(backend_url), queues=[queue]) as app:
        producer = Client(app.broker, app.backend)
        task_id = await producer.schedule("compat_unique", queue=queue, delay=1.0, unique_for=None)
    print(json.dumps({"task_id": task_id}))

asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3]))
"""


async def _schedule_opt_out(
    producer: str, python_0120: str, app: SwarmQ, rabbitmq_url: str, valkey_url: str, queue: str
) -> str:
    if producer == "new":
        return await Client(app.broker, app.backend).schedule(_UNIQUE_TASK, queue=queue, delay=1.0, unique_for=None)
    with tempfile.TemporaryDirectory() as cwd:  # nie das Repo im Pfad des 0.12.0-venv
        result = await asyncio.to_thread(
            subprocess.run,
            [python_0120, "-c", _OLD_OPT_OUT_PRODUCER, rabbitmq_url, valkey_url, queue],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
            cwd=cwd,
        )
    assert result.returncode == 0, result.stderr[-2000:]
    task_id: str = json.loads(result.stdout.strip().splitlines()[-1])["task_id"]
    return task_id


async def _wait_oneshot_committed(valkey_url: str, task_id: str) -> None:
    import valkey.asyncio as aio_valkey

    from swarmq.keyspace import KeySpace

    zset = KeySpace(None).sched_zset()
    client = aio_valkey.Valkey.from_url(valkey_url)
    try:

        async def committed() -> bool:
            return await client.zscore(zset, f"oneshot:{task_id}") is None

        assert await wait_until(committed, timeout=_EVENT_TIMEOUT), "the leader never fired the oneshot"
    finally:
        await client.aclose()


@pytest.mark.parametrize("leader", ["old", "new"], ids=["leader-0.12.0", "leader-new"])
@pytest.mark.parametrize("producer", ["old", "new"], ids=["producer-0.12.0", "producer-new"])
async def test_deferred_opt_out_between_0_12_0_and_new(
    producer: str,
    leader: str,
    start_worker: StartWorker,
    python_0120: str,
    python_repo: str,
    app: SwarmQ,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    from swarmq.serialization import compute_dedup_hash

    worker = start_worker(python_repo, "repo") if leader == "new" else start_worker(python_0120, BASE_012_VERSION)
    await _wait_consumers(rabbitmq_url, queue, 1)
    # Ein anderer Lauf mit denselben Argumenten haelt das Flag.
    dedup_hash = compute_dedup_hash(_UNIQUE_TASK, (), {})
    assert await app.backend.claim_unique(dedup_hash, "fs14-other-run", 600_000) is None

    task_id = await _schedule_opt_out(producer, python_0120, app, rabbitmq_url, valkey_url, queue)
    await _wait_oneshot_committed(valkey_url, task_id)

    if producer == leader == "new":
        assert await app.get_result(task_id, timeout=_EVENT_TIMEOUT) == "repo"
        runs = [(tid, runtime) for _, tid, _, runtime in await recorded_runs(valkey_url, queue)]
        assert runs == [(task_id, "repo")]
    else:
        # Rollout-Grenze (Doku): der Leader dedupliziert nach seiner TaskInfo.
        async def dropped_as_duplicate() -> bool:
            return any("dedup_detected" in line for line in worker.lines)

        assert await wait_until(dropped_as_duplicate, timeout=_EVENT_TIMEOUT), "leader never logged dedup_detected"
        assert await recorded_runs(valkey_url, queue) == []
    assert not [line for line in worker.lines if "Traceback" in line]
    assert await message_count(rabbitmq_url, queue) == 0
    assert await message_count(rabbitmq_url, f"{queue}.XQ") == 0


# ---------------------------------------------------------------------------
# Plan 2026-10-17-2103, U1: Cron-Fire mit Leader-Wechsel zwischen einer Basis
# der 0.12-Reihe und dem Arbeitsstand (Muster: ``test_mixed_operation``).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("first", "second"), [("old", "new"), ("new", "old")], ids=["old-leads", "new-leads"])
async def test_cron_fire_survives_leader_change_with_0_12_x(
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

    leader = start_worker(*spec[first], module=_CRON_MODULE)
    await wait_leader(valkey_url, leader)
    follower = start_worker(*spec[second], module=_CRON_MODULE)
    await _wait_consumers(rabbitmq_url, queue, 2)

    await force_cron_fire(valkey_url, _CRON)
    await wait_runs(valkey_url, queue, _CRON, 1)

    leader.stop()
    await wait_leader(valkey_url, follower)
    await force_cron_fire(valkey_url, _CRON)
    await wait_runs(valkey_url, queue, _CRON, 2)

    follower.stop()
    runs = [run for run in await recorded_runs(valkey_url, queue) if run[0] == _CRON]
    assert len(runs) == 2, f"each forced fire runs exactly once: {runs}"
    assert len({task_id for _, task_id, _, _ in runs}) == 2
    assert runs[-1][3] == follower.runtime, "after the leader stopped, only the follower consumes"
    assert await message_count(rabbitmq_url, queue) == 0, "no duplicate fire left in the queue"
