"""``error_class`` im Mischbetrieb (Plan 2026-10-12-1105, MR 3, P9, Q2).

Ein Worker aus dem Arbeitsstand schreibt ``error_class`` ins Fehlerergebnis.
Ein 0.11.1-Client kennt das Feld nicht; er muss es ignorieren und denselben
``TaskError`` werfen wie bisher -- dieselbe Meldung, auch wenn sie leer ist.

Test-Liste:
- [x] Neuer Worker, 0.11.1-Client: ``get_result`` wirft ``TaskError`` mit
      derselben Meldung wie der neue Client, auch bei leerer Exception-Meldung;
      der neue Client sieht zusaetzlich ``error_class``
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
from collections.abc import Callable

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from swarmq.exceptions import TaskError
from tests.compat._harness import OLD_VERSION, CompatWorker

pytestmark = [pytest.mark.compat, pytest.mark.timeout(120)]

StartWorker = Callable[..., CompatWorker]

# Laeuft im 0.11.1-venv: nur die oeffentliche API, die 0.11.1 kennt.
_OLD_CLIENT = """
import asyncio, importlib.metadata, json, sys
from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError

async def main(broker_url, backend_url, queue, task_ids):
    out = {"version": importlib.metadata.version("swarmq"), "errors": []}
    async with SwarmQ(RabbitMQBroker(broker_url), ValkeyBackend(backend_url), queues=[queue]) as app:
        for task_id in task_ids:
            try:
                await app.get_result(task_id, timeout=30)
            except TaskError as exc:
                out["errors"].append([type(exc).__name__, str(exc)])
    print(json.dumps(out))

asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]))
"""


async def test_old_client_reads_new_error_results_unchanged(
    start_worker: StartWorker,
    python_011: str,
    python_repo: str,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
) -> None:
    start_worker(python_repo, "repo")
    async with SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue]) as app:
        producer = Client(app.broker, app.backend)
        task_ids = [
            await producer.schedule("compat_empty_error", queue=queue),
            await producer.schedule("compat_poison", queue=queue),
        ]
        new: list[tuple[str, str | None]] = []
        for task_id in task_ids:
            with pytest.raises(TaskError) as raised:
                await app.get_result(task_id, timeout=30)
            new.append((str(raised.value), raised.value.error_class))

    assert new == [("", "builtins.ValueError"), ("poison until fixed", "builtins.RuntimeError")]
    with tempfile.TemporaryDirectory() as cwd:  # nie das Repo im Pfad des 0.11.1-venv
        result = await asyncio.to_thread(
            subprocess.run,
            [python_011, "-c", _OLD_CLIENT, rabbitmq_url, valkey_url, queue, *task_ids],
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
            cwd=cwd,
        )
    assert result.returncode == 0, result.stderr[-2000:]
    old = json.loads(result.stdout.strip().splitlines()[-1])
    assert old["version"] == OLD_VERSION
    assert old["errors"] == [["TaskError", ""], ["TaskError", "poison until fixed"]]
