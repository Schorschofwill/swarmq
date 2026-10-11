"""Rollback mit gesetztem Namensraum (Plan 0.12.0, Q2, MR 15, BZ-10).

Ab 0.12 setzt ``SWARMQ_NAMESPACE`` ein Praefix vor alle Valkey-Keys. Ein
Rollback auf 0.11.1 fiele ohne Sperre still in den Zustand ohne Praefix
(Termine, Ergebnisse, Locks unter anderen Keys). 0.11.1 verweigert darum den
Start (MR 7): Exit 78, keine Nachricht verbraucht. Rollback nach dem Setzen
des Namensraums also nur auf >= 0.12 (Plan Abschnitt 7).

Test-Liste:
- [x] 0.12 reiht mit Namensraum ein; ein 0.11.1-Worker mit demselben
      ``SWARMQ_NAMESPACE`` endet mit Exit 78 und Hinweis auf 0.12, die
      Nachrichten bleiben in der Queue
"""

from __future__ import annotations

import subprocess

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from tests.compat._harness import OLD_VERSION, message_count
from tests.support.worker_subprocess import kill_subprocess, spawn_worker

pytestmark = [pytest.mark.compat, pytest.mark.timeout(120)]

_NAMESPACE = "compat-ns"
_MESSAGES = 2


async def test_0_11_1_refuses_a_set_namespace(python_011: str, rabbitmq_url: str, valkey_url: str, queue: str) -> None:
    swarm = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url, namespace=_NAMESPACE), queues=[queue])
    async with swarm:
        await swarm.broker.declare_queue(queue)
        producer = Client(swarm.broker, swarm.backend)
        for _ in range(_MESSAGES):
            await producer.schedule("compat_flaky", queue=queue)

    proc = spawn_worker(
        queue,
        fixture_module="tests.compat._compat_tasks",
        broker_url=rabbitmq_url,
        backend_url=valkey_url,
        concurrency=2,
        env_extra={"COMPAT_EXPECT_SWARMQ": OLD_VERSION, "SWARMQ_NAMESPACE": _NAMESPACE},
        python=python_011,
    )
    try:
        output, _ = proc.communicate(timeout=100)
    except subprocess.TimeoutExpired:
        kill_subprocess(proc)
        raise

    assert proc.returncode == 78, f"swarmq {OLD_VERSION} started with a namespace: {output[-2000:]}"
    assert "swarmq >= 0.12" in output, output[-2000:]
    assert await message_count(rabbitmq_url, queue) == _MESSAGES
