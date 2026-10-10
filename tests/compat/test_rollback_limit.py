"""Rollback-Grenze 0.11.0 (Plan 0.12.0, MR 8; Abschnitt 1, Nachtrag 2026-10-10).

Ab 0.12 legt swarmq neue Quorum-Queues mit ``x-dead-letter-strategy=at-least-once``
und ``x-overflow=reject-publish`` an (BZ-19). 0.11.0 deklariert dieselbe Queue
ohne diese Argumente und bekommt ``PRECONDITION_FAILED``; nur 0.11.1 übernimmt
sie. Deshalb geht der Rollout erst auf 0.11.1. Dieses Szenario hält fest, dass
0.11.0 dabei sauber scheitert: 406 im Log, Exit ≠ 0, keine Nachricht verloren,
Queue-Argumente unverändert.

Test-Liste:
- [x] 0.11.0-Worker auf einer von 0.12 angelegten Queue: 406, Exit ≠ 0, Nachrichten
      und Argumente unverändert
"""

from __future__ import annotations

import subprocess
from typing import Any

import aio_pika
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from tests.compat._harness import ROLLBACK_LIMIT_VERSION, message_count
from tests.support.worker_subprocess import kill_subprocess, spawn_worker

pytestmark = [pytest.mark.compat, pytest.mark.timeout(120)]

_MESSAGES = 3
_ALO = {"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"}


def _arguments_012(queue: str) -> dict[str, dict[str, Any]]:
    """Die Argumente, mit denen 0.12 die Queue-Familie neu anlegt (``auto``)."""
    return {
        queue: {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": f"{queue}.XQ",
            "x-delivery-limit": 20,
            **_ALO,
        },
        f"{queue}.DQ": {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": queue,
            **_ALO,
        },
        f"{queue}.XQ": {"x-queue-type": "quorum"},
    }


async def _declare_equivalent(rabbitmq_url: str, name: str, arguments: dict[str, Any]) -> None:
    """Eine Deklaration mit genau diesen Argumenten gelingt nur, wenn die Queue
    sie unverändert trägt (sonst 406)."""
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        await channel.declare_queue(name, durable=True, arguments=arguments)
    finally:
        await connection.close()


async def test_0_11_0_fails_cleanly_on_a_queue_created_by_0_12(
    python_0110: str, rabbitmq_url: str, valkey_url: str, queue: str
) -> None:
    swarm = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue])
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
        env_extra={"COMPAT_EXPECT_SWARMQ": ROLLBACK_LIMIT_VERSION},
        python=python_0110,
    )
    try:
        output, _ = proc.communicate(timeout=100)
    except subprocess.TimeoutExpired:
        kill_subprocess(proc)
        raise

    assert proc.returncode != 0, f"swarmq {ROLLBACK_LIMIT_VERSION} started on a 0.12 queue: {output[-2000:]}"
    assert "PRECONDITION_FAILED" in output, output[-2000:]
    assert "x-dead-letter-strategy" in output, output[-2000:]
    assert await message_count(rabbitmq_url, queue) == _MESSAGES
    for name, arguments in _arguments_012(queue).items():
        await _declare_equivalent(rabbitmq_url, name, arguments)
