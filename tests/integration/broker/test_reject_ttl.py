"""Abgelehnte Nachrichten mit Ablaufzeit im ``.XQ`` (Plan 2026-10-12-1105, MR 8, P5b).

Der Worker lehnt Nachrichten ab, die er nicht ausfuehren kann. Bis 0.12
gingen sie per ``nack(requeue=False)`` ueber den DLX ohne Ablaufzeit ins
``.XQ`` und hielten dort das Verfallen am Kopf der Queue auf. Jetzt legt
der Worker eine Kopie mit der Dead-Letter-Frist und dem Ablage-Stempel ab;
``swarmq dlq list`` zeigt den Grund in ``ERROR_PREVIEW``.

Test-Liste:
- [x] kaputte Nachricht -> Kopie im ``.XQ`` mit ``expiration`` aus ``dead_letter_ttl_s`` und
      ``x-swarmq-dead-lettered-at``; Haupt-Queue leer; ``swarmq dlq list`` zeigt ``undecodable: ...``
- [x] Body ueber ``max_message_body_bytes`` (E6 A, Spike S1) -> ebenso, Grund ``body_too_large``
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import aio_pika
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_DEAD_LETTERED_AT
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(40)]

REPO_ROOT = Path(__file__).resolve().parents[3]
_TTL_S = 7200.0
_CAP = 1024

Reject = Callable[[bytes], Awaitable[aio_pika.abc.AbstractIncomingMessage]]


async def _depth(rabbitmq_url: str, queue: str) -> int:
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        return int(declared.declaration_result.message_count or 0)
    finally:
        await connection.close()


async def _first_xq_message(rabbitmq_url: str, queue: str) -> aio_pika.abc.AbstractIncomingMessage:
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        xq = await channel.get_queue(f"{queue}.XQ", ensure=False)
        message = await xq.get(no_ack=False, fail=True)
        assert message is not None
        await message.nack(requeue=True)
        return message
    finally:
        await connection.close()


def _dlq_list(queue: str, rabbitmq_url: str, valkey_url: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "SWARMQ_BROKER_URL": rabbitmq_url, "SWARMQ_BACKEND_URL": valkey_url}
    return subprocess.run(
        [sys.executable, "-m", "swarmq.cli", "dlq", "list", "--queue", queue],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


@pytest.fixture
async def queue() -> str:
    return f"reject-ttl-{uuid.uuid4().hex[:10]}"


@pytest.fixture
async def reject(rabbitmq_url: str, valkey_url: str, queue: str) -> AsyncIterator[Reject]:
    """Laufender Worker auf *queue*; publiziert einen Body und wartet, bis er im ``.XQ`` liegt."""
    producer = RabbitMQBroker(rabbitmq_url)
    await producer.connect()
    await producer.declare_queue(queue)
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    app = SwarmQ(worker_broker, worker_backend, queues=[queue], dead_letter_ttl_s=_TTL_S, max_message_body_bytes=_CAP)
    worker_task: asyncio.Task[Any] = asyncio.create_task(app.start_worker())

    async def _reject(body: bytes) -> aio_pika.abc.AbstractIncomingMessage:
        await producer.publish(queue, body)

        async def in_xq() -> bool:
            return await _depth(rabbitmq_url, f"{queue}.XQ") == 1

        assert await wait_until(in_xq, timeout=15), "rejected message never reached the .XQ"
        return await _first_xq_message(rabbitmq_url, queue)

    try:
        yield _reject
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()
        await delete_queue_family(rabbitmq_url, queue)
        await producer.close()


async def test_undecodable_message_lands_in_xq_with_expiration(
    reject: Reject, queue: str, rabbitmq_url: str, valkey_url: str
) -> None:
    copy = await reject(b"not json {{{")

    assert copy.expiration == _TTL_S
    headers = copy.headers or {}
    assert isinstance(headers[HEADER_DEAD_LETTERED_AT], int)
    assert str(headers["x-swarmq-traceback"]).startswith("undecodable: ")
    assert "x-death" not in headers
    assert await _depth(rabbitmq_url, queue) == 0

    listed = _dlq_list(queue, rabbitmq_url, valkey_url)
    assert listed.returncode == 0, listed.stderr
    assert "undecodable: " in listed.stdout


async def test_body_over_cap_lands_in_xq_with_expiration(reject: Reject, queue: str, rabbitmq_url: str) -> None:
    body = b'{"task": {"id": "t-big", "name": "big", "queue": "q"}, "args": ["' + b"x" * _CAP + b'"], "kwargs": {}}'

    copy = await reject(body)

    assert copy.body == body
    assert copy.expiration == _TTL_S
    headers = copy.headers or {}
    assert isinstance(headers[HEADER_DEAD_LETTERED_AT], int)
    assert str(headers["x-swarmq-traceback"]) == f"body_too_large: body {len(body)} bytes over cap {_CAP}"
    assert await _depth(rabbitmq_url, queue) == 0
