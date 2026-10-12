"""Consumer-Wächter und Readiness im Worker (Plan 2026-10-08-1541, U6, R1/R2).

Der Wächter selbst ist in ``tests/unit/broker/test_consumer_watch.py``
getestet; hier geht es nur um das Einhängen in den Worker-Start.

Test-Liste:
1. ``/ready`` enthält den Consumer-Check über alle konsumierten Queues
   (mit Prioritäten das Tripel); eine fehlende Queue macht nicht bereit.
2. Nach dem Konsumstart läuft der Wächter im konfigurierten Takt und zählt
   Neuanmeldungen in die Metrik.
"""

from __future__ import annotations

import asyncio
import json
import socket
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.worker import Worker

pytestmark = pytest.mark.timeout(5)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _broker() -> MagicMock:
    b = MagicMock(spec=RabbitMQBroker)
    for name in ("connect", "declare_queue", "consume", "close", "cancel_consume"):
        setattr(b, name, AsyncMock())
    b.consumer_gaps = Mock(return_value={})
    b.start_consumer_watch = Mock()
    return b


def _backend() -> MagicMock:
    be = MagicMock(spec=ValkeyBackend)
    be.connect = AsyncMock()
    be.close = AsyncMock()
    be.ping = AsyncMock()
    return be


async def _http_get(port: int, path: str) -> tuple[int, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    status = int((await reader.readline()).split()[1])
    length = 0
    while (line := await reader.readline()) not in (b"\r\n", b""):
        key, _, value = line.decode().partition(":")
        if key.strip().lower() == "content-length":
            length = int(value)
    body = await reader.readexactly(length)
    writer.close()
    await writer.wait_closed()
    return status, body


async def _running(worker: Worker) -> asyncio.Task[None]:
    online = asyncio.Event()

    async def _online(**_kw: object) -> None:
        online.set()

    worker._signal_dispatcher.connect(Signal.WORKER_ONLINE, _online)
    task = asyncio.create_task(worker.start())
    await asyncio.wait_for(online.wait(), 2.0)
    return task


async def _stop(worker: Worker, task: asyncio.Task[None]) -> None:
    worker._request_shutdown()
    await asyncio.wait_for(task, 2.0)


async def test_ready_reports_every_consumed_queue_including_priorities():
    port = _free_port()
    config = SwarmQConfig(
        broker_url="amqp://x",
        backend_url="valkey://x",
        queues=["default"],
        priorities=True,
        health_port=port,
    )
    broker = _broker()
    broker.consumer_gaps.return_value = {"default.high": 0.0, "default": 0.0}
    worker = Worker(broker=broker, backend=_backend(), registry=TaskRegistry(), config=config)
    task = await _running(worker)
    try:
        status, body = await _http_get(port, "/ready")
    finally:
        await _stop(worker, task)

    assert status == 503
    assert json.loads(body)["consumers"] == {"ok": False, "error": "not consuming: default.low"}


async def test_watch_starts_with_the_configured_interval_and_counts_into_the_metric():
    config = SwarmQConfig(
        broker_url="amqp://x",
        backend_url="valkey://x",
        consumer_watch_interval_s=2.0,
        metrics_port=_free_port(),
    )
    broker = _broker()
    worker = Worker(broker=broker, backend=_backend(), registry=TaskRegistry(), config=config)
    task = await _running(worker)
    try:
        assert worker._metrics is not None
        broker.start_consumer_watch.assert_called_once_with(2.0, worker._metrics.count_consumer_reregistration)
        broker.consume.assert_awaited()
    finally:
        await _stop(worker, task)
