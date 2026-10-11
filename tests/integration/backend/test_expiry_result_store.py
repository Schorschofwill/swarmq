"""Expiry-Gate speichert das Fehlerergebnis wirklich (Bug-Fund aus U15).

Vor dem Fix übergab das Gate ``store_result`` ein dict statt Bytes: gegen
echtes Valkey warf SET, der Best-Effort-Catch schluckte den Fehler, und
``get_result`` eines abgelaufenen Tasks blieb für immer leer (Timeout statt
``TaskError("expired")``). Dieser Test läuft gegen echtes Valkey und pinnt:
nach dem Expiry-Pfad liegt ein dekodierbarer Fehler-Envelope im Store.
"""

from __future__ import annotations

import time
import uuid
from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.signals import SignalDispatcher
from swarmq.task import Task, TaskInfo
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.gates.expiry import ExpiryGate
from swarmq.worker.outcome import Terminal
from tests.support.messages import make_message

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


class ExpiringTask(Task):
    task_info = TaskInfo(name="int-expiry-task", queues=["default"], max_age=1)

    @override
    async def run(self) -> None:  # pragma: no cover — läuft nie (abgelaufen)
        return None


@pytest.fixture
async def backend(valkey_url):
    b = ValkeyBackend(valkey_url)
    await b.connect()
    try:
        yield b
    finally:
        await b.close()


async def test_expired_task_error_result_is_stored_and_decodable(backend):
    task_id = f"expiry-{uuid.uuid4().hex[:12]}"
    broker = MagicMock()
    broker.ack = AsyncMock()
    broker.dead_letter = AsyncMock()
    broker.redeliver = AsyncMock()
    registry = TaskRegistry()
    registry.register(ExpiringTask)
    config = SwarmQConfig(broker_url="amqp://unused", backend_url="valkey://unused", result_ttl=60)
    deps = GateDeps(
        broker=broker,
        backend=backend,
        config=config,
        signals=SignalDispatcher(),
        registry=registry,
        rate_limiter=lambda: None,
        lock_manager=lambda: None,
        finalize_schedfire=AsyncMock(),
        maybe_release_unique=AsyncMock(),
        extract_valid_workflow_id=lambda _msg: None,
        register_lock=lambda _hid, _tok: None,
    )
    gate = ExpiryGate(deps)
    msg = make_message(
        {
            "task": {"id": task_id, "queue": "default", "name": "int-expiry-task"},
            "args": [],
            "kwargs": {},
        },
        headers={HEADER_ENQUEUED_AT: int(time.time()) - 3600},
    )
    ctx = GateContext(
        task_id=task_id,
        task_name="int-expiry-task",
        queue="default",
        task_info=ExpiringTask.task_info,
        task_class=ExpiringTask,
    )

    outcome = await gate.apply(msg, ctx)

    assert outcome == Terminal.release_only()  # Expiry ist terminal (U19), gibt frei (BZ-22)
    raw = await backend.get_result(task_id)
    assert raw is not None, (
        "Expiry-Fehlerergebnis fehlt im Store — get_result() eines abgelaufenen Tasks würde bis zum Timeout hängen"
    )
    data = decode_result(raw)
    assert data["is_error"] is True
    assert "expired" in data["error_message"]
