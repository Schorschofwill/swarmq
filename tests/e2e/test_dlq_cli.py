"""E2E tests for ``swarmq dlq`` subcommands (U1.4).

Full-stack: an in-process SwarmQ worker consumes a NoRetry-raising
task to populate {queue}.XQ, then the tests invoke ``swarmq dlq ...``
as a real subprocess against the same RabbitMQ + Valkey. Exercises the
CLI surface from the same side an operator would use it.

Verifies:
- list / inspect / retry / retry --all / purge happy paths
- drain-and-republish: list/retry scan works over many messages
- exit-code contract: 0 success, 2 config error, 3 not-found
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task, TaskInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
pytestmark = [pytest.mark.timeout(40)]


def _unique_queue() -> str:
    return f"e2e-dlq-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


def _run_cli(
    argv: list[str],
    rabbitmq_url: str,
    valkey_url: str,
) -> subprocess.CompletedProcess[str]:
    """Invoke ``swarmq <argv>`` as a subprocess; capture stdout/stderr."""
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = rabbitmq_url
    env["SWARMQ_BACKEND_URL"] = valkey_url
    env["PYTHONUNBUFFERED"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "swarmq.cli", *argv],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


async def _dlq_count(rabbitmq_url: str, queue: str) -> int:
    """Return the current message count in ``{queue}.XQ`` via passive
    declare (always returns fresh broker-side count)."""
    import aio_pika
    conn = await aio_pika.connect_robust(rabbitmq_url)
    try:
        ch = await conn.channel()
        decl = await ch.declare_queue(f"{queue}.XQ", passive=True)
        return decl.declaration_result.message_count
    finally:
        await conn.close()


async def _poison_queue(
    *,
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
    task_name: str,
    count: int,
) -> list[str]:
    """Schedule *count* tasks that always raise, wait until all are
    in the DLQ, and return the task_ids.

    Uses ``max_retries=0`` so each task lands in the DLQ immediately
    after its first failure (no retry backoff). Polls the broker-side
    DLQ count rather than relying on TASK_FAILED signal wiring — the
    count is the authoritative source for "message is in DLQ."
    """
    class PoisonTask(Task):
        # max_retries=1 keeps the retry-exhaustion DLQ path exercised
        # (since Härtung R2, max_retries=0 also DLQ-routes — the old
        # "silently stores and acks" gate is gone).
        task_info = TaskInfo(
            name=task_name, queues=[queue],
            max_retries=1,
            retry_backoff="fixed",
            retry_base_delay=0.1,
        )

        async def run(self) -> None:
            raise ValueError("poison")

    # Separate client + worker instances — same pattern as
    # tests/e2e/test_phase2.py::test_retry_exhaustion_dlq.
    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker, worker_backend, queues=[queue], concurrency=4,
    )

    task_ids: list[str] = []
    async with client_app:
        await client_app.broker.declare_queue(queue)
        worker_task = asyncio.create_task(worker_app.start_worker())
        try:
            # Let the worker install its consumer before the first
            # publish — avoids a race where the initial publishes
            # land before the worker is subscribed.
            await asyncio.sleep(0.5)
            for _ in range(count):
                tid = await client_app.schedule(task_name)
                task_ids.append(tid)
            # Poll DLQ count until all landed (max 15s).
            deadline = asyncio.get_running_loop().time() + 15.0
            while asyncio.get_running_loop().time() < deadline:
                if await _dlq_count(rabbitmq_url, queue) >= count:
                    break
                await asyncio.sleep(0.2)
            else:
                raise AssertionError(
                    f"DLQ never reached {count} messages "
                    f"(got {await _dlq_count(rabbitmq_url, queue)})"
                )
            await asyncio.sleep(0.3)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()
    return task_ids


async def test_dlq_list_finds_poisoned_messages(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    queue = _unique_queue()
    task_name = _task_name("poison")
    await _poison_queue(
        rabbitmq_url=rabbitmq_url, valkey_url=valkey_url,
        queue=queue, task_name=task_name, count=3,
    )

    result = _run_cli(
        ["dlq", "list", "--queue", queue, "--format", "json"],
        rabbitmq_url, valkey_url,
    )
    assert result.returncode == 0, (
        f"dlq list failed: rc={result.returncode} "
        f"stderr={result.stderr!r}"
    )
    data = json.loads(result.stdout)
    entries = data["entries"]
    assert len(entries) == 3
    names = {entry["task_name"] for entry in entries}
    assert names == {task_name}


async def test_dlq_list_empty_queue_returns_empty(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    queue = _unique_queue()
    # Declare the main queue (auto-creates the .XQ) but don't poison.
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    try:
        await broker.declare_queue(queue)
    finally:
        await broker.close()

    result = _run_cli(
        ["dlq", "list", "--queue", queue, "--format", "json"],
        rabbitmq_url, valkey_url,
    )
    assert result.returncode == 0
    data = json.loads(result.stdout)
    assert data["entries"] == []


async def test_dlq_inspect_shows_payload_and_retry_count(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    queue = _unique_queue()
    task_name = _task_name("inspect")
    task_ids = await _poison_queue(
        rabbitmq_url=rabbitmq_url, valkey_url=valkey_url,
        queue=queue, task_name=task_name, count=2,
    )
    tid = task_ids[0]

    result = _run_cli(
        ["dlq", "inspect", tid, "--queue", queue, "--format", "json"],
        rabbitmq_url, valkey_url,
    )
    assert result.returncode == 0, (
        f"dlq inspect failed: stderr={result.stderr!r}"
    )
    data = json.loads(result.stdout)
    assert data["task_id"] == tid
    assert data["task_name"] == task_name
    # max_retries=1 means the task hit its retry ceiling once — the
    # header records the final retry_count value (1) at DLQ time.
    headers = data.get("headers", {})
    retry_count = headers.get("x-swarmq-retry-count")
    assert retry_count in (1, "1"), (
        f"unexpected retry-count header: {retry_count!r}"
    )


async def test_dlq_inspect_unknown_task_id_exits_3(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    queue = _unique_queue()
    # Declare main queue so the XQ exists and the scan doesn't error.
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    try:
        await broker.declare_queue(queue)
    finally:
        await broker.close()

    result = _run_cli(
        ["dlq", "inspect", "does-not-exist", "--queue", queue],
        rabbitmq_url, valkey_url,
    )
    assert result.returncode == 3, (
        f"expected exit code 3 (not-found); got {result.returncode} "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )


async def test_dlq_retry_single_republishes_into_main_queue(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    queue = _unique_queue()
    task_name = _task_name("retry1")
    task_ids = await _poison_queue(
        rabbitmq_url=rabbitmq_url, valkey_url=valkey_url,
        queue=queue, task_name=task_name, count=3,
    )

    before_count = await _dlq_count(rabbitmq_url, queue)
    assert before_count == 3

    result = _run_cli(
        ["dlq", "retry", task_ids[0], "--queue", queue, "--format", "json"],
        rabbitmq_url, valkey_url,
    )
    assert result.returncode == 0, (
        f"dlq retry failed: stderr={result.stderr!r}"
    )
    # Give the broker a moment to settle the republish.
    await asyncio.sleep(0.3)
    after_count = await _dlq_count(rabbitmq_url, queue)
    assert after_count == 2, (
        f"expected 2 messages left in DLQ after retrying one; got "
        f"{after_count}"
    )


async def test_dlq_retry_all_republishes_every_message(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    queue = _unique_queue()
    task_name = _task_name("retryall")
    await _poison_queue(
        rabbitmq_url=rabbitmq_url, valkey_url=valkey_url,
        queue=queue, task_name=task_name, count=5,
    )

    result = _run_cli(
        ["dlq", "retry", "--all", "--queue", queue, "--format", "json"],
        rabbitmq_url, valkey_url,
    )
    assert result.returncode == 0, (
        f"dlq retry --all failed: stderr={result.stderr!r}"
    )
    await asyncio.sleep(0.5)
    after_count = await _dlq_count(rabbitmq_url, queue)
    assert after_count == 0


async def test_dlq_purge_empties_queue(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    queue = _unique_queue()
    task_name = _task_name("purge")
    await _poison_queue(
        rabbitmq_url=rabbitmq_url, valkey_url=valkey_url,
        queue=queue, task_name=task_name, count=4,
    )

    assert await _dlq_count(rabbitmq_url, queue) == 4

    result = _run_cli(
        ["dlq", "purge", "--queue", queue],
        rabbitmq_url, valkey_url,
    )
    assert result.returncode == 0, (
        f"dlq purge failed: rc={result.returncode} "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert await _dlq_count(rabbitmq_url, queue) == 0


async def test_dlq_list_scans_beyond_default_limit(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """Drain-and-republish invariant: list must surface all N messages
    even when N exceeds the implicit prefetch. Poisons 25 messages,
    asks for --limit 100, expects all 25 back."""
    queue = _unique_queue()
    task_name = _task_name("manydlq")
    await _poison_queue(
        rabbitmq_url=rabbitmq_url, valkey_url=valkey_url,
        queue=queue, task_name=task_name, count=25,
    )

    result = _run_cli(
        [
            "dlq", "list", "--queue", queue,
            "--limit", "100", "--format", "json",
        ],
        rabbitmq_url, valkey_url,
    )
    assert result.returncode == 0, (
        f"dlq list failed: stderr={result.stderr!r}"
    )
    data = json.loads(result.stdout)
    entries = data["entries"]
    assert len(entries) == 25, (
        f"drain-and-republish scan lost messages: got {len(entries)}/25"
    )
