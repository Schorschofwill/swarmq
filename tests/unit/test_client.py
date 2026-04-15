"""Unit tests for SwarmQ Client."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from swarmq.client import Client
from swarmq.exceptions import ResultNotReadyError, TaskError, TaskNotFoundError
from swarmq.serialization import decode_json, encode_result
from swarmq.task import Task, TaskInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def broker():
    b = AsyncMock()
    b.publish = AsyncMock()
    return b


@pytest.fixture
def backend():
    b = AsyncMock()
    b.get_result = AsyncMock(return_value=None)
    return b


@pytest.fixture
def registry():
    r = MagicMock()
    r.has = MagicMock(return_value=True)
    entry = MagicMock()
    entry.task_info = TaskInfo(name="add", queues=["math"])
    r.get_entry = MagicMock(return_value=entry)
    return r


@pytest.fixture
def client(broker, backend, registry):
    return Client(broker=broker, backend=backend, registry=registry)


@pytest.fixture
def client_no_registry(broker, backend):
    return Client(broker=broker, backend=backend)


# ---------------------------------------------------------------------------
# 1. schedule() with explicit queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schedule_explicit_queue_generates_uuid7_publishes_returns_id(
    client, broker,
):
    task_id = await client.schedule("add", 1, 2, queue="math")

    # Returns a valid UUID string
    parsed = uuid.UUID(task_id)
    assert parsed.version == 7

    # Broker was called exactly once
    broker.publish.assert_awaited_once()
    call_args = broker.publish.await_args
    assert call_args[0][0] == "math"  # queue name

    # Message is valid JSON with correct structure
    payload = decode_json(call_args[0][1])
    assert payload["task"]["id"] == task_id
    assert payload["task"]["queue"] == "math"
    assert payload["task"]["name"] == "add"
    assert payload["args"] == [1, 2]
    assert payload["kwargs"] == {}


# ---------------------------------------------------------------------------
# 2. schedule() with registry resolves queue from TaskInfo
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schedule_resolves_queue_from_registry(client, broker, registry):
    task_id = await client.schedule("add", 1, 2)

    call_args = broker.publish.await_args
    assert call_args[0][0] == "math"  # resolved from TaskInfo.queues[0]
    payload = decode_json(call_args[0][1])
    assert payload["task"]["queue"] == "math"


# ---------------------------------------------------------------------------
# 3. get_result() finds result, deserializes correctly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_returns_deserialized_value(client, backend):
    result_data = encode_result({"result": 42, "is_error": False})
    backend.get_result = AsyncMock(return_value=result_data)

    value = await client.get_result("some-task-id", timeout=1)
    assert value == 42


# ---------------------------------------------------------------------------
# 4. run() combines schedule() and get_result()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_combines_schedule_and_get_result(client, broker, backend):
    result_data = encode_result({"result": 3, "is_error": False})
    backend.get_result = AsyncMock(return_value=result_data)

    value = await client.run("add", 1, 2, queue="math", timeout=1)
    assert value == 3
    broker.publish.assert_awaited_once()
    backend.get_result.assert_awaited()


# ---------------------------------------------------------------------------
# 5. Task message has correct JSON format with queue field
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_task_message_format(client, broker):
    await client.schedule("add", 10, b=20, queue="math")

    call_args = broker.publish.await_args
    payload = decode_json(call_args[0][1])

    assert set(payload.keys()) == {"task", "args", "kwargs"}
    assert set(payload["task"].keys()) == {"id", "queue", "name"}
    assert payload["args"] == [10]
    assert payload["kwargs"] == {"b": 20}


# ---------------------------------------------------------------------------
# 6. schedule() without queue and without registry raises error
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schedule_no_queue_no_registry_raises(client_no_registry):
    with pytest.raises(ValueError, match="queue"):
        await client_no_registry.schedule("add", 1, 2)


# ---------------------------------------------------------------------------
# 7. schedule() with registry but unknown task name raises TaskNotFoundError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schedule_unknown_task_with_registry_raises(client, registry):
    registry.has.return_value = False

    with pytest.raises(TaskNotFoundError):
        await client.schedule("nonexistent", queue="q")


# ---------------------------------------------------------------------------
# 8. get_result() timeout raises ResultNotReadyError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_timeout_raises(client, backend):
    backend.get_result = AsyncMock(return_value=None)

    with pytest.raises(ResultNotReadyError):
        await client.get_result("missing-task", timeout=0.1, polling=0.05)


# ---------------------------------------------------------------------------
# 9. get_result() with error result raises TaskError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_error_raises_task_error(client, backend):
    result_data = encode_result({
        "result": None,
        "is_error": True,
        "error_message": "division by zero",
    })
    backend.get_result = AsyncMock(return_value=result_data)

    with pytest.raises(TaskError, match="division by zero"):
        await client.get_result("err-task", timeout=1)


# ---------------------------------------------------------------------------
# 10. get_result() polls multiple times before result available
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_polls_multiple_times(client, backend):
    result_data = encode_result({"result": "ok", "is_error": False})
    backend.get_result = AsyncMock(
        side_effect=[None, None, result_data],
    )

    value = await client.get_result("task-id", timeout=2, polling=0.05)
    assert value == "ok"
    assert backend.get_result.await_count == 3


# ---------------------------------------------------------------------------
# 11. schedule() with empty args/kwargs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schedule_empty_args_kwargs(client, broker):
    task_id = await client.schedule("add", queue="math")

    payload = decode_json(broker.publish.await_args[0][1])
    assert payload["args"] == []
    assert payload["kwargs"] == {}
    assert payload["task"]["id"] == task_id


# ---------------------------------------------------------------------------
# 12. get_result(timeout=0) checks once, raises if not found
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_timeout_zero_checks_once(client, backend):
    backend.get_result = AsyncMock(return_value=None)

    with pytest.raises(ResultNotReadyError):
        await client.get_result("task-id", timeout=0)

    backend.get_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_result_timeout_zero_returns_if_found(client, backend):
    result_data = encode_result({"result": 99, "is_error": False})
    backend.get_result = AsyncMock(return_value=result_data)

    value = await client.get_result("task-id", timeout=0)
    assert value == 99
    backend.get_result.assert_awaited_once()


# ---------------------------------------------------------------------------
# 13. Client-Modus: without registry, no validation, explicit queue works
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_client_mode_no_registry_explicit_queue(
    client_no_registry, broker,
):
    task_id = await client_no_registry.schedule(
        "any.task.name", "arg1", queue="my-queue",
    )

    parsed = uuid.UUID(task_id)
    assert parsed.version == 7

    call_args = broker.publish.await_args
    assert call_args[0][0] == "my-queue"
    payload = decode_json(call_args[0][1])
    assert payload["task"]["name"] == "any.task.name"
