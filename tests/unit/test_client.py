"""Unit tests for SwarmQ Client."""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, Mock, patch

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
    # Pub/Sub mock: create_pubsub returns a mock PubSub that times out by default
    mock_pubsub = AsyncMock()
    mock_pubsub.get_message = AsyncMock(return_value=None)  # no notification
    b.create_pubsub = Mock(return_value=mock_pubsub)
    b._mock_pubsub = mock_pubsub  # expose for test assertions
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


# ---------------------------------------------------------------------------
# 14. Pub/Sub happy path: notification received -> result fetched immediately
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_pubsub_notification_fetches_result(client, backend):
    """Pub/Sub notification received -> GET fetches result."""
    result_data = encode_result({"result": "pubsub-ok", "is_error": False})
    # Initial check returns None, then after notification returns result
    backend.get_result = AsyncMock(side_effect=[None, result_data])
    # Mock pubsub to return a message on first get_message call
    backend._mock_pubsub.get_message = AsyncMock(
        return_value={"type": "message", "data": b"done"}
    )

    value = await client.get_result("task-pub", timeout=5)
    assert value == "pubsub-ok"
    backend.create_pubsub.assert_called_once()
    # First call is the initial check, second is after notification
    assert backend.get_result.await_count == 2


# ---------------------------------------------------------------------------
# 15. Pub/Sub happy path: result already exists -> initial check finds it
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_initial_check_finds_result(client, backend):
    """Result already in backend -> returned before Pub/Sub is attempted."""
    result_data = encode_result({"result": "fast", "is_error": False})
    backend.get_result = AsyncMock(return_value=result_data)

    value = await client.get_result("task-fast", timeout=5)
    assert value == "fast"
    # Pub/Sub subscribe was called but result was found before waiting
    backend.get_result.assert_awaited_once()


# ---------------------------------------------------------------------------
# 16. Pub/Sub miss -> polling fallback finds result
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_pubsub_miss_falls_back_to_polling(client, backend):
    """Pub/Sub times out -> polling loop finds result."""
    result_data = encode_result({"result": "polled", "is_error": False})
    # Initial check: None, pub/sub times out, polling finds result eventually
    # Need enough None values for 1s pub/sub wait + polling attempts
    side_effects = [None] * 50 + [result_data]
    backend.get_result = AsyncMock(side_effect=side_effects)
    # Pub/Sub get_message returns None (timeout) — already the default from fixture

    value = await client.get_result("task-poll", timeout=5)
    assert value == "polled"
    assert backend.get_result.await_count >= 2


# ---------------------------------------------------------------------------
# 17. Error-result -> TaskError raised (same as before, through new flow)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_pubsub_error_result_raises_task_error(client, backend):
    """Error result via Pub/Sub path still raises TaskError."""
    result_data = encode_result({
        "result": None,
        "is_error": True,
        "error_message": "boom",
    })
    # Initial check returns None, after Pub/Sub notification GET returns error result
    backend.get_result = AsyncMock(side_effect=[None, result_data, result_data])
    backend._mock_pubsub.get_message = AsyncMock(
        return_value={"type": "message", "data": b"done"}
    )

    with pytest.raises(TaskError, match="boom"):
        await client.get_result("task-err", timeout=5)


# ---------------------------------------------------------------------------
# 18. timeout=0 -> single GET, no Pub/Sub (backward compatible)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_timeout_zero_no_pubsub(client, backend):
    """timeout=0 does a single GET without touching Pub/Sub."""
    backend.get_result = AsyncMock(return_value=None)

    with pytest.raises(ResultNotReadyError):
        await client.get_result("task-id", timeout=0)

    backend.get_result.assert_awaited_once()
    backend.create_pubsub.assert_not_called()


# ---------------------------------------------------------------------------
# 19. Overall timeout -> ResultNotReadyError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_overall_timeout_raises(client, backend):
    """Full timeout exhausted -> ResultNotReadyError."""
    backend.get_result = AsyncMock(return_value=None)

    with pytest.raises(ResultNotReadyError):
        await client.get_result("task-timeout", timeout=0.05)


# ---------------------------------------------------------------------------
# 20. subscribe_and_wait returns False (Pub/Sub failed) -> polling fallback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_pubsub_failure_falls_back(client, backend):
    """Pub/Sub subscribe failure triggers polling fallback."""
    result_data = encode_result({"result": "recovered", "is_error": False})
    # Initial check None, then polling finds it
    backend.get_result = AsyncMock(side_effect=[None, result_data])
    # Make pubsub.subscribe raise — simulates Pub/Sub unavailable
    mock_pubsub = AsyncMock()
    mock_pubsub.subscribe = AsyncMock(side_effect=Exception("pubsub unavailable"))
    backend.create_pubsub = Mock(return_value=mock_pubsub)

    value = await client.get_result("task-recover", timeout=5)
    assert value == "recovered"


# ---------------------------------------------------------------------------
# 21. CancelledError during Pub/Sub -> propagates cleanly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_cancelled_error_propagates(client, backend):
    """CancelledError during Pub/Sub wait propagates without catching."""
    backend.get_result = AsyncMock(return_value=None)
    # Make get_message raise CancelledError
    backend._mock_pubsub.get_message = AsyncMock(side_effect=asyncio.CancelledError)

    with pytest.raises(asyncio.CancelledError):
        await client.get_result("task-cancel", timeout=5)


# ---------------------------------------------------------------------------
# 22. schedule_many() publishes N tasks and returns N task_ids
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schedule_many_returns_ids_and_publishes(client, broker):
    tasks = [
        ("add", (1, 2), {}),
        ("add", (3, 4), {"extra": 5}),
        ("add", (5, 6), {}),
    ]
    task_ids = await client.schedule_many(tasks, queue="math")

    assert len(task_ids) == 3
    for tid in task_ids:
        parsed = uuid.UUID(tid)
        assert parsed.version == 7

    assert broker.publish.await_count == 3

    # Verify each publish got the right queue and task_id
    for i, call in enumerate(broker.publish.await_args_list):
        assert call[0][0] == "math"
        payload = decode_json(call[0][1])
        assert payload["task"]["id"] == task_ids[i]
        assert payload["task"]["name"] == "add"


# ---------------------------------------------------------------------------
# 23. schedule_many([]) returns empty list, no publishes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schedule_many_empty_list(client, broker):
    task_ids = await client.schedule_many([], queue="math")

    assert task_ids == []
    broker.publish.assert_not_awaited()


# ---------------------------------------------------------------------------
# 24. get_results() returns all results in order
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_results_returns_all_in_order(client, backend):
    data_a = encode_result({"result": "alpha", "is_error": False})
    data_b = encode_result({"result": "beta", "is_error": False})
    data_c = encode_result({"result": "gamma", "is_error": False})

    # First poll: none ready; second poll: all ready
    backend.get_results_batch = AsyncMock(
        side_effect=[
            [None, None, None],
            [data_a, data_b, data_c],
        ]
    )

    results = await client.get_results(["a", "b", "c"], timeout=2, poll_interval=0.01)
    assert results == ["alpha", "beta", "gamma"]


# ---------------------------------------------------------------------------
# 25. get_results() timeout raises ResultNotReadyError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_results_timeout_raises(client, backend):
    backend.get_results_batch = AsyncMock(return_value=[None, None])

    with pytest.raises(ResultNotReadyError):
        await client.get_results(["x", "y"], timeout=0.05, poll_interval=0.01)


# ---------------------------------------------------------------------------
# 26. schedule_many() resolves queue from registry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schedule_many_resolves_queue_from_registry(client, broker, registry):
    tasks = [("add", (1, 2), {})]
    task_ids = await client.schedule_many(tasks)

    assert len(task_ids) == 1
    call_args = broker.publish.await_args
    assert call_args[0][0] == "math"  # resolved from TaskInfo.queues[0]
    payload = decode_json(call_args[0][1])
    assert payload["task"]["queue"] == "math"
