"""Unit tests for SwarmQ Client."""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from swarmq.client import Client
from swarmq.exceptions import (
    ResultNotReadyError,
    TaskCancelledError,
    TaskError,
    TaskNotFoundError,
)
from swarmq.serialization import decode_json, encode_result
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo


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
# 9b. get_result() with is_cancelled result raises TaskCancelledError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_result_cancelled_raises_task_cancelled_error(client, backend):
    result_data = encode_result({
        "task": {"id": "cancel-task", "queue": "default", "name": "t"},
        "result": None,
        "is_error": False,
        "is_cancelled": True,
        "error_message": "",
    })
    backend.get_result = AsyncMock(return_value=result_data)

    with pytest.raises(TaskCancelledError) as excinfo:
        await client.get_result("cancel-task", timeout=1)

    assert "cancel-task" in str(excinfo.value)
    assert isinstance(excinfo.value, TaskError)


@pytest.mark.asyncio
async def test_get_result_cancelled_takes_priority_over_error(client, backend):
    """When both is_cancelled=True and is_error=True, TaskCancelledError wins."""
    result_data = encode_result({
        "task": {"id": "dual", "queue": "default", "name": "t"},
        "result": None,
        "is_error": True,
        "is_cancelled": True,
        "error_message": "some error",
    })
    backend.get_result = AsyncMock(return_value=result_data)

    with pytest.raises(TaskCancelledError):
        await client.get_result("dual", timeout=1)


@pytest.mark.asyncio
async def test_get_result_legacy_result_without_is_cancelled_key(client, backend):
    """Backward-compat: results stored before R3.6 have no is_cancelled key."""
    result_data = encode_result({
        "task": {"id": "legacy", "queue": "default", "name": "t"},
        "result": 99,
        "is_error": False,
        # no is_cancelled key
    })
    backend.get_result = AsyncMock(return_value=result_data)

    result = await client.get_result("legacy", timeout=1)
    assert result == 99


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


# ---------------------------------------------------------------------------
# Middleware + Signals integration (Unit 5)
# ---------------------------------------------------------------------------


class TestClientMiddlewareAndSignals:
    """Client integration for Middleware hooks and Signals."""

    @pytest.fixture
    def middleware_chain(self):
        from swarmq.middleware import MiddlewareChain

        chain = MagicMock(spec=MiddlewareChain)
        chain.run_pre_enqueue = AsyncMock()
        chain.run_post_enqueue = AsyncMock()
        return chain

    @pytest.fixture
    def signal_dispatcher(self):
        dispatcher = MagicMock()
        dispatcher.dispatch = AsyncMock()
        return dispatcher

    @pytest.fixture
    def client_with_hooks(
        self, broker, backend, registry, middleware_chain, signal_dispatcher,
    ):
        return Client(
            broker=broker,
            backend=backend,
            registry=registry,
            middleware_chain=middleware_chain,
            signal_dispatcher=signal_dispatcher,
        )

    # ------------------------------------------------------------------
    # 1. pre_enqueue is called before broker.publish
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_pre_enqueue_called_before_publish(
        self, client_with_hooks, broker, middleware_chain,
    ):
        call_order = []
        middleware_chain.run_pre_enqueue = AsyncMock(
            side_effect=lambda msg: call_order.append("pre_enqueue")
        )
        broker.publish = AsyncMock(
            side_effect=lambda *a, **kw: call_order.append("publish")
        )

        await client_with_hooks.schedule("add", 1, 2, queue="math")

        assert call_order == ["pre_enqueue", "publish"]
        middleware_chain.run_pre_enqueue.assert_awaited_once()

    # ------------------------------------------------------------------
    # 2. post_enqueue is called after broker.publish
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_post_enqueue_called_after_publish(
        self, client_with_hooks, broker, middleware_chain,
    ):
        call_order = []
        broker.publish = AsyncMock(
            side_effect=lambda *a, **kw: call_order.append("publish")
        )
        middleware_chain.run_post_enqueue = AsyncMock(
            side_effect=lambda msg: call_order.append("post_enqueue")
        )

        await client_with_hooks.schedule("add", 1, 2, queue="math")

        assert call_order == ["publish", "post_enqueue"]
        middleware_chain.run_post_enqueue.assert_awaited_once()

    # ------------------------------------------------------------------
    # 3. TASK_ENQUEUED signal dispatched after publish with correct kwargs
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_task_enqueued_signal_dispatched_with_fields(
        self, client_with_hooks, broker, signal_dispatcher,
    ):
        task_id = await client_with_hooks.schedule("add", 1, 2, queue="math")

        signal_dispatcher.dispatch.assert_awaited_once()
        call = signal_dispatcher.dispatch.await_args
        # First positional arg is the Signal enum
        assert call[0][0] is Signal.TASK_ENQUEUED
        # kwargs contain task_id, task_name, queue
        assert call[1]["task_id"] == task_id
        assert call[1]["task_name"] == "add"
        assert call[1]["queue"] == "math"

    # ------------------------------------------------------------------
    # 4. pre_enqueue raises -> broker.publish NOT called, exception propagates
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_pre_enqueue_raises_blocks_publish(
        self, client_with_hooks, broker, middleware_chain,
        signal_dispatcher,
    ):
        middleware_chain.run_pre_enqueue = AsyncMock(
            side_effect=RuntimeError("veto")
        )

        with pytest.raises(RuntimeError, match="veto"):
            await client_with_hooks.schedule("add", 1, 2, queue="math")

        broker.publish.assert_not_awaited()
        middleware_chain.run_post_enqueue.assert_not_awaited()
        signal_dispatcher.dispatch.assert_not_awaited()

    # ------------------------------------------------------------------
    # 5. schedule_many: pre_enqueue called for each task
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_schedule_many_pre_enqueue_per_task(
        self, client_with_hooks, middleware_chain,
    ):
        tasks = [
            ("add", (1, 2), {}),
            ("add", (3, 4), {}),
            ("add", (5, 6), {}),
        ]
        await client_with_hooks.schedule_many(tasks, queue="math")

        assert middleware_chain.run_pre_enqueue.await_count == 3

    # ------------------------------------------------------------------
    # 6. schedule_many: post_enqueue and TASK_ENQUEUED called per task after gather
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_schedule_many_post_enqueue_and_signal_per_task(
        self, client_with_hooks, middleware_chain, signal_dispatcher,
    ):
        tasks = [
            ("add", (1, 2), {}),
            ("add", (3, 4), {}),
            ("add", (5, 6), {}),
        ]
        task_ids = await client_with_hooks.schedule_many(tasks, queue="math")

        assert middleware_chain.run_post_enqueue.await_count == 3
        assert signal_dispatcher.dispatch.await_count == 3
        dispatched_task_ids = [
            c[1]["task_id"] for c in signal_dispatcher.dispatch.await_args_list
        ]
        assert set(dispatched_task_ids) == set(task_ids)
        for call in signal_dispatcher.dispatch.await_args_list:
            assert call[0][0] is Signal.TASK_ENQUEUED
            assert call[1]["task_name"] == "add"
            assert call[1]["queue"] == "math"

    # ------------------------------------------------------------------
    # 7. message_dict contains all canonical fields
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_message_dict_contains_canonical_fields(
        self, client_with_hooks, middleware_chain,
    ):
        task_id = await client_with_hooks.schedule(
            "add", 10, 20, queue="math", extra="foo",
        )

        # pre_enqueue receives the canonical message_dict
        pre_msg = middleware_chain.run_pre_enqueue.await_args[0][0]
        assert pre_msg["task_id"] == task_id
        assert pre_msg["task_name"] == "add"
        assert pre_msg["queue"] == "math"
        assert pre_msg["args"] == [10, 20]
        assert pre_msg["kwargs"] == {"extra": "foo"}
        assert pre_msg["delivery_mode"] == "at_least_once"

        # post_enqueue receives the same canonical message_dict
        post_msg = middleware_chain.run_post_enqueue.await_args[0][0]
        assert post_msg["task_id"] == task_id
        assert post_msg["task_name"] == "add"
        assert post_msg["queue"] == "math"
        assert post_msg["args"] == [10, 20]
        assert post_msg["kwargs"] == {"extra": "foo"}
        assert post_msg["delivery_mode"] == "at_least_once"

    # ------------------------------------------------------------------
    # 8. Backward compat: no middleware/signals registered -> client works as before
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_backward_compat_no_hooks(self, broker, backend, registry):
        """Client without middleware_chain/signal_dispatcher works unchanged."""
        plain_client = Client(broker=broker, backend=backend, registry=registry)
        task_id = await plain_client.schedule("add", 1, 2, queue="math")

        parsed = uuid.UUID(task_id)
        assert parsed.version == 7
        broker.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_backward_compat_schedule_many_no_hooks(
        self, broker, backend, registry,
    ):
        plain_client = Client(broker=broker, backend=backend, registry=registry)
        tasks = [("add", (1, 2), {}), ("add", (3, 4), {})]
        task_ids = await plain_client.schedule_many(tasks, queue="math")

        assert len(task_ids) == 2
        assert broker.publish.await_count == 2

    # ------------------------------------------------------------------
    # 9. schedule_many: pre_enqueue raises on task N -> batch aborts, no publish
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_schedule_many_pre_enqueue_raises_aborts_batch(
        self, client_with_hooks, broker, middleware_chain, signal_dispatcher,
    ):
        # Pre_enqueue succeeds for first, raises on second
        middleware_chain.run_pre_enqueue = AsyncMock(
            side_effect=[None, RuntimeError("veto"), None]
        )

        tasks = [
            ("add", (1, 2), {}),
            ("add", (3, 4), {}),
            ("add", (5, 6), {}),
        ]

        with pytest.raises(RuntimeError, match="veto"):
            await client_with_hooks.schedule_many(tasks, queue="math")

        # No publishes happened (abort BEFORE gather)
        broker.publish.assert_not_awaited()
        # No post_enqueue, no signals
        middleware_chain.run_post_enqueue.assert_not_awaited()
        signal_dispatcher.dispatch.assert_not_awaited()

    # ------------------------------------------------------------------
    # 10. Factory middleware_chain: middleware added AFTER Client
    #     construction is visible on the next schedule() call.
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_add_middleware_after_client_construction(
        self, broker, backend, registry,
    ):
        """Factory reads a live list -> schedule() sees middleware added later.

        This is the primary regression test for the middleware_chain snapshot
        bug: before the fix, Client captured a MiddlewareChain snapshot at
        __init__ time and appending to the source list had no effect.
        """
        from swarmq.middleware import Middleware, MiddlewareChain

        # Live list — tests mutate this after Client construction.
        live_middlewares: list[Middleware] = []

        class Spy(Middleware):
            def __init__(self) -> None:
                self.pre_calls: list[dict] = []
                self.post_calls: list[dict] = []

            async def pre_enqueue(self, message: dict) -> None:
                self.pre_calls.append(message)

            async def post_enqueue(self, message: dict) -> None:
                self.post_calls.append(message)

        client = Client(
            broker=broker,
            backend=backend,
            registry=registry,
            # Factory: reads the live list every time schedule() runs.
            middleware_chain=lambda: MiddlewareChain(list(live_middlewares)),
        )

        # Step 1: list is empty -> no middleware fires.
        await client.schedule("add", 1, 2, queue="math")
        broker.publish.assert_awaited_once()

        # Step 2: register middleware AFTER Client construction.
        spy = Spy()
        live_middlewares.append(spy)

        await client.schedule("add", 3, 4, queue="math")

        # Spy saw the second schedule() but not the first.
        assert len(spy.pre_calls) == 1
        assert len(spy.post_calls) == 1
        assert spy.pre_calls[0]["task_name"] == "add"
        assert spy.pre_calls[0]["args"] == [3, 4]

    # ------------------------------------------------------------------
    # 11. Backward compat: passing a static MiddlewareChain still works.
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_client_accepts_static_middleware_chain(
        self, broker, backend, registry,
    ):
        """A real MiddlewareChain instance is wrapped in a lambda internally."""
        from swarmq.middleware import Middleware, MiddlewareChain

        class Spy(Middleware):
            def __init__(self) -> None:
                self.pre_calls: list[dict] = []

            async def pre_enqueue(self, message: dict) -> None:
                self.pre_calls.append(message)

        spy = Spy()
        static_chain = MiddlewareChain([spy])

        client = Client(
            broker=broker,
            backend=backend,
            registry=registry,
            middleware_chain=static_chain,
        )

        await client.schedule("add", 1, 2, queue="math")

        assert len(spy.pre_calls) == 1
        assert spy.pre_calls[0]["task_name"] == "add"
        # Internal factory returns the same chain every time.
        assert client._get_chain() is static_chain

    def test_client_rejects_invalid_middleware_chain_type(
        self, broker, backend, registry,
    ):
        """Client.__init__ raises TypeError including the received type name."""
        with pytest.raises(
            TypeError,
            match=r"middleware_chain must be.*got int",
        ):
            Client(
                broker=broker,
                backend=backend,
                registry=registry,
                middleware_chain=42,  # type: ignore[arg-type]
            )


# ---------------------------------------------------------------------------
# delivery_mode resolution in message_dict
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_resolves_delivery_mode_from_registry(broker, backend):
    """Registered task with FIRE_AND_FORGET -> message_dict has that value."""
    from swarmq.middleware import Middleware, MiddlewareChain

    captured: list[dict] = []

    class CaptureMiddleware(Middleware):
        async def pre_enqueue(self, message: dict) -> None:
            captured.append(dict(message))

    # Registry mock whose TaskInfo has FIRE_AND_FORGET delivery
    r = MagicMock()
    r.has = MagicMock(return_value=True)
    entry = MagicMock()
    entry.task_info = TaskInfo(
        name="fire",
        queues=["ff"],
        delivery=DeliveryMode.FIRE_AND_FORGET,
    )
    r.get_entry = MagicMock(return_value=entry)

    chain = MiddlewareChain([CaptureMiddleware()])
    client = Client(
        broker=broker,
        backend=backend,
        registry=r,
        middleware_chain=chain,
    )

    await client.schedule("fire", 1, 2)

    assert len(captured) == 1
    assert captured[0]["delivery_mode"] == "fire_and_forget"


@pytest.mark.asyncio
async def test_schedule_default_delivery_mode_without_registry(broker, backend):
    """No registry + explicit queue -> default delivery_mode is 'at_least_once'."""
    from swarmq.middleware import Middleware, MiddlewareChain

    captured: list[dict] = []

    class CaptureMiddleware(Middleware):
        async def pre_enqueue(self, message: dict) -> None:
            captured.append(dict(message))

    chain = MiddlewareChain([CaptureMiddleware()])
    client = Client(
        broker=broker,
        backend=backend,
        middleware_chain=chain,
    )

    await client.schedule("any.task", 1, queue="my-q")

    assert len(captured) == 1
    assert captured[0]["delivery_mode"] == "at_least_once"


# ---------------------------------------------------------------------------
# Client.cancel() — Phase 3b.3 / Unit 5
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_calls_backend_set_cancel_flag_and_returns_bool(
    client, backend,
):
    backend.set_cancel_flag = AsyncMock(return_value=True)

    result = await client.cancel("task-abc", ttl_ms=60_000)

    backend.set_cancel_flag.assert_awaited_once_with("task-abc", 60_000)
    assert result is True


@pytest.mark.asyncio
async def test_cancel_returns_false_when_already_cancelled(client, backend):
    backend.set_cancel_flag = AsyncMock(return_value=False)

    result = await client.cancel("task-abc")

    assert result is False


@pytest.mark.asyncio
async def test_cancel_default_ttl_is_24h(client, backend):
    backend.set_cancel_flag = AsyncMock(return_value=True)

    await client.cancel("task-abc")

    backend.set_cancel_flag.assert_awaited_once_with("task-abc", 86_400_000)


@pytest.mark.asyncio
async def test_cancel_rejects_empty_task_id(client):
    with pytest.raises(ValueError):
        await client.cancel("")


@pytest.mark.asyncio
async def test_cancel_propagates_backend_errors(client, backend):
    from swarmq.exceptions import BackendConnectionError

    backend.set_cancel_flag = AsyncMock(
        side_effect=BackendConnectionError("valkey down")
    )

    with pytest.raises(BackendConnectionError):
        await client.cancel("task-abc")


# ---------------------------------------------------------------------------
# schedule(delay=) / schedule(eta=) — Unit 9
# ---------------------------------------------------------------------------


@pytest.fixture
def _patched_schedule_store(monkeypatch):
    """Replace ScheduleStore with an AsyncMock so we can assert calls
    without needing a connected backend + Lua scripts."""
    from swarmq import client as client_mod

    store_instance = MagicMock()
    store_instance.add_schedule = AsyncMock()

    def _factory(backend):
        return store_instance

    monkeypatch.setattr(client_mod, "ScheduleStore", _factory)
    return store_instance


class TestClientScheduleDelayEta:
    async def test_schedule_without_delay_or_eta_takes_normal_path(
        self, client, broker, backend, _patched_schedule_store
    ):
        await client.schedule("add", 1, 2, queue="math")
        # Normal publish path, no ScheduleStore touch
        broker.publish.assert_awaited()
        _patched_schedule_store.add_schedule.assert_not_awaited()

    async def test_schedule_with_delay_writes_to_store(
        self, client, broker, backend, _patched_schedule_store
    ):
        tid = await client.schedule("add", 1, 2, queue="math", delay=60.0)

        # Normal publish is bypassed for deferred schedules
        broker.publish.assert_not_awaited()

        _patched_schedule_store.add_schedule.assert_awaited_once()
        definition = _patched_schedule_store.add_schedule.await_args.args[0]
        # schedule_name encodes the task_id so that one-shots are
        # discoverable in list_schedules
        assert definition.schedule_name == f"oneshot:{tid}"
        assert definition.cron is None
        assert definition.source == "cli"
        # fire_ts is now + delay*1000 — within a reasonable window
        import time
        now_ms = int(time.time() * 1000)
        assert now_ms + 55_000 <= definition.fire_ts_ms <= now_ms + 65_000

    async def test_schedule_with_eta_writes_to_store(
        self, client, broker, backend, _patched_schedule_store
    ):
        from datetime import UTC, datetime, timedelta
        eta = datetime.now(UTC) + timedelta(hours=2)
        await client.schedule("add", queue="math", eta=eta)

        _patched_schedule_store.add_schedule.assert_awaited_once()
        definition = _patched_schedule_store.add_schedule.await_args.args[0]
        expected_ms = int(eta.timestamp() * 1000)
        assert definition.fire_ts_ms == expected_ms

    async def test_schedule_with_both_delay_and_eta_raises(
        self, client, _patched_schedule_store
    ):
        from datetime import UTC, datetime, timedelta
        eta = datetime.now(UTC) + timedelta(seconds=60)
        with pytest.raises(ValueError, match="delay OR eta"):
            await client.schedule("add", queue="math", delay=1.0, eta=eta)

    async def test_schedule_with_negative_delay_raises(
        self, client, _patched_schedule_store
    ):
        with pytest.raises(ValueError, match="delay"):
            await client.schedule("add", queue="math", delay=-0.5)

    async def test_schedule_with_past_eta_raises(
        self, client, _patched_schedule_store
    ):
        from datetime import UTC, datetime, timedelta
        past = datetime.now(UTC) - timedelta(hours=1)
        with pytest.raises(ValueError, match="eta"):
            await client.schedule("add", queue="math", eta=past)

    async def test_schedule_with_delay_zero_uses_normal_publish(
        self, client, broker, backend, _patched_schedule_store
    ):
        """delay=0 is explicitly sofortiger publish, not a zero-delay
        scheduler detour (plan test-scenario)."""
        await client.schedule("add", queue="math", delay=0)
        broker.publish.assert_awaited()
        _patched_schedule_store.add_schedule.assert_not_awaited()

    async def test_schedule_reuses_existing_store(
        self, client, _patched_schedule_store
    ):
        """Lazy-init: second delay-schedule uses the cached store."""
        await client.schedule("add", queue="math", delay=1.0)
        await client.schedule("add", queue="math", delay=2.0)
        assert _patched_schedule_store.add_schedule.await_count == 2

    async def test_schedule_with_delay_without_connected_backend_raises(
        self, client, broker
    ):
        """If backend is not connected, lazy-init must not silently proceed
        (Fork 2 — clear RuntimeError with migration hint)."""
        # Ensure backend has no _client attribute (unconnected)
        # We use a fresh client with a plain Mock backend
        from swarmq.client import Client as _Client
        backend = MagicMock()
        backend._client = None
        c = _Client(broker=broker, backend=backend, registry=None)
        with pytest.raises(RuntimeError, match="connected"):
            await c.schedule("add", queue="math", delay=1.0)
