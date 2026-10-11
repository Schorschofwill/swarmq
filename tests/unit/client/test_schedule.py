"""Client-Schedule-Tests (Plan U10) — verbatim aus ``test_client.py``:
schedule/schedule_many, Delay/ETA, Unique/Dedup, Middleware+Signale."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.client import Client
from swarmq.exceptions import (
    TaskNotFoundError,
)
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import DeliveryMode, Task, TaskInfo

# ---------------------------------------------------------------------------
# Fixtures
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


@pytest.mark.asyncio
async def test_schedule_resolves_queue_from_registry(client, broker, registry):
    await client.schedule("add", 1, 2)

    call_args = broker.publish.await_args
    assert call_args[0][0] == "math"  # resolved from TaskInfo.queues[0]
    payload = decode_json(call_args[0][1])
    assert payload["task"]["queue"] == "math"


@pytest.mark.asyncio
async def test_task_message_format(client, broker):
    await client.schedule("add", 10, b=20, queue="math")

    call_args = broker.publish.await_args
    payload = decode_json(call_args[0][1])

    assert set(payload.keys()) == {"task", "args", "kwargs"}
    assert set(payload["task"].keys()) == {"id", "queue", "name"}
    assert payload["args"] == [10]
    assert payload["kwargs"] == {"b": 20}


@pytest.mark.asyncio
async def test_schedule_no_queue_no_registry_raises(client_no_registry):
    with pytest.raises(ValueError, match="queue"):
        await client_no_registry.schedule("add", 1, 2)


@pytest.mark.asyncio
async def test_schedule_unknown_task_with_registry_raises(client, registry):
    registry.has.return_value = False

    with pytest.raises(TaskNotFoundError):
        await client.schedule("nonexistent", queue="q")


@pytest.mark.asyncio
async def test_schedule_empty_args_kwargs(client, broker):
    task_id = await client.schedule("add", queue="math")

    payload = decode_json(broker.publish.await_args[0][1])
    assert payload["args"] == []
    assert payload["kwargs"] == {}
    assert payload["task"]["id"] == task_id


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


@pytest.mark.asyncio
async def test_schedule_many_empty_list(client, broker):
    task_ids = await client.schedule_many([], queue="math")

    assert task_ids == []
    broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_schedule_many_resolves_queue_from_registry(client, broker, registry):
    tasks = [("add", (1, 2), {})]
    task_ids = await client.schedule_many(tasks)

    assert len(task_ids) == 1
    call_args = broker.publish.await_args
    assert call_args[0][0] == "math"  # resolved from TaskInfo.queues[0]
    payload = decode_json(call_args[0][1])
    assert payload["task"]["queue"] == "math"


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


@pytest.fixture
def _patched_schedule_store(monkeypatch):
    """Replace ScheduleStore with an AsyncMock so we can assert calls
    without needing a connected backend + Lua scripts."""
    from swarmq.client import schedule as client_mod

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

    async def test_schedule_with_delay_persists_returned_task_id(
        self, client, _patched_schedule_store
    ):
        # BZ-02 (Plan 0.12.0, MR 5): der Leader feuert unter dieser ID.
        tid = await client.schedule("add", 1, 2, queue="math", delay=60.0)

        definition = _patched_schedule_store.add_schedule.await_args.args[0]
        assert definition.task_id == tid

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
        from unittest.mock import PropertyMock

        from swarmq.client import Client as _Client
        from swarmq.exceptions import BackendConnectionError
        backend = MagicMock()
        # U18: die Fassade wirft vor connect() auf ``.schedules``
        type(backend).schedules = PropertyMock(side_effect=BackendConnectionError("not connected"))
        c = _Client(broker=broker, backend=backend, registry=None)
        with pytest.raises(RuntimeError, match="connected"):
            await c.schedule("add", queue="math", delay=1.0)


class TestClientUniqueSchedule:
    """Producer-side SETNX dedup in Client.schedule()."""

    @pytest.fixture
    def backend_setnx(self, backend):
        """Backend variant with claim_unique mocked — default NX granted."""
        backend.claim_unique = AsyncMock(return_value=None)  # NX success
        return backend

    @pytest.fixture
    def client_setnx(self, broker, backend_setnx, registry):
        return Client(broker=broker, backend=backend_setnx, registry=registry)

    async def test_schedule_without_unique_for_is_unchanged(
        self, client_setnx, broker, backend_setnx,
    ):
        """No unique_for kwarg + TaskInfo default is None → no SETNX call,
        only R4.4 enqueued-at header (no dedup headers)."""
        await client_setnx.schedule("add", 1, 2, queue="math")
        broker.publish.assert_awaited_once()
        backend_setnx.claim_unique.assert_not_awaited()
        call_kwargs = broker.publish.await_args.kwargs
        headers = call_kwargs.get("headers")
        assert headers is not None
        assert set(headers.keys()) == {"x-swarmq-enqueued-at"}
        assert isinstance(headers["x-swarmq-enqueued-at"], int)

    async def test_schedule_with_unique_for_calls_setnx_and_sets_header(
        self, client_setnx, broker, backend_setnx,
    ):
        task_id = await client_setnx.schedule(
            "add", 1, 2, queue="math", unique_for=60,
        )
        backend_setnx.claim_unique.assert_awaited_once()
        setnx_call = backend_setnx.claim_unique.await_args
        dedup_hash = setnx_call[0][0]
        stored_task_id = setnx_call[0][1]
        ttl_ms = setnx_call[0][2]
        assert len(dedup_hash) == 64  # sha256 hex
        assert stored_task_id == task_id
        assert ttl_ms == 60_000
        # Header set on publish
        publish_kwargs = broker.publish.await_args.kwargs
        assert publish_kwargs["headers"]["x-swarmq-unique-hash"] == dedup_hash
        assert publish_kwargs["headers"]["x-swarmq-unique-until"] == "completion"

    async def test_schedule_duplicate_returns_existing_task_id(
        self, client_setnx, broker, backend_setnx,
    ):
        """Silent dedup: schedule() returns existing task_id, does not publish."""
        backend_setnx.claim_unique = AsyncMock(return_value="t-existing")
        task_id = await client_setnx.schedule(
            "add", 1, 2, queue="math", unique_for=60,
        )
        assert task_id == "t-existing"
        broker.publish.assert_not_awaited()

    async def test_schedule_duplicate_emits_signal(
        self, client_setnx, broker, backend_setnx,
    ):
        """TASK_DUPLICATE_DETECTED signal emitted with expected payload."""
        from swarmq.signals import Signal, SignalDispatcher

        dispatcher = SignalDispatcher()
        received: list[dict] = []

        @dispatcher.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def handler(**kwargs):
            received.append(kwargs)

        backend_setnx.claim_unique = AsyncMock(return_value="t-win")
        client = Client(
            broker=broker, backend=backend_setnx,
            registry=client_setnx._registry,
            signal_dispatcher=dispatcher,
        )
        returned = await client.schedule(
            "add", 1, 2, queue="math", unique_for=60,
        )
        assert returned == "t-win"
        assert len(received) == 1
        assert received[0]["existing_task_id"] == "t-win"
        assert received[0]["task_name"] == "add"
        assert received[0]["source"] == "client.schedule"
        assert len(received[0]["dedup_hash"]) == 64

    async def test_schedule_per_call_override_none_disables_dedup(
        self, client_setnx, broker, backend_setnx,
    ):
        """Per-call unique_for=None overrides TaskInfo default."""
        # Registry fixture's task_info has no unique_for (None by default).
        # Use explicit None — same outcome.
        await client_setnx.schedule(
            "add", 1, 2, queue="math", unique_for=None,
        )
        backend_setnx.claim_unique.assert_not_awaited()

    async def test_schedule_unique_for_uses_taskinfo_default(
        self, broker, backend_setnx,
    ):
        """When kwarg is _MISSING (not passed), use TaskInfo.unique_for."""
        # Build registry with unique_for=30 TaskInfo default.
        registry = MagicMock()
        registry.has = MagicMock(return_value=True)
        entry = MagicMock()
        entry.task_info = TaskInfo(
            name="add", queues=["math"], unique_for=30, unique_until="start",
        )
        registry.get_entry = MagicMock(return_value=entry)

        client = Client(broker=broker, backend=backend_setnx, registry=registry)
        await client.schedule("add", 1, queue="math")

        backend_setnx.claim_unique.assert_awaited_once()
        assert backend_setnx.claim_unique.await_args[0][2] == 30_000
        publish_kwargs = broker.publish.await_args.kwargs
        assert publish_kwargs["headers"]["x-swarmq-unique-until"] == "start"

    async def test_schedule_client_modus_ignores_taskinfo_default(
        self, broker, backend_setnx,
    ):
        """Client-Modus (registry=None) can't resolve TaskInfo — _MISSING → None."""
        client = Client(broker=broker, backend=backend_setnx, registry=None)
        await client.schedule("add", 1, queue="math")  # no unique_for
        backend_setnx.claim_unique.assert_not_awaited()

    async def test_schedule_client_modus_explicit_unique_for_works(
        self, broker, backend_setnx,
    ):
        """Client-Modus still honors explicit per-call unique_for."""
        client = Client(broker=broker, backend=backend_setnx, registry=None)
        await client.schedule(
            "add", 1, queue="math", unique_for=60, unique_until="start",
        )
        backend_setnx.claim_unique.assert_awaited_once()
        publish_kwargs = broker.publish.await_args.kwargs
        assert publish_kwargs["headers"]["x-swarmq-unique-until"] == "start"

    async def test_schedule_per_call_unique_until_overrides_taskinfo(
        self, broker, backend_setnx,
    ):
        """Per-call unique_until overrides TaskInfo default."""
        registry = MagicMock()
        registry.has = MagicMock(return_value=True)
        entry = MagicMock()
        entry.task_info = TaskInfo(
            name="add", queues=["math"], unique_for=30, unique_until="completion",
        )
        registry.get_entry = MagicMock(return_value=entry)

        client = Client(broker=broker, backend=backend_setnx, registry=registry)
        await client.schedule("add", 1, queue="math", unique_until="start")
        publish_kwargs = broker.publish.await_args.kwargs
        assert publish_kwargs["headers"]["x-swarmq-unique-until"] == "start"

    async def test_schedule_same_args_produce_stable_hash(
        self, client_setnx, backend_setnx,
    ):
        """Hash must be stable across calls with identical args+kwargs."""
        captured_hashes: list[str] = []

        async def capture_setnx(h, _tid, _ttl, **_kw):
            captured_hashes.append(h)
            return None

        backend_setnx.claim_unique = AsyncMock(side_effect=capture_setnx)
        await client_setnx.schedule("add", 1, 2, queue="math", unique_for=60)
        await client_setnx.schedule("add", 1, 2, queue="math", unique_for=60)
        assert captured_hashes[0] == captured_hashes[1]

    async def test_schedule_different_kwargs_order_same_hash(
        self, client_setnx, backend_setnx,
    ):
        """kwarg insertion order must not affect hash (canonical_encode key-sort)."""
        captured_hashes: list[str] = []

        async def capture_setnx(h, _tid, _ttl, **_kw):
            captured_hashes.append(h)
            return None

        backend_setnx.claim_unique = AsyncMock(side_effect=capture_setnx)
        await client_setnx.schedule(
            "add", queue="math", unique_for=60, a=1, b=2,
        )
        await client_setnx.schedule(
            "add", queue="math", unique_for=60, b=2, a=1,
        )
        assert captured_hashes[0] == captured_hashes[1]

    # --- P1 #1 / COR-001: rollback claims on post-SETNX failure ---

    async def test_publish_failure_rolls_back_dedup_flag(
        self, client_setnx, broker, backend_setnx,
    ):
        """broker.publish raises after SETNX success → flag must be released.

        Without rollback, subsequent schedule() with same args returns the
        orphan task_id as `existing` for the full unique_for window.
        """
        backend_setnx.release_unique = AsyncMock(return_value=True)
        broker.publish = AsyncMock(side_effect=RuntimeError("broker down"))

        with pytest.raises(RuntimeError, match="broker down"):
            await client_setnx.schedule(
                "add", 1, 2, queue="math", unique_for=60,
            )
        backend_setnx.claim_unique.assert_awaited_once()
        backend_setnx.release_unique.assert_awaited_once()
        # Same (hash, task_id) pair passed to both.
        setnx_args = backend_setnx.claim_unique.await_args.args
        release_args = backend_setnx.release_unique.await_args.args
        assert setnx_args[0] == release_args[0]  # dedup_hash
        assert setnx_args[1] == release_args[1]  # task_id


class TestClientUniqueDeferred:
    """Producer-side SETNX dedup for delay/eta deferred schedules."""

    @pytest.fixture
    def backend_setnx(self, backend):
        backend.claim_unique = AsyncMock(return_value=None)  # NX granted
        return backend

    @pytest.fixture
    def client_setnx(self, broker, backend_setnx, registry):
        return Client(broker=broker, backend=backend_setnx, registry=registry)

    async def test_deferred_without_unique_for_skips_setnx(
        self, client_setnx, backend_setnx, _patched_schedule_store,
    ):
        """delay= + no unique_for → no SETNX, ScheduleDefinition stays source=cli."""
        await client_setnx.schedule("add", 1, queue="math", delay=60.0)
        backend_setnx.claim_unique.assert_not_awaited()
        definition = _patched_schedule_store.add_schedule.await_args.args[0]
        assert definition.source == "cli"
        assert definition.dedup_hash is None

    async def test_deferred_with_unique_for_sets_flag_and_persists_hash(
        self, client_setnx, backend_setnx, _patched_schedule_store,
    ):
        """delay=60 + unique_for=300 → SETNX with TTL 360_000ms; hash persisted."""
        task_id = await client_setnx.schedule(
            "add", 1, 2, queue="math", delay=60.0, unique_for=300,
        )
        backend_setnx.claim_unique.assert_awaited_once()
        call = backend_setnx.claim_unique.await_args
        dedup_hash = call[0][0]
        stored_task_id = call[0][1]
        ttl_ms = call[0][2]
        assert len(dedup_hash) == 64  # sha256 hex
        assert stored_task_id == task_id
        # TTL = (unique_for + fire_offset) * 1000 — offset ~60s (delay)
        # so total between 355_000 and 365_000 (slack for clock).
        assert 355_000 <= ttl_ms <= 365_000

        definition = _patched_schedule_store.add_schedule.await_args.args[0]
        assert definition.source == "cli_unique"
        assert definition.dedup_hash == dedup_hash

    async def test_deferred_duplicate_returns_existing_and_skips_store(
        self, client_setnx, backend_setnx, _patched_schedule_store,
    ):
        """Dup: return existing task_id; no ScheduleStore.add_schedule call."""
        backend_setnx.claim_unique = AsyncMock(return_value="t-existing")
        tid = await client_setnx.schedule(
            "add", 1, queue="math", delay=10.0, unique_for=60,
        )
        assert tid == "t-existing"
        _patched_schedule_store.add_schedule.assert_not_awaited()

    async def test_deferred_duplicate_emits_signal(
        self, broker, backend_setnx, registry, _patched_schedule_store,
    ):
        """TASK_DUPLICATE_DETECTED dispatched on deferred dedup conflict."""
        from swarmq.signals import Signal, SignalDispatcher

        dispatcher = SignalDispatcher()
        received: list[dict] = []

        @dispatcher.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def handler(**kwargs):
            received.append(kwargs)

        backend_setnx.claim_unique = AsyncMock(return_value="t-win")
        client = Client(
            broker=broker, backend=backend_setnx,
            registry=registry, signal_dispatcher=dispatcher,
        )
        await client.schedule(
            "add", 1, queue="math", delay=5.0, unique_for=30,
        )
        assert len(received) == 1
        assert received[0]["existing_task_id"] == "t-win"
        # Deferred path now emits a distinct source literal (review
        # AC-01 / COR-003 fix) so operators can filter deferred dups.
        assert received[0]["source"] == "client.schedule_deferred"
        assert received[0]["task_name"] == "add"

    async def test_deferred_with_eta_ttl_covers_fire_plus_unique_for(
        self, client_setnx, backend_setnx, _patched_schedule_store,
    ):
        """eta=now+120s + unique_for=300 → TTL ≈ (300+120)*1000 ms."""
        from datetime import UTC, datetime, timedelta
        eta = datetime.now(UTC) + timedelta(seconds=120)
        await client_setnx.schedule(
            "add", 1, queue="math", eta=eta, unique_for=300,
        )
        ttl_ms = backend_setnx.claim_unique.await_args[0][2]
        # Range allows for any clock drift / asyncio scheduling.
        assert 415_000 <= ttl_ms <= 425_000

    async def test_deferred_past_eta_clamps_fire_offset_to_zero(
        self, client_setnx, backend_setnx, _patched_schedule_store,
    ):
        """eta in the past (within 1s lenience) → TTL == unique_for*1000."""
        from datetime import UTC, datetime, timedelta
        # Lenience window allows 1s past — use 0.5s in the past.
        eta = datetime.now(UTC) - timedelta(milliseconds=500)
        await client_setnx.schedule(
            "add", 1, queue="math", eta=eta, unique_for=60,
        )
        ttl_ms = backend_setnx.claim_unique.await_args[0][2]
        # fire_offset clamped to 0, so TTL stays at unique_for * 1000.
        assert ttl_ms == 60_000

    async def test_deferred_client_modus_requires_explicit_unique_for(
        self, broker, backend_setnx, _patched_schedule_store,
    ):
        """Without registry, _MISSING → no dedup; explicit unique_for works."""
        client = Client(broker=broker, backend=backend_setnx, registry=None)
        await client.schedule("add", queue="math", delay=10.0)
        backend_setnx.claim_unique.assert_not_awaited()

        await client.schedule(
            "add", queue="math", delay=10.0, unique_for=60,
        )
        backend_setnx.claim_unique.assert_awaited_once()

    async def test_deferred_setnx_call_before_store_insert(
        self, client_setnx, backend_setnx, _patched_schedule_store,
    ):
        """SETNX must happen BEFORE store.add_schedule — dup path skips store."""
        call_order: list[str] = []

        async def track_setnx(*args, **kwargs):
            call_order.append("setnx")
            return None

        async def track_store(*args, **kwargs):
            call_order.append("store")

        backend_setnx.claim_unique = AsyncMock(side_effect=track_setnx)
        _patched_schedule_store.add_schedule = AsyncMock(side_effect=track_store)

        await client_setnx.schedule(
            "add", 1, queue="math", delay=10.0, unique_for=60,
        )
        assert call_order == ["setnx", "store"]

    async def test_deferred_store_failure_rolls_back_flag(
        self, client_setnx, backend_setnx, _patched_schedule_store,
    ):
        """store.add_schedule raises after SETNX success → flag released.

        Particularly important for deferred with large delay: TTL is
        (unique_for + fire_offset) which can be hours. An orphan flag
        of that duration blocks re-schedule silently.
        """
        backend_setnx.release_unique = AsyncMock(return_value=True)
        _patched_schedule_store.add_schedule = AsyncMock(
            side_effect=RuntimeError("store error")
        )

        with pytest.raises(RuntimeError, match="store error"):
            await client_setnx.schedule(
                "add", 1, queue="math", delay=60.0, unique_for=300,
            )
        backend_setnx.claim_unique.assert_awaited_once()
        backend_setnx.release_unique.assert_awaited_once()


class TestClientUniqueScheduleMany:
    """Batch-API per-element SETNX and duplicate handling."""

    @pytest.fixture
    def backend_setnx(self, backend):
        backend.claim_unique = AsyncMock(return_value=None)
        return backend

    @pytest.fixture
    def client_setnx(self, broker, backend_setnx, registry):
        return Client(broker=broker, backend=backend_setnx, registry=registry)

    async def test_schedule_many_without_unique_for_is_unchanged(
        self, client_setnx, broker, backend_setnx,
    ):
        """Baseline: no unique_for → no SETNX, only R4.4 enqueued-at header."""
        tasks = [("add", (1, 2), {}), ("add", (3, 4), {})]
        ids = await client_setnx.schedule_many(tasks, queue="math")
        assert len(ids) == 2
        backend_setnx.claim_unique.assert_not_awaited()
        for call in broker.publish.await_args_list:
            headers = call.kwargs.get("headers")
            # R4.4: enqueued-at is always stamped; dedup headers are absent.
            assert headers is not None
            assert set(headers.keys()) == {"x-swarmq-enqueued-at"}
            assert isinstance(headers["x-swarmq-enqueued-at"], int)

    async def test_schedule_many_per_element_setnx_and_header(
        self, client_setnx, broker, backend_setnx,
    ):
        """unique_for=60 → SETNX per task, header carried on publish."""
        tasks = [("add", (1,), {}), ("add", (2,), {}), ("add", (3,), {})]
        ids = await client_setnx.schedule_many(
            tasks, queue="math", unique_for=60,
        )
        assert len(ids) == 3
        assert backend_setnx.claim_unique.await_count == 3
        assert broker.publish.await_count == 3
        for call in broker.publish.await_args_list:
            headers = call.kwargs["headers"]
            assert "x-swarmq-unique-hash" in headers
            assert headers["x-swarmq-unique-until"] == "completion"

    async def test_schedule_many_duplicate_returns_existing_at_position(
        self, client_setnx, broker, backend_setnx,
    ):
        """Dup element gets existing_task_id; non-dups get fresh ids.

        Sequence: element 0 duplicate, element 1 fresh, element 2 duplicate.
        """
        returns = iter(["t-existing-0", None, "t-existing-2"])

        async def setnx_mock(*_args, **_kwargs):
            return next(returns)

        backend_setnx.claim_unique = AsyncMock(side_effect=setnx_mock)

        tasks = [("add", (1,), {}), ("add", (2,), {}), ("add", (3,), {})]
        ids = await client_setnx.schedule_many(
            tasks, queue="math", unique_for=60,
        )
        assert len(ids) == 3
        assert ids[0] == "t-existing-0"
        assert ids[2] == "t-existing-2"
        # Middle fresh id is a uuid7.
        import uuid as _uuid
        assert _uuid.UUID(ids[1]).version == 7
        # Only the single non-dup publishes.
        assert broker.publish.await_count == 1

    async def test_schedule_many_duplicate_emits_signal_with_batch_index(
        self, broker, backend_setnx, registry,
    ):
        """Dup path emits TASK_DUPLICATE_DETECTED with batch_index + schedule_many source."""
        from swarmq.signals import Signal, SignalDispatcher

        dispatcher = SignalDispatcher()
        received: list[dict] = []

        @dispatcher.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def handler(**kwargs):
            received.append(kwargs)

        returns = iter([None, "t-dup", None])

        async def setnx_mock(*_a, **_kw):
            return next(returns)

        backend_setnx.claim_unique = AsyncMock(side_effect=setnx_mock)
        client = Client(
            broker=broker, backend=backend_setnx,
            registry=registry, signal_dispatcher=dispatcher,
        )
        tasks = [("add", (1,), {}), ("add", (2,), {}), ("add", (3,), {})]
        await client.schedule_many(tasks, queue="math", unique_for=60)
        assert len(received) == 1
        assert received[0]["existing_task_id"] == "t-dup"
        assert received[0]["source"] == "client.schedule_many"
        assert received[0]["batch_index"] == 1

    async def test_schedule_many_batch_override_none_disables_dedup(
        self, broker, backend_setnx,
    ):
        """Batch-level unique_for=None disables SETNX for all elements."""
        registry = MagicMock()
        registry.has = MagicMock(return_value=True)
        entry = MagicMock()
        entry.task_info = TaskInfo(
            name="add", queues=["math"], unique_for=30,
        )
        registry.get_entry = MagicMock(return_value=entry)
        client = Client(broker=broker, backend=backend_setnx, registry=registry)

        tasks = [("add", (1,), {}), ("add", (2,), {})]
        await client.schedule_many(tasks, queue="math", unique_for=None)
        backend_setnx.claim_unique.assert_not_awaited()

    async def test_schedule_many_uses_taskinfo_default_when_missing(
        self, broker, backend_setnx,
    ):
        """_MISSING resolves per-element TaskInfo.unique_for."""
        registry = MagicMock()
        registry.has = MagicMock(return_value=True)
        entry = MagicMock()
        entry.task_info = TaskInfo(
            name="add", queues=["math"], unique_for=45, unique_until="start",
        )
        registry.get_entry = MagicMock(return_value=entry)
        client = Client(broker=broker, backend=backend_setnx, registry=registry)

        tasks = [("add", (1,), {}), ("add", (2,), {})]
        await client.schedule_many(tasks, queue="math")
        assert backend_setnx.claim_unique.await_count == 2
        # All elements use the TaskInfo default TTL.
        for call in backend_setnx.claim_unique.await_args_list:
            assert call.args[2] == 45_000
        for call in broker.publish.await_args_list:
            assert call.kwargs["headers"]["x-swarmq-unique-until"] == "start"

    async def test_schedule_many_empty_list_still_returns_empty(
        self, client_setnx, backend_setnx,
    ):
        ids = await client_setnx.schedule_many([], queue="math", unique_for=60)
        assert ids == []
        backend_setnx.claim_unique.assert_not_awaited()

    async def test_schedule_many_duplicates_skip_task_enqueued_signal(
        self, broker, backend_setnx, registry,
    ):
        """A duplicated element must NOT emit TASK_ENQUEUED (dup replaces it)."""
        from swarmq.signals import Signal, SignalDispatcher

        dispatcher = SignalDispatcher()
        enqueued: list[str] = []

        @dispatcher.on_signal(Signal.TASK_ENQUEUED)
        async def handler(**kwargs):
            enqueued.append(kwargs["task_id"])

        returns = iter([None, "t-dup"])

        async def setnx_mock(*_a, **_kw):
            return next(returns)

        backend_setnx.claim_unique = AsyncMock(side_effect=setnx_mock)
        client = Client(
            broker=broker, backend=backend_setnx,
            registry=registry, signal_dispatcher=dispatcher,
        )
        tasks = [("add", (1,), {}), ("add", (2,), {})]
        ids = await client.schedule_many(tasks, queue="math", unique_for=60)
        assert ids[1] == "t-dup"
        # Only the fresh publish emits TASK_ENQUEUED.
        assert enqueued == [ids[0]]

    # --- P1 #2 / COR-002 / ADV-004: rollback claims on partial batch failure ---

    async def test_schedule_many_partial_publish_failure_rolls_back_failed_flags(
        self, broker, backend_setnx, registry,
    ):
        """Element 1 publish raises → its flag is released; element 0's
        successful publish keeps its flag (worker will release at terminal)."""
        backend_setnx.release_unique = AsyncMock(return_value=True)

        publish_calls = [0]

        async def publish_mock(*_a, **_kw):
            publish_calls[0] += 1
            if publish_calls[0] == 2:
                raise RuntimeError("broker blip on element 1")

        broker.publish = AsyncMock(side_effect=publish_mock)

        client = Client(broker=broker, backend=backend_setnx, registry=registry)
        tasks = [("add", (1,), {}), ("add", (2,), {})]

        with pytest.raises(RuntimeError, match="blip on element 1"):
            await client.schedule_many(tasks, queue="math", unique_for=60)

        # Exactly one rollback — for the failed publish's flag.
        backend_setnx.release_unique.assert_awaited_once()

    async def test_schedule_many_validation_fails_before_any_setnx(
        self, broker, backend_setnx,
    ):
        """TaskNotFoundError on any element aborts Pass 0 validation
        BEFORE any SETNX fires, so no flags are claimed or orphaned.

        This is a stricter guarantee than the sequential implementation
        offered — parallel SETNX (P3 #22) makes fail-fast validation
        free and removes an entire class of partial-claim scenarios.
        """
        backend_setnx.release_unique = AsyncMock(return_value=True)

        registry = MagicMock()
        registry.has = MagicMock(side_effect=[True, False])
        entry = MagicMock()
        entry.task_info = TaskInfo(name="add", queues=["math"])
        registry.get_entry = MagicMock(return_value=entry)

        client = Client(broker=broker, backend=backend_setnx, registry=registry)
        tasks = [("add", (1,), {}), ("unknown", (), {})]

        with pytest.raises(TaskNotFoundError):
            await client.schedule_many(tasks, queue="math", unique_for=60)

        # No SETNX was called (validation fails first), so no rollback
        # is needed either.
        backend_setnx.claim_unique.assert_not_awaited()
        backend_setnx.release_unique.assert_not_awaited()

    async def test_schedule_many_parallel_setnx_pre_enqueue_fail_rolls_back(
        self, broker, backend_setnx, registry,
    ):
        """If pre_enqueue raises during Pass 2 after all SETNX succeeded
        in Pass 1, every claimed flag must be rolled back."""
        from swarmq.middleware import Middleware, MiddlewareChain

        backend_setnx.release_unique = AsyncMock(return_value=True)

        class BoomMiddleware(Middleware):
            call_count = 0

            async def pre_enqueue(self, ctx):
                type(self).call_count += 1
                if type(self).call_count == 3:
                    raise RuntimeError("pre_enqueue boom")

        client = Client(
            broker=broker, backend=backend_setnx, registry=registry,
            middleware_chain=MiddlewareChain([BoomMiddleware()]),
        )
        tasks = [("add", (i,), {}) for i in range(3)]

        with pytest.raises(RuntimeError, match="pre_enqueue boom"):
            await client.schedule_many(tasks, queue="math", unique_for=60)

        # All three SETNX fired in parallel; all three must be rolled back.
        assert backend_setnx.claim_unique.await_count == 3
        assert backend_setnx.release_unique.await_count == 3


class TestClientPriorityRouting:
    async def _make_client_with_task(self, priority: str):
        Task._clear_registered_tasks()

        class P(Task):
            task_info = TaskInfo(
                name="p_task", queues=["work"], priority=priority,  # type: ignore[arg-type]
            )

            async def run(self) -> str:
                return "ok"

        broker = AsyncMock()
        broker.publish = AsyncMock()
        backend = AsyncMock()
        registry = TaskRegistry()
        registry.register(P)
        client = Client(
            broker=broker, backend=backend, registry=registry,
            signal_dispatcher=SignalDispatcher(),
        )
        return client, broker

    async def test_normal_priority_publishes_to_base_queue(self):
        client, broker = await self._make_client_with_task("normal")
        try:
            await client.schedule("p_task")
            call = broker.publish.await_args
            assert call.args[0] == "work"
        finally:
            Task._clear_registered_tasks()

    async def test_high_priority_publishes_to_high_suffix(self):
        client, broker = await self._make_client_with_task("high")
        try:
            await client.schedule("p_task")
            call = broker.publish.await_args
            assert call.args[0] == "work.high"
        finally:
            Task._clear_registered_tasks()

    async def test_low_priority_publishes_to_low_suffix(self):
        client, broker = await self._make_client_with_task("low")
        try:
            await client.schedule("p_task")
            call = broker.publish.await_args
            assert call.args[0] == "work.low"
        finally:
            Task._clear_registered_tasks()


# ---------------------------------------------------------------------------
# Logging (Plan 2026-10-06-0831, U9: R2, R17, R18)
# ---------------------------------------------------------------------------
# Test list:
# 1. Ein Duplikat loggt ``dedup_detected`` (vorher ``swarmq.dedup.detected``)
#    auf INFO ueber den Logger ``swarmq.client.schedule`` mit der Payload des
#    Signals.
# 2. Scheitert der Rollback eines Dedup-Claims, loggt das
#    ``dedup_rollback_failed`` (vorher ``swarmq.dedup.rollback_failed``) auf
#    WARNING mit ``error_type`` und ``error`` -- der TTL heilt den Claim.


def _schedule_events(entries: list[dict], event: str) -> list[dict]:
    return [e for e in entries if e["event"] == event and e["logger"] == "swarmq.client.schedule"]


class TestScheduleLogging:
    @pytest.mark.timeout(5)
    async def test_duplicate_logs_dedup_detected(self, broker, backend, registry, swarmq_logs):
        backend.claim_unique = AsyncMock(return_value="t-existing")
        client = Client(broker=broker, backend=backend, registry=registry)

        await client.schedule("add", 1, 2, queue="math", unique_for=60)

        [entry] = _schedule_events(swarmq_logs, "dedup_detected")
        assert entry["log_level"] == "info"
        assert entry["existing_task_id"] == "t-existing"
        assert entry["task_name"] == "add"
        assert entry["source"] == "client.schedule"
        assert len(entry["dedup_hash"]) == 64

    @pytest.mark.timeout(5)
    async def test_failed_rollback_logs_dedup_rollback_failed_with_error_fields(
        self, broker, backend, registry, swarmq_logs,
    ):
        backend.claim_unique = AsyncMock(return_value=None)
        backend.release_unique = AsyncMock(side_effect=ConnectionError("valkey down"))
        broker.publish = AsyncMock(side_effect=RuntimeError("broker down"))
        client = Client(broker=broker, backend=backend, registry=registry)

        with pytest.raises(RuntimeError, match="broker down"):
            await client.schedule("add", 1, 2, queue="math", unique_for=60)

        [entry] = _schedule_events(swarmq_logs, "dedup_rollback_failed")
        assert entry["log_level"] == "warning"
        assert entry["task_id"] == backend.claim_unique.await_args.args[1]
        assert entry["dedup_hash"] == backend.claim_unique.await_args.args[0]
        assert entry["error_type"] == "ConnectionError"
        assert entry["error"] == "valkey down"
