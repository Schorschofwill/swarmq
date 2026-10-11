"""Tests for SwarmQ App class."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import ConfigurationError
from swarmq.task import Task, TaskInfo


@pytest.fixture()
def broker():
    b = MagicMock(spec=RabbitMQBroker)
    b._url = "amqp://guest:guest@localhost/"
    b.connect = AsyncMock()
    b.close = AsyncMock()
    return b


@pytest.fixture()
def backend():
    b = MagicMock(spec=ValkeyBackend)
    b._url = "valkey://localhost:6379/0"
    b.connect = AsyncMock()
    b.close = AsyncMock()
    # Cancel-flag primitives (Phase 3b.3). Default: no flag set.
    b.is_cancelled = AsyncMock(return_value=False)
    b.set_cancel_flag = AsyncMock(return_value=True)
    b.clear_cancel_flag = AsyncMock()
    return b


class TestSwarmQInstantiation:
    def test_creates_instance_with_broker_and_backend(self, broker, backend):
        app = SwarmQ(broker, backend)

        assert app.broker is broker
        assert app.backend is backend
        assert app.config is not None
        assert app.registry is not None
        assert app.client is not None

    def test_config_receives_urls_from_broker_and_backend(self, broker, backend):
        app = SwarmQ(broker, backend)

        assert app.config.broker_url == "amqp://guest:guest@localhost/"
        assert app.config.backend_url == "valkey://localhost:6379/0"

    def test_config_kwargs_forwarded(self, broker, backend):
        app = SwarmQ(broker, backend, concurrency=5, log_level="DEBUG")

        assert app.config.concurrency == 5
        assert app.config.log_level == "DEBUG"


class TestSwarmQInjectedBackends:
    """URL becomes optional per backend once a connection object is
    injected. Uses real broker/backend instances (not mocks) to exercise
    the actual SwarmQ.__init__ -> SwarmQConfig path."""

    def test_injected_backend_client_makes_backend_url_optional(self):
        """AE4: broker by URL + backend by injected client -> config valid,
        backend_url carries the credential-free placeholder (not None)."""
        real_broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        real_backend = ValkeyBackend(client=AsyncMock())

        app = SwarmQ(real_broker, real_backend)

        assert app.config.broker_url == "amqp://guest:guest@localhost/"
        assert app.config.backend_url == "<injected>"

    def test_both_injected_constructs_without_any_url(self):
        real_broker = RabbitMQBroker(connection=AsyncMock())
        real_backend = ValkeyBackend(client=AsyncMock())

        app = SwarmQ(real_broker, real_backend)

        assert app.config.broker_url == "<injected>"
        assert app.config.backend_url == "<injected>"

    def test_injected_broker_makes_broker_url_optional(self):
        real_broker = RabbitMQBroker(connection=AsyncMock())
        real_backend = ValkeyBackend("valkey://localhost:6379/0")

        app = SwarmQ(real_broker, real_backend)

        assert app.config.broker_url == "<injected>"
        assert app.config.backend_url == "valkey://localhost:6379/0"

    def test_config_placeholder_carries_no_credentials(self):
        """The placeholder must never be derived from the borrowed object,
        so no AMQP/Valkey credentials can leak into a config dump."""
        real_broker = RabbitMQBroker(connection=AsyncMock())
        real_backend = ValkeyBackend(client=AsyncMock())

        app = SwarmQ(real_broker, real_backend)

        assert "@" not in app.config.broker_url
        assert "@" not in app.config.backend_url


class TestSwarmQStartWorkerInjectionGuard:
    """start_worker() rejects an injected broker/backend before spawning,
    because injected connection objects cannot cross the spawn boundary
    (AE3)."""

    @pytest.mark.anyio()
    async def test_start_worker_rejects_injected_broker(self):
        broker = RabbitMQBroker(connection=AsyncMock())
        backend = ValkeyBackend("valkey://localhost:6379/0")
        app = SwarmQ(broker, backend)

        with pytest.raises(ConfigurationError, match="injected broker"):
            await app.start_worker()

    @pytest.mark.anyio()
    async def test_start_worker_rejects_injected_backend(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        backend = ValkeyBackend(client=AsyncMock())
        app = SwarmQ(broker, backend)

        with pytest.raises(ConfigurationError, match="injected backend"):
            await app.start_worker()

    @pytest.mark.anyio()
    async def test_start_worker_error_names_spawn_boundary_reason(self):
        broker = RabbitMQBroker(connection=AsyncMock())
        backend = ValkeyBackend(client=AsyncMock())
        app = SwarmQ(broker, backend)

        with pytest.raises(ConfigurationError, match="SWARMQ_BROKER_URL"):
            await app.start_worker()

    @pytest.mark.anyio()
    async def test_start_worker_both_injected_names_both_in_message(self):
        broker = RabbitMQBroker(connection=AsyncMock())
        backend = ValkeyBackend(client=AsyncMock())
        app = SwarmQ(broker, backend)

        with pytest.raises(ConfigurationError, match="broker and backend"):
            await app.start_worker()

    @pytest.mark.anyio()
    async def test_start_worker_proceeds_for_url_constructed_backends(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        backend = ValkeyBackend("valkey://localhost:6379/0")
        app = SwarmQ(broker, backend)

        with patch("swarmq.app.Worker") as mock_worker_cls:
            mock_worker_cls.return_value.start = AsyncMock()
            await app.start_worker()

        # Guard did not fire — the worker was constructed and started.
        mock_worker_cls.return_value.start.assert_awaited_once()


class TestSwarmQContextManager:
    @pytest.mark.anyio()
    async def test_aenter_connects_broker_and_backend(self, broker, backend):
        app = SwarmQ(broker, backend)

        result = await app.__aenter__()

        assert result is app
        broker.connect.assert_awaited_once()
        backend.connect.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_aexit_closes_broker_and_backend(self, broker, backend):
        app = SwarmQ(broker, backend)
        await app.__aenter__()

        await app.__aexit__(None, None, None)

        broker.close.assert_awaited_once()
        backend.close.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_aexit_closes_backend_even_if_broker_close_raises(
        self,
        broker,
        backend,
    ):
        """Härtung R2 P1-4: ``__aexit__`` lief sequenziell ohne
        try/finally — ein werfendes ``broker.close()`` (im Inject-Pfad
        bewusst möglich, siehe test_pool_close_releases_even_if_channel_
        close_raises) ließ den Valkey-Client und den Log-Listener-Thread
        offen. Backend-Close und die Freigabe der Opt-in-Log-Queue (U8)
        müssen garantiert laufen; die erste Exception propagiert."""
        import logging

        app = SwarmQ(broker, backend, log_queue=True)
        await app.__aenter__()
        broker.close.side_effect = RuntimeError("channel close boom")

        with pytest.raises(RuntimeError, match="channel close boom"):
            await app.__aexit__(None, None, None)

        backend.close.assert_awaited_once()
        assert logging.getLogger("swarmq").handlers == []

    @pytest.mark.anyio()
    async def test_aexit_releases_the_log_queue_even_if_backend_close_raises(
        self,
        broker,
        backend,
    ):
        import logging

        app = SwarmQ(broker, backend, log_queue=True)
        await app.__aenter__()
        backend.close.side_effect = RuntimeError("backend boom")

        with pytest.raises(RuntimeError, match="backend boom"):
            await app.__aexit__(None, None, None)

        broker.close.assert_awaited_once()
        assert logging.getLogger("swarmq").handlers == []

    @pytest.mark.anyio()
    async def test_async_with_syntax(self, broker, backend):
        async with SwarmQ(broker, backend) as app:
            assert app is not None
        broker.connect.assert_awaited_once()
        backend.connect.assert_awaited_once()
        broker.close.assert_awaited_once()
        backend.close.assert_awaited_once()


class TestSwarmQPartialConnectFailure:
    @pytest.mark.anyio()
    async def test_backend_connect_failure_closes_broker(self, broker, backend):
        backend.connect.side_effect = ConnectionError("backend down")
        app = SwarmQ(broker, backend)

        with pytest.raises(ConnectionError, match="backend down"):
            await app.__aenter__()

        broker.connect.assert_awaited_once()
        broker.close.assert_awaited_once()


class TestSwarmQDelegation:
    @pytest.mark.anyio()
    async def test_schedule_delegates_to_client(self, broker, backend):
        app = SwarmQ(broker, backend)
        app.client.schedule = AsyncMock(return_value="task-id-123")

        result = await app.schedule("my_task", 1, 2, key="val")

        app.client.schedule.assert_awaited_once_with("my_task", 1, 2, key="val")
        assert result == "task-id-123"

    @pytest.mark.anyio()
    async def test_get_result_delegates_to_client(self, broker, backend):
        app = SwarmQ(broker, backend)
        app.client.get_result = AsyncMock(return_value=42)

        result = await app.get_result("task-id-123", timeout=5, polling=0.1)

        app.client.get_result.assert_awaited_once_with("task-id-123", timeout=5, polling=0.1)
        assert result == 42

    @pytest.mark.anyio()
    async def test_run_delegates_to_client(self, broker, backend):
        app = SwarmQ(broker, backend)
        app.client.run = AsyncMock(return_value="done")

        result = await app.run("my_task", "a", timeout=3, polling=0.5, x=10)

        app.client.run.assert_awaited_once_with("my_task", "a", timeout=3, polling=0.5, x=10)
        assert result == "done"


class TestSwarmQAutoDiscovery:
    def test_registered_tasks_are_discovered(self, broker, backend):
        saved = Task._registered_tasks.copy()
        try:
            Task._clear_registered_tasks()

            class AddTask(Task):
                task_info = TaskInfo(name="add", queues=["default"])

                async def run(self, a: int, b: int) -> int:
                    return a + b

            app = SwarmQ(broker, backend)

            assert app.registry.has("add")
            assert app.registry.get("add") is AddTask
        finally:
            Task._registered_tasks[:] = saved


class TestSwarmQConfigValidation:
    def test_invalid_config_raises_configuration_error(self):
        broker = MagicMock(spec=RabbitMQBroker)
        broker._url = "amqp://guest:guest@localhost/"
        backend = MagicMock(spec=ValkeyBackend)
        backend._url = "valkey://localhost:6379/0"

        with pytest.raises(ConfigurationError):
            SwarmQ(broker, backend, concurrency=-1)


class TestSwarmQDoubleExit:
    @pytest.mark.anyio()
    async def test_double_aexit_is_safe(self, broker, backend):
        app = SwarmQ(broker, backend)
        await app.__aenter__()

        await app.__aexit__(None, None, None)
        await app.__aexit__(None, None, None)

        # Both close methods called twice without error
        assert broker.close.await_count == 2
        assert backend.close.await_count == 2


class TestSwarmQLogging:
    """Plan 2026-10-06-0831, U8 (R1, KTD4): Die Bibliothek konfiguriert nie.

    Test-Liste:
    1. ``SwarmQ(...)`` laesst Root-Handler, Root-Level, ``structlog.get_config()``
       und die Logger-Klasse unveraendert (AE1).
    2. ``SwarmQ(...)`` ohne Opt-in startet keinen Thread.
    3. ``SwarmQ(log_level=..., log_json=...)`` ohne Opt-in wirft nicht und
       konfiguriert nichts.
    4. ``__aexit__`` gibt den Opt-in frei, auch wenn ``broker.close()`` bzw.
       ``backend.close()`` wirft (in ``TestSwarmQContextManager``).
    """

    def test_init_leaves_root_structlog_and_logger_class_untouched(self, broker, backend):
        import logging

        import structlog

        root = logging.getLogger()
        app_handler = logging.NullHandler()
        root.addHandler(app_handler)
        root.setLevel(logging.WARNING)
        handlers = list(root.handlers)
        config = structlog.get_config()
        logger_class = logging.getLoggerClass()

        SwarmQ(broker, backend)

        assert root.handlers == handlers
        assert root.level == logging.WARNING
        assert structlog.get_config() == config
        assert logging.getLoggerClass() is logger_class

    def test_init_without_opt_in_starts_no_thread(self, broker, backend):
        import threading

        before = threading.active_count()

        SwarmQ(broker, backend, log_queue=False)

        assert threading.active_count() == before

    def test_log_level_and_log_json_without_opt_in_configure_nothing(self, broker, backend):
        import logging

        root = logging.getLogger()
        handlers = list(root.handlers)
        swarmq_logger = logging.getLogger("swarmq")
        swarmq_state = (list(swarmq_logger.handlers), swarmq_logger.level, swarmq_logger.propagate)

        SwarmQ(broker, backend, log_level="DEBUG", log_json=False)

        assert root.handlers == handlers
        assert (list(swarmq_logger.handlers), swarmq_logger.level, swarmq_logger.propagate) == swarmq_state


class TestSwarmQStartWorker:
    @pytest.mark.anyio()
    async def test_start_worker_creates_and_starts_worker(self, broker, backend):
        app = SwarmQ(broker, backend)

        with patch("swarmq.app.Worker") as MockWorker:
            mock_instance = MockWorker.return_value
            mock_instance.start = AsyncMock()

            await app.start_worker()

            from swarmq.middleware import MiddlewareChain

            MockWorker.assert_called_once()
            call_args, call_kwargs = MockWorker.call_args
            assert call_args == (broker, backend, app.registry, app.config)
            # middleware_chain is a factory callable so add_middleware() calls
            # made after SwarmQ construction are visible to the Worker.
            factory = call_kwargs["middleware_chain"]
            assert callable(factory)
            assert isinstance(factory(), MiddlewareChain)
            assert call_kwargs["signal_dispatcher"] is app._signal_dispatcher
            mock_instance.start.assert_awaited_once()


class TestSwarmQMiddlewareAndSignals:
    def test_init_creates_empty_middleware_list(self, broker, backend):
        app = SwarmQ(broker, backend)

        assert app._middlewares == []

    def test_init_creates_signal_dispatcher(self, broker, backend):
        from swarmq.signals import SignalDispatcher

        app = SwarmQ(broker, backend)

        assert isinstance(app._signal_dispatcher, SignalDispatcher)

    def test_add_middleware_appends_to_middleware_list(self, broker, backend):
        from swarmq.middleware import Middleware

        app = SwarmQ(broker, backend)
        mw = Middleware()

        app.add_middleware(mw)

        assert app._middlewares == [mw]

    def test_add_middleware_returns_none(self, broker, backend):
        from swarmq.middleware import Middleware

        app = SwarmQ(broker, backend)
        mw = Middleware()

        result = app.add_middleware(mw)

        assert result is None

    def test_multiple_add_middleware_calls_accumulate_in_order(self, broker, backend):
        from swarmq.middleware import Middleware

        app = SwarmQ(broker, backend)
        mw1 = Middleware()
        mw2 = Middleware()
        mw3 = Middleware()

        app.add_middleware(mw1)
        app.add_middleware(mw2)
        app.add_middleware(mw3)

        assert app._middlewares == [mw1, mw2, mw3]

    def test_middleware_chain_property_returns_chain_with_registered_middlewares(self, broker, backend):
        from swarmq.middleware import Middleware, MiddlewareChain

        app = SwarmQ(broker, backend)
        mw1 = Middleware()
        mw2 = Middleware()
        app.add_middleware(mw1)
        app.add_middleware(mw2)

        chain = app.middleware_chain

        assert isinstance(chain, MiddlewareChain)
        assert chain._middlewares == [mw1, mw2]

    def test_middleware_chain_property_empty_when_no_middlewares(self, broker, backend):
        from swarmq.middleware import MiddlewareChain

        app = SwarmQ(broker, backend)

        chain = app.middleware_chain

        assert isinstance(chain, MiddlewareChain)
        assert chain._middlewares == []

    def test_middleware_chain_property_rebuilds_on_each_access(self, broker, backend):
        from swarmq.middleware import Middleware

        app = SwarmQ(broker, backend)
        mw1 = Middleware()
        app.add_middleware(mw1)

        chain1 = app.middleware_chain
        mw2 = Middleware()
        app.add_middleware(mw2)
        chain2 = app.middleware_chain

        # First chain has snapshot of state at access time
        assert chain1._middlewares == [mw1]
        # Second chain has the later state
        assert chain2._middlewares == [mw1, mw2]

    def test_on_signal_is_decorator_that_registers_handler(self, broker, backend):
        from swarmq.signals import Signal

        app = SwarmQ(broker, backend)

        @app.on_signal(Signal.TASK_COMPLETED)
        async def handler(**kwargs):
            pass

        registered = app._signal_dispatcher._handlers.get(Signal.TASK_COMPLETED, [])
        assert handler in registered

    def test_on_signal_returns_original_function_unchanged(self, broker, backend):
        from swarmq.signals import Signal

        app = SwarmQ(broker, backend)

        async def handler(**kwargs):
            pass

        decorated = app.on_signal(Signal.TASK_COMPLETED)(handler)

        assert decorated is handler

    def test_on_signal_delegates_to_signal_dispatcher(self, broker, backend):
        from swarmq.signals import Signal

        app = SwarmQ(broker, backend)

        async def handler(**kwargs):
            pass

        app.on_signal(Signal.TASK_STARTED)(handler)

        # Handler should be registered on the same dispatcher instance
        assert handler in app._signal_dispatcher._handlers[Signal.TASK_STARTED]

    def test_multiple_handlers_on_same_signal_all_registered(self, broker, backend):
        from swarmq.signals import Signal

        app = SwarmQ(broker, backend)

        @app.on_signal(Signal.TASK_FAILED)
        async def handler1(**kwargs):
            pass

        @app.on_signal(Signal.TASK_FAILED)
        async def handler2(**kwargs):
            pass

        handlers = app._signal_dispatcher._handlers[Signal.TASK_FAILED]
        assert handler1 in handlers
        assert handler2 in handlers

    @pytest.mark.anyio()
    async def test_add_middleware_after_swarmq_init_fires_in_client(
        self,
        broker,
        backend,
    ):
        """Regression: middleware added AFTER SwarmQ.__init__ must still fire.

        Before the fix, SwarmQ captured ``self.middleware_chain`` as a stale
        MiddlewareChain snapshot when it constructed the Client. Calls to
        ``add_middleware()`` appended to ``_middlewares`` but the Client kept
        using the old snapshot, so middleware never ran for client-side
        hooks.

        This test creates a SwarmQ, registers a spy middleware AFTERWARDS,
        schedules a task, and verifies the spy's pre_enqueue/post_enqueue
        were called.
        """
        from swarmq.middleware import Middleware
        from swarmq.task import Task, TaskInfo

        # Register a task so schedule() passes registry validation.
        saved = Task._registered_tasks.copy()
        try:
            Task._clear_registered_tasks()

            class AddTask(Task):
                task_info = TaskInfo(name="add", queues=["math"])

                async def run(self, a: int, b: int) -> int:
                    return a + b

            app = SwarmQ(broker, backend)

            # Mock broker.publish so we don't need a real AMQP connection.
            broker.publish = AsyncMock()

            class Spy(Middleware):
                def __init__(self) -> None:
                    self.pre_calls: list[dict] = []
                    self.post_calls: list[dict] = []

                async def pre_enqueue(self, message: dict) -> None:
                    self.pre_calls.append(message)

                async def post_enqueue(self, message: dict) -> None:
                    self.post_calls.append(message)

            spy = Spy()

            # Register middleware AFTER SwarmQ construction — this is the
            # case the bug broke.
            app.add_middleware(spy)

            task_id = await app.schedule("add", 1, 2)

            broker.publish.assert_awaited_once()
            assert len(spy.pre_calls) == 1
            assert len(spy.post_calls) == 1
            assert spy.pre_calls[0]["task_id"] == task_id
            assert spy.pre_calls[0]["task_name"] == "add"
            assert spy.pre_calls[0]["queue"] == "math"
            assert spy.pre_calls[0]["args"] == [1, 2]
            assert spy.post_calls[0]["task_id"] == task_id
        finally:
            Task._registered_tasks[:] = saved

    @pytest.mark.anyio()
    async def test_add_middleware_after_swarmq_init_visible_to_worker(
        self,
        broker,
        backend,
    ):
        """Regression: middleware added AFTER SwarmQ.__init__ must fire for
        Worker hooks, just like it does for Client hooks.

        Before the fix, SwarmQ.start_worker() passed ``self.middleware_chain``
        as a snapshot to Worker. ``add_middleware()`` appended to
        ``_middlewares`` but the Worker kept using the stale chain.

        This test builds a SwarmQ, registers a Spy middleware AFTERWARDS,
        constructs a real Worker via the same wiring start_worker() uses,
        dispatches one message via ``_handle_message``, and asserts the Spy
        observed pre_execute / post_execute.
        """
        from swarmq.middleware import Middleware
        from swarmq.registry import TaskRegistry
        from swarmq.task import Task, TaskInfo
        from swarmq.worker import Worker

        saved = Task._registered_tasks.copy()
        try:
            Task._clear_registered_tasks()

            class NoopTask(Task):
                task_info = TaskInfo(name="noop", queues=["q"])

                async def run(self) -> str:
                    return "ok"

            app = SwarmQ(broker, backend, queues=["q"])

            class Spy(Middleware):
                def __init__(self) -> None:
                    self.pre_execute_calls: list[dict] = []
                    self.post_execute_calls: list[tuple[dict, object]] = []

                async def pre_execute(self, message: dict) -> None:
                    self.pre_execute_calls.append(dict(message))

                async def post_execute(self, message: dict, result: object = None) -> None:
                    self.post_execute_calls.append((dict(message), result))

            spy = Spy()
            app.add_middleware(spy)  # AFTER SwarmQ() — was the bug path

            # Mirror SwarmQ.start_worker()'s wiring: factory, not snapshot.
            registry = TaskRegistry()
            registry.register(NoopTask)
            worker = Worker(
                broker=broker,
                backend=backend,
                registry=registry,
                config=app.config,
                middleware_chain=lambda: app.middleware_chain,
            )

            from tests.support.messages import make_message
            from tests.unit.worker.test_worker import _default_payload

            msg = make_message(_default_payload(name="noop", queue="q", args=[]))
            await worker._handle_message(msg)

            assert len(spy.pre_execute_calls) == 1
            assert len(spy.post_execute_calls) == 1
            assert spy.pre_execute_calls[0]["task_name"] == "noop"
            assert spy.post_execute_calls[0][1] == "ok"
        finally:
            Task._registered_tasks[:] = saved


class TestSwarmQPublicAPIExports:
    def test_middleware_importable_from_swarmq(self):
        from swarmq import Middleware
        from swarmq.middleware import Middleware as _MiddlewareDirect

        assert Middleware is _MiddlewareDirect

    def test_signal_importable_from_swarmq(self):
        from swarmq import Signal
        from swarmq.signals import Signal as _SignalDirect

        assert Signal is _SignalDirect

    def test_signal_dispatcher_importable_from_swarmq(self):
        from swarmq import SignalDispatcher
        from swarmq.signals import SignalDispatcher as _DispatcherDirect

        assert SignalDispatcher is _DispatcherDirect


# ---------------------------------------------------------------------------
# SwarmQ.cancel — Phase 3b.3 / Unit 5
# ---------------------------------------------------------------------------


class TestSwarmQCancel:
    async def test_cancel_delegates_to_client_with_computed_ttl(self, broker, backend):
        app = SwarmQ(broker, backend, result_ttl=60)
        app.client.cancel = AsyncMock(return_value=True)

        result = await app.cancel("task-abc")

        # BZ-23 (Plan 0.12.0, MR 5): ttl_ms = max(24 h, result_ttl) — ein
        # kurzes result_ttl verkuerzt das Flag nicht mehr.
        app.client.cancel.assert_awaited_once_with("task-abc", ttl_ms=86_400_000)
        assert result is True

    async def test_cancel_ttl_follows_result_ttl_above_24h(self, broker, backend):
        app = SwarmQ(broker, backend, result_ttl=172_800)
        app.client.cancel = AsyncMock(return_value=True)

        await app.cancel("task-long")

        app.client.cancel.assert_awaited_once_with("task-long", ttl_ms=172_800_000)

    async def test_cancel_uses_default_result_ttl_when_not_overridden(
        self,
        broker,
        backend,
    ):
        app = SwarmQ(broker, backend)
        app.client.cancel = AsyncMock(return_value=True)

        await app.cancel("task-def")

        # Default result_ttl is 86400 → 86_400_000 ms
        app.client.cancel.assert_awaited_once_with("task-def", ttl_ms=86_400_000)

    async def test_cancel_returns_false_when_already_cancelled(
        self,
        broker,
        backend,
    ):
        app = SwarmQ(broker, backend)
        app.client.cancel = AsyncMock(return_value=False)

        result = await app.cancel("task-ghi")
        assert result is False


class TestPublishTimeoutWiring:
    """Plan 2026-08-28-001, U1 / R3: die App reicht die konfigurierte
    Publish-Deadline an den Broker durch — gleicher Weg wie queue_type."""

    def test_default_timeout_reaches_broker(self, broker, backend):
        app = SwarmQ(broker, backend)
        assert broker.publish_timeout_s == 120.0
        assert app.config.publish_timeout_s == 120.0

    def test_env_timeout_reaches_broker(self, broker, backend, monkeypatch):
        monkeypatch.setenv("SWARMQ_PUBLISH_TIMEOUT_S", "12.5")
        SwarmQ(broker, backend)
        assert broker.publish_timeout_s == 12.5


class TestDeliveryLimitWiring:
    """B5: die App reicht das konfigurierte Zustellbudget an den Broker
    durch — gleicher Weg wie queue_type und publish_timeout_s."""

    def test_default_limit_reaches_broker(self, broker, backend):
        app = SwarmQ(broker, backend)
        assert broker.delivery_limit == 20
        assert app.config.delivery_limit == 20

    def test_env_limit_reaches_broker(self, broker, backend, monkeypatch):
        monkeypatch.setenv("SWARMQ_DELIVERY_LIMIT", "7")
        SwarmQ(broker, backend)
        assert broker.delivery_limit == 7


class TestDeadLetterTtlWiring:
    """Plan 2026-10-08-1541, U9 (KTD6): die App reicht ``dead_letter_ttl_s``
    an den Broker durch -- gleicher Weg wie ``delivery_limit``."""

    @pytest.mark.parametrize("ttl", [3600.0, None])
    def test_configured_ttl_reaches_broker(self, broker, backend, ttl):
        SwarmQ(broker, backend, dead_letter_ttl_s=ttl)
        assert broker.dead_letter_ttl_s == ttl
