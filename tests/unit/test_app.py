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

        app.client.get_result.assert_awaited_once_with(
            "task-id-123", timeout=5, polling=0.1
        )
        assert result == 42

    @pytest.mark.anyio()
    async def test_run_delegates_to_client(self, broker, backend):
        app = SwarmQ(broker, backend)
        app.client.run = AsyncMock(return_value="done")

        result = await app.run("my_task", "a", timeout=3, polling=0.5, x=10)

        app.client.run.assert_awaited_once_with(
            "my_task", "a", timeout=3, polling=0.5, x=10
        )
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
    def test_init_calls_setup_logging(self, broker, backend):
        with patch("swarmq.app.setup_logging") as mock_setup:
            mock_listener = MagicMock()
            mock_setup.return_value = mock_listener

            app = SwarmQ(broker, backend)

            mock_setup.assert_called_once_with(
                app.config.log_level, app.config.log_json
            )
            assert app._log_listener is mock_listener

    def test_init_stores_listener_for_shutdown(self, broker, backend):
        with patch("swarmq.app.setup_logging") as mock_setup:
            mock_listener = MagicMock()
            mock_setup.return_value = mock_listener

            app = SwarmQ(broker, backend)

            assert app._log_listener is mock_listener

    @pytest.mark.anyio()
    async def test_aexit_stops_log_listener(self, broker, backend):
        with patch("swarmq.app.setup_logging") as mock_setup:
            mock_listener = MagicMock()
            mock_setup.return_value = mock_listener

            app = SwarmQ(broker, backend)
            await app.__aenter__()
            await app.__aexit__(None, None, None)

            mock_listener.stop.assert_called_once()


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
            assert isinstance(call_kwargs["middleware_chain"], MiddlewareChain)
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

    def test_multiple_add_middleware_calls_accumulate_in_order(
        self, broker, backend
    ):
        from swarmq.middleware import Middleware

        app = SwarmQ(broker, backend)
        mw1 = Middleware()
        mw2 = Middleware()
        mw3 = Middleware()

        app.add_middleware(mw1)
        app.add_middleware(mw2)
        app.add_middleware(mw3)

        assert app._middlewares == [mw1, mw2, mw3]

    def test_middleware_chain_property_returns_chain_with_registered_middlewares(
        self, broker, backend
    ):
        from swarmq.middleware import Middleware, MiddlewareChain

        app = SwarmQ(broker, backend)
        mw1 = Middleware()
        mw2 = Middleware()
        app.add_middleware(mw1)
        app.add_middleware(mw2)

        chain = app.middleware_chain

        assert isinstance(chain, MiddlewareChain)
        assert chain._middlewares == [mw1, mw2]

    def test_middleware_chain_property_empty_when_no_middlewares(
        self, broker, backend
    ):
        from swarmq.middleware import MiddlewareChain

        app = SwarmQ(broker, backend)

        chain = app.middleware_chain

        assert isinstance(chain, MiddlewareChain)
        assert chain._middlewares == []

    def test_middleware_chain_property_rebuilds_on_each_access(
        self, broker, backend
    ):
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

    def test_on_signal_returns_original_function_unchanged(
        self, broker, backend
    ):
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

    def test_multiple_handlers_on_same_signal_all_registered(
        self, broker, backend
    ):
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
