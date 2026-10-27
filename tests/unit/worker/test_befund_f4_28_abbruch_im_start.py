"""F4-28: Ein Abbruch von ``start()`` geht nicht verloren, wenn aiormq ihn in
einen Kanalfehler verwandelt.

aiormq fuehrt jeden RPC als eigenen Task aus; der Aufrufer wartet ueber einen
``TaskWrapper``. Schliesst aiormq den Kanal, waehrend der Abbruch des
Aufrufers den inneren Task erreicht, kommt beim Aufrufer statt
``CancelledError`` der Kanalfehler an (``TaskWrapper.throw``). swarmq faengt
manche davon als normalen Ablauf (``ChannelNotFoundEntity`` beim passiven
Declare einer neuen Queue), aiormqs ``close()`` schluckt ``ChannelClosed``.
Der Task behaelt ``cancelling() == 1``, der Worker startete aber zu Ende und
wartete fuer immer auf das Shutdown.

Die Nachbildung hier braucht nur asyncio: Ein innerer Task wirft bei seinem
Abbruch einen Kanalfehler, wie ``TaskWrapper.__inner``.
"""

from __future__ import annotations

# Test list:
# 1. Der Abbruch wird waehrend der Queue-Deklaration verschluckt (der Broker
#    faengt den umgewandelten Kanalfehler): ``start()`` endet mit
#    ``CancelledError``, WORKER_ONLINE kommt nicht.
# 2. Der Abbruch wird waehrend der Queue-Deklaration zu einem Fehler des
#    Brokers: ``start()`` endet mit ``CancelledError``, der Fehler als Ursache.
# 3. Der Abbruch wird im WORKER_ONLINE-Handler verschluckt (der Dispatcher
#    faengt Handler-Fehler): ``start()`` endet trotzdem mit ``CancelledError``.
# 4. Ein Aufrufer, der schon mit ``cancelling() == 1`` startet (frueher
#    gefangener Abbruch ohne ``uncancel``), startet normal: Nur ein Anstieg
#    gegenueber dem Eintritt zaehlt (wie ``delivery.settle``, KTD9).
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from aio_pika.exceptions import ChannelNotFoundEntity

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.exceptions import BrokerConnectionError
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.worker import Worker

pytestmark = [pytest.mark.timeout(5), pytest.mark.befund("F4-28")]


async def _rpc_converting_cancel(entered: asyncio.Event) -> None:
    """Wie ein aiormq-RPC-Task, dessen Kanal beim Abbruch geschlossen wird:
    der Abbruch kommt als Kanalfehler beim Aufrufer an."""

    async def _inner() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise ChannelNotFoundEntity("channel closed while cancelling") from None

    rpc = asyncio.create_task(_inner())
    entered.set()
    await rpc


@pytest.fixture
def broker() -> MagicMock:
    b = MagicMock(spec=RabbitMQBroker)
    b.connect = AsyncMock()
    b.declare_queue = AsyncMock()
    b.consume = AsyncMock()
    b.close = AsyncMock()
    b.cancel_consume = AsyncMock()
    return b


@pytest.fixture
def backend() -> MagicMock:
    be = MagicMock(spec=ValkeyBackend)
    be.connect = AsyncMock()
    be.close = AsyncMock()
    return be


@pytest.fixture
def worker(broker: MagicMock, backend: MagicMock) -> Worker:
    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        queues=["default"],
        scheduler="off",
    )
    return Worker(broker=broker, backend=backend, registry=TaskRegistry(), config=config)


async def test_cancel_swallowed_during_queue_declaration_still_ends_start(broker: MagicMock, worker: Worker) -> None:
    declaring = asyncio.Event()

    async def _declare(*_args: object, **_kwargs: object) -> None:
        try:
            await _rpc_converting_cancel(declaring)
        except ChannelNotFoundEntity:
            return  # wie declare_chain: Queue fehlt, wird angelegt

    broker.declare_queue = AsyncMock(side_effect=_declare)
    online = asyncio.Event()

    async def _online(**_kwargs: object) -> None:
        online.set()

    worker._signal_dispatcher.connect(Signal.WORKER_ONLINE, _online)
    start = asyncio.create_task(worker.start())
    await asyncio.wait_for(declaring.wait(), timeout=2)

    start.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(start, timeout=1)
    assert not online.is_set()


async def test_cancel_turned_into_a_broker_error_during_declaration_ends_as_cancel(
    broker: MagicMock, worker: Worker
) -> None:
    declaring = asyncio.Event()

    async def _declare(*_args: object, **_kwargs: object) -> None:
        try:
            await _rpc_converting_cancel(declaring)
        except ChannelNotFoundEntity as exc:
            raise BrokerConnectionError("the connection was lost while declaring") from exc

    broker.declare_queue = AsyncMock(side_effect=_declare)
    start = asyncio.create_task(worker.start())
    await asyncio.wait_for(declaring.wait(), timeout=2)

    start.cancel()

    with pytest.raises(asyncio.CancelledError) as raised:
        await asyncio.wait_for(start, timeout=1)
    assert isinstance(raised.value.__cause__, BrokerConnectionError)


async def test_cancel_swallowed_in_a_worker_online_handler_still_ends_start(worker: Worker) -> None:
    in_handler = asyncio.Event()

    async def _online(**_kwargs: object) -> None:
        await _rpc_converting_cancel(in_handler)

    worker._signal_dispatcher.connect(Signal.WORKER_ONLINE, _online)
    start = asyncio.create_task(worker.start())
    await asyncio.wait_for(in_handler.wait(), timeout=2)

    start.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(start, timeout=1)


async def test_a_caller_already_cancelling_starts_and_shuts_down_normally(worker: Worker) -> None:
    online = asyncio.Event()

    async def _online(**_kwargs: object) -> None:
        online.set()

    worker._signal_dispatcher.connect(Signal.WORKER_ONLINE, _online)

    async def _caller() -> None:
        current = asyncio.current_task()
        assert current is not None
        current.cancel()
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass  # gefangen, ohne uncancel
        assert current.cancelling() == 1
        await worker.start()

    caller = asyncio.create_task(_caller())
    await asyncio.wait_for(online.wait(), timeout=2)
    worker._request_shutdown()

    await asyncio.wait_for(caller, timeout=2)
