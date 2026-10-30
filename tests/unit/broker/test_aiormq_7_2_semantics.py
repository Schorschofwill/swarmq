"""Versionsschutz: Semantik von aiormq 7.2, auf die sich swarmq stützt (Plan 2026-10-29-1945, U3).

Charakterisierung gegen die installierte Bibliothek, ohne Broker. Die Tests
halten fest, was KTD2, KTD3 und KTD4 über aiormq 7.2.4 annehmen. Ändert ein
Upgrade eine dieser Stellen, wird diese Datei rot, bevor die Rückfall-Pfade in
``delivery.py``, ``publish_phase.py`` und ``connection.py`` still greifen.

``closing`` steht in ``aiormq.base.Base``; ``aiormq.Channel`` und
``aiormq.Connection`` erben es. Geprüft wird deshalb an einer minimalen
``Base``-Unterklasse ohne Verbindung.

Test-Liste:
# [x] ``Channel.closing`` ist die Property aus ``Base`` (nicht überschrieben)
# [x] zwei Zugriffe auf ``closing`` liefern zwei verschiedene Futures, keiner ist ``_closing``
# [x] jeder offene Beobachter hängt einen Callback an ``_closing`` (Leck-Prämisse KTD2/KTD6)
# [x] ein abgebrochener Beobachter entfernt seinen Callback von ``_closing``
# [x] ein Beobachter folgt dem Ende von ``_closing`` (Ergebnis wie Ausnahme)
# [x] nach dem Ende liefert ``closing`` das innere Future selbst, ohne neuen Callback
# [x] ``basic_publish`` kennt die Locals ``sent`` und ``drain_future`` (KTD3)
# [x] ``sent = True`` steht nach ``write_queue.put``, ``drain_future``/``sent`` werden
#     erst unter der Kanal-Sperre gebunden (KTD3, FS-08)
# [x] ``basic_publish`` läuft unter ``_raise_on_channel_close``
# [x] die Kanal-Sperre prüft ``is_closed`` vor und nach dem Erwerb (KTD2)
# [x] ``InvalidFrameError`` ist ``ProtocolSyntaxError``/``AMQPError``, weder
#     ``AMQPConnectionError`` noch ``AMQPChannelError`` (KTD4)
# [x] swarmqs ``_CHANNEL_TRANSIENT_ERRORS`` fängt ``InvalidFrameError`` (seit U5;
#     in U3 xfail strict)
# [x] ``TaskWrapper`` macht aus dem Abbruch des Aufrufers weiter den Fehler, mit
#     dem der Kanal schließt (F4-28; aiormq #239 gibt nur den Grund an
#     ``Task.cancel`` weiter, U8)
"""

from __future__ import annotations

import asyncio
import inspect
from typing import override

import aiormq
import pytest
from aiormq.abc import ExceptionType
from aiormq.base import Base, FutureStore
from aiormq.exceptions import (
    AMQPChannelError,
    AMQPConnectionError,
    AMQPError,
    ChannelNotFoundEntity,
    InvalidFrameError,
    ProtocolSyntaxError,
)

from swarmq.broker.rabbitmq.connection import _CHANNEL_TRANSIENT_ERRORS

pytestmark = [pytest.mark.timeout(5)]


class _Resource(Base):
    """``Base`` ohne Verbindung: nur ``closing``/``_closing`` werden geprüft."""

    @override
    async def _on_close(self, exc: ExceptionType | None = None) -> None:
        return None

    @property
    @override
    def is_closed(self) -> bool:
        return self._closing.done()

    @override
    def __str__(self) -> str:
        return "resource"


def _callbacks(future: asyncio.Future[object]) -> int:
    """Zahl der Done-Callbacks eines Futures (``_callbacks`` ist ``None`` ohne Callbacks)."""
    return len(future._callbacks or ())  # privat: asyncio bietet keine öffentliche Zählung


def test_the_channel_inherits_closing_from_base() -> None:
    assert aiormq.Channel.closing is Base.closing
    assert aiormq.Connection.closing is Base.closing


async def test_two_reads_of_closing_return_two_distinct_observers() -> None:
    resource = _Resource(loop=asyncio.get_running_loop())

    first = resource.closing
    second = resource.closing

    assert first is not second
    assert first is not resource._closing
    assert second is not resource._closing


async def test_each_pending_observer_keeps_a_callback_on_the_inner_future() -> None:
    resource = _Resource(loop=asyncio.get_running_loop())
    before = _callbacks(resource._closing)

    observers = [resource.closing for _ in range(100)]

    assert _callbacks(resource._closing) == before + 100
    assert not any(observer.done() for observer in observers)


async def test_a_cancelled_observer_removes_its_callback_from_the_inner_future() -> None:
    resource = _Resource(loop=asyncio.get_running_loop())
    before = _callbacks(resource._closing)
    observer = resource.closing
    assert _callbacks(resource._closing) == before + 1

    observer.cancel()
    await asyncio.sleep(0)  # Done-Callbacks laufen erst im nächsten Schritt der Loop

    assert _callbacks(resource._closing) == before
    assert not resource._closing.done()


async def test_an_observer_follows_the_end_of_the_inner_future() -> None:
    resource = _Resource(loop=asyncio.get_running_loop())
    ok = resource.closing
    resource._closing.set_result(None)
    await asyncio.sleep(0)
    assert ok.done()
    assert ok.result() is None

    failing = _Resource(loop=asyncio.get_running_loop())
    observer = failing.closing
    reason = ConnectionError("gone")
    failing._closing.set_exception(reason)
    await asyncio.sleep(0)
    assert observer.exception() is reason


async def test_after_the_end_closing_is_the_inner_future_itself() -> None:
    resource = _Resource(loop=asyncio.get_running_loop())
    resource._closing.set_result(None)

    assert resource.closing is resource._closing
    assert resource.closing is resource.closing


def test_basic_publish_has_the_locals_sent_and_drain_future() -> None:
    names = aiormq.Channel.basic_publish.__code__.co_varnames

    assert "sent" in names
    assert "drain_future" in names
    assert "confirmation" in names


def test_sent_is_set_after_the_frames_are_enqueued_and_bound_under_the_lock() -> None:
    """KTD3: ``sent`` heißt "in der ``write_queue``", noch nicht "auf dem Socket".

    ``drain_future`` und ``sent`` werden erst unter der Kanal-Sperre gebunden:
    Ein Publish, der in deren Warteschlange endet, hat beide nicht gebunden.
    Neu gegenüber 7.0: ``drain_future`` ist schon vor dem Einreihen gebunden
    (``None``); ``publish_phase._before_enqueue`` erkennt das Fenster zwischen
    Sperre und ``write_queue.put`` deshalb nicht mehr, ``sent`` schon."""
    source = inspect.getsource(aiormq.Channel.basic_publish)
    lock = source.index("async with self.lock:")
    drain_bound = source.index("drain_future: asyncio.Future | None = None")
    sent_false = source.index("sent = False")
    enqueue = source.index("await self.write_queue.put(")
    sent_true = source.index("sent = True")
    drained = source.index("await drain_future")

    assert lock < drain_bound < sent_false < enqueue < sent_true < drained


def test_basic_publish_runs_under_raise_on_channel_close() -> None:
    assert inspect.isfunction(aiormq.Channel._raise_on_channel_close)
    assert "self._raise_on_channel_close()" in inspect.getsource(aiormq.Channel.basic_publish)


def test_the_channel_lock_checks_is_closed_before_and_after_acquiring() -> None:
    """KTD2 (aiormq #240): Wer in der Warteschlange der Sperre steht, während
    der Kanal schließt, bekommt nach dem Erwerb ``ChannelInvalidStateError``,
    statt Frames in die tote Verbindung zu reihen. Das Verhalten belegt
    ``tests/integration/broker/test_channel_callbacks_flat.py`` gegen RabbitMQ."""
    lock = vars(aiormq.Channel)["lock"]
    assert isinstance(lock, property)
    assert lock.fget is not None
    source = inspect.getsource(lock.fget)

    before, _, after = source.partition("async with self.__lock:")
    assert "if self.is_closed:" in before
    assert "if self.is_closed:" in after


def test_invalid_frame_error_is_a_protocol_error_neither_connection_nor_channel() -> None:
    assert issubclass(InvalidFrameError, ProtocolSyntaxError)
    assert issubclass(InvalidFrameError, AMQPError)
    assert not issubclass(InvalidFrameError, AMQPConnectionError)
    assert not issubclass(InvalidFrameError, AMQPChannelError)


def test_swarmq_treats_an_invalid_frame_as_a_transient_channel_error() -> None:
    assert issubclass(InvalidFrameError, _CHANNEL_TRANSIENT_ERRORS)


async def test_the_task_wrapper_still_turns_the_callers_cancel_into_the_closing_error() -> None:
    """F4-28 unter aiormq 7.2.4: Schließt der Kanal (``reject_all``) einen
    laufenden RPC und trifft danach der Abbruch den Aufrufer, kommt beim
    Aufrufer der Kanalfehler an, nicht ``CancelledError``; der Aufrufer
    trägt den Abbruch weiter in ``cancelling()``. #239 hat daran nichts
    geändert, deshalb bleibt die Prüfung in ``WorkerLifecycle.start``."""
    store = FutureStore(asyncio.get_running_loop())
    rpc_running = asyncio.Event()
    rejected = asyncio.Event()

    async def rpc() -> None:
        rpc_running.set()
        try:
            await asyncio.Event().wait()
        finally:
            # Der Kanal hat in den RPC-Task geworfen; der Task lebt noch, bis
            # der Abbruch des Aufrufers ihn trifft.
            rejected.set()
            await asyncio.Event().wait()

    async def caller() -> None:
        await store.create_task(rpc())

    waiting = asyncio.create_task(caller())
    await rpc_running.wait()
    rejecting = asyncio.ensure_future(store.reject_all(ChannelNotFoundEntity("closing")))
    await rejected.wait()
    waiting.cancel()

    with pytest.raises(ChannelNotFoundEntity):
        await waiting
    assert waiting.cancelling() == 1
    await rejecting
