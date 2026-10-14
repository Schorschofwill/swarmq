"""Consumer-Wächter im Herunterfahren gegen einen echten ``aiormq.Channel``
(FS-09, Plan 2026-10-14-0748, U10, KTD16).

Ein abgebrochenes ``basic.cancel``/``basic.consume`` beantwortet aiormq mit
``Channel.Close(504)``; auf dem Kanal liegen alle Consumer des Workers. Der
Kanal läuft hier ohne Socket (wie im Nachweis
``test_befund_fs09_consumer_watch_rpc_abort.py``), der Broker antwortet nicht
von selbst; Antworten legt der Test in ``rpc_frames``.

Test-Liste:
# [x] stop() während eines laufenden RPC sendet kein Channel.Close (abkoppeln statt abbrechen)
# [x] die consume-Antwort eines abgekoppelten Takts nach cancel_all: Tag sofort abgemeldet, nicht übernommen
# [x] eine späte consume-Antwort, deren Abmeldung scheitert: geloggt, nicht übernommen
# [x] ein abgekoppelter Takt endet mit dem Schließen des Kanals (kein hängender Task)
# [x] ein abgekoppelter Takt meldet nach stop() keinen weiteren Eintrag neu an (Review U10)
# Kanalbezug vor dem RPC (aio-pika wartet dort auf ``connection.ready()``, ohne Frist):
# [x] cancel_all bei getrennter Verbindung: endet nach timeout_s mit Fehler je Tag
# [x] cancel_all gibt dem RPC die Notbremse mit
# [x] eine späte consume-Antwort bei getrennter Verbindung: Abmelden endet, geloggt
# [x] Verbindung bricht nach dem eigenen Cancel weg: Neuanmeldung endet, geloggt
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast, override

import pytest
from aiormq.abc import AbstractConnection, ExceptionType
from aiormq.base import Base
from aiormq.channel import Channel
from pamqp import commands as spec
from pamqp.frame import unmarshal

from swarmq.broker.rabbitmq import consumer_watch as consumer_watch_module
from swarmq.broker.rabbitmq.consumer_watch import ConsumerWatch
from tests.support.consumer_fakes import FakeChannel, FakeQueue, FakeUnderlay

pytestmark = pytest.mark.timeout(5)


class _Connector(Base):
    """Verbindungsseite ohne Broker; gesendete Frames bleiben in ``write_queue``."""

    def __init__(self) -> None:
        super().__init__(loop=asyncio.get_running_loop())
        self.publisher_confirms = True
        self.connection_tune = SimpleNamespace(frame_max=131072)
        self.write_queue: asyncio.Queue[Any] = asyncio.Queue()

    @override
    async def _on_close(self, exc: ExceptionType | None = None) -> None:
        return None

    @override
    def __str__(self) -> str:
        return "u10-connector"


class _Queue:
    """Die Teile von aio-pikas ``RobustQueue``, die der Wächter benutzt."""

    def __init__(self, underlay: Channel) -> None:
        self.underlay = underlay
        self.channel = self

    async def get_underlay_channel(self) -> Channel:
        return self.underlay

    async def cancel(self, tag: str, timeout: float | None = None) -> None:
        await self.underlay.basic_cancel(tag, timeout=timeout)

    async def consume(self, callback: Any, timeout: float | None = None) -> str:
        ok = await self.underlay.basic_consume("tasks", callback, timeout=timeout)
        return str(ok.consumer_tag)


def _decode(item: Any) -> list[Any]:
    frames: list[Any] = []
    payload = item.payload
    while payload:
        consumed, _channel, frame = unmarshal(payload)
        payload = payload[consumed:]
        frames.append(frame)
    return frames


def _sent(connector: _Connector) -> list[Any]:
    """Alle Methoden-Frames, die bisher in die write_queue gingen."""
    frames: list[Any] = []
    while not connector.write_queue.empty():
        frames.extend(_decode(connector.write_queue.get_nowait()))
    return frames


def _closes(frames: list[Any]) -> list[spec.Channel.Close]:
    return [frame for frame in frames if isinstance(frame, spec.Channel.Close)]


@pytest.fixture
async def channel() -> AsyncIterator[tuple[_Connector, Channel]]:
    connector = _Connector()
    underlay = Channel(cast("AbstractConnection", connector), 1, publisher_confirms=True)
    try:
        yield connector, underlay
    finally:
        await connector.close(None)


@pytest.fixture
def short_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wartezeit von ``stop()`` auf den laufenden Takt verkürzen (Default 5 s)."""
    monkeypatch.setattr(consumer_watch_module, "_STOP_TIMEOUT_S", 0.01)


async def _callback(_message: Any) -> None:
    return None


async def _next_sent(connector: _Connector, kind: type[Any]) -> list[Any]:
    """Sammelt gesendete Frames, bis einer vom Typ *kind* dabei ist."""
    frames: list[Any] = []
    while not any(isinstance(frame, kind) for frame in frames):
        frames.extend(_decode(await connector.write_queue.get()))
    return frames


async def test_stop_during_a_running_rpc_sends_no_channel_close(
    channel: tuple[_Connector, Channel], short_stop: None, swarmq_logs: list[dict[str, Any]]
) -> None:
    connector, underlay = channel
    watch = ConsumerWatch()
    watch.add("tasks", _Queue(underlay), _callback, "u10-ctag")
    watch._entries[0].tag = None  # abgemeldet, der Takt meldet neu an
    watch.start(interval_s=3600.0)
    watch.request_check()
    await asyncio.wait_for(_next_sent(connector, spec.Basic.Consume), 1.0)

    await watch.stop()

    assert _closes(_sent(connector)) == []
    [detached] = [e for e in swarmq_logs if e["event"] == "consumer_watch_detached"]
    assert detached["timeout_s"] == 0.01


async def test_a_late_consume_answer_after_cancel_all_is_cancelled_at_once(
    channel: tuple[_Connector, Channel], short_stop: None
) -> None:
    connector, underlay = channel
    watch = ConsumerWatch()
    watch.add("tasks", _Queue(underlay), _callback, "u10-ctag")
    watch._entries[0].tag = None  # abgemeldet, der Takt meldet neu an
    watch.start(interval_s=3600.0)
    watch.request_check()
    await asyncio.wait_for(_next_sent(connector, spec.Basic.Consume), 1.0)
    assert await watch.cancel_all() == []

    underlay.rpc_frames.put_nowait(spec.Basic.ConsumeOk(consumer_tag="late-1"))
    frames = await asyncio.wait_for(_next_sent(connector, spec.Basic.Cancel), 1.0)
    underlay.rpc_frames.put_nowait(spec.Basic.CancelOk(consumer_tag="late-1"))

    assert await watch.wait_cancelled("late-1", timeout=1.0) is True
    assert [f.consumer_tag for f in frames if isinstance(f, spec.Basic.Cancel)] == ["late-1"]
    assert _closes(frames) == []
    assert watch.tags() == []
    assert not watch.is_watched("late-1")


async def test_a_late_consume_answer_whose_cancel_fails_is_logged_and_not_taken_over(
    short_stop: None, swarmq_logs: list[dict[str, Any]]
) -> None:
    queue = FakeQueue("q1", FakeChannel(FakeUnderlay()))
    entered, answer = asyncio.Event(), asyncio.Event()
    plain_consume = queue.consume

    async def slow_consume(callback: Any, timeout: float | None = None) -> str:
        entered.set()
        await answer.wait()
        return await plain_consume(callback, timeout)

    queue.consume = slow_consume  # type: ignore[method-assign]
    queue.cancel_error = RuntimeError("channel gone")
    watch = ConsumerWatch()
    watch.add("q1", queue, object(), "tag-1")
    watch._entries[0].tag = None
    before = asyncio.all_tasks()
    watch.start(interval_s=3600.0)
    watch.request_check()
    await asyncio.wait_for(entered.wait(), 1.0)
    await watch.cancel_all()
    [detached] = asyncio.all_tasks() - before

    answer.set()
    await asyncio.wait_for(detached, 1.0)

    [failed] = [e for e in swarmq_logs if e["event"] == "consumer_cancel_failed"]
    assert failed["consumer_tag"] == "q1-new-1"
    assert failed["error_type"] == "RuntimeError"
    assert watch.tags() == []
    assert not [e for e in swarmq_logs if e["event"] == "consumer_reregistered"]


async def test_a_detached_tick_ends_when_the_channel_closes(
    channel: tuple[_Connector, Channel], short_stop: None
) -> None:
    connector, underlay = channel
    watch = ConsumerWatch()
    watch.add("tasks", _Queue(underlay), _callback, "u10-ctag")
    watch._entries[0].tag = None
    before = asyncio.all_tasks()
    watch.start(interval_s=3600.0)
    watch.request_check()
    await asyncio.wait_for(_next_sent(connector, spec.Basic.Consume), 1.0)
    await watch.stop()
    detached = {task for task in asyncio.all_tasks() - before if not task.done()}
    assert detached, "der Takt haengt im RPC und ist abgekoppelt"

    await connector.close(None)

    _done, pending = await asyncio.wait(detached, timeout=1.0)
    assert pending == set()
    assert _closes(_sent(connector)) == []


async def test_a_detached_tick_registers_no_further_entry_after_stop(short_stop: None) -> None:
    """Review U10: Der erste Eintrag hängt im Cancel über ``stop()`` hinaus;
    nach dessen Antwort darf der Takt den zweiten (Neuanmeldung ausstehend)
    nicht mehr am Broker anmelden -- der Worker drainiert schon."""
    first = FakeQueue("q1", FakeChannel(FakeUnderlay()))
    second = FakeQueue("q2", FakeChannel(FakeUnderlay()))
    entered, answer = asyncio.Event(), asyncio.Event()
    plain_cancel = first.cancel

    async def slow_cancel(tag: str, timeout: float | None = None) -> None:
        entered.set()
        await answer.wait()
        await plain_cancel(tag, timeout)

    first.cancel = slow_cancel  # type: ignore[method-assign]
    watch = ConsumerWatch()
    watch.add("q1", first, object(), "tag-1")
    watch.add("q2", second, object(), "tag-2")
    watch._entries[0].seen_on = first.channel.underlay  # Tag fehlt auf offenem, unverändertem Kanal
    watch._entries[1].tag = None  # Neuanmeldung steht aus
    before = asyncio.all_tasks()
    watch.start(interval_s=3600.0)
    watch.request_check()
    await asyncio.wait_for(entered.wait(), 1.0)
    await watch.stop()
    [detached] = asyncio.all_tasks() - before

    answer.set()
    await asyncio.wait_for(detached, 1.0)

    assert first.cancelled == ["tag-1"]
    assert second.consumed == []


class _GatedQueue:
    """Wie aio-pikas ``RobustQueue``: ``cancel``/``consume`` holen den Kanal
    selbst über ``get_underlay_channel``, das auf ``connection.ready()``
    wartet -- bei getrennter Verbindung (``ready`` gelöscht) ohne Ende."""

    def __init__(self) -> None:
        self.ready = asyncio.Event()
        self.ready.set()
        self.underlay = FakeUnderlay()
        self.channel = self
        self.cancels: list[tuple[str, float | None]] = []
        self.consumes = 0

    async def get_underlay_channel(self) -> FakeUnderlay:
        await self.ready.wait()
        return self.underlay

    async def cancel(self, tag: str, timeout: float | None = None) -> None:
        await self.get_underlay_channel()
        self.cancels.append((tag, timeout))
        self.underlay.consumers.pop(tag, None)

    async def consume(self, callback: Any, timeout: float | None = None) -> str:
        await self.get_underlay_channel()
        self.consumes += 1
        tag = f"gated-{self.consumes}"
        self.underlay.consumers[tag] = callback
        return tag


async def test_cancel_all_on_a_disconnected_channel_ends_after_the_channel_timeout() -> None:
    queue = _GatedQueue()
    queue.ready.clear()  # Verbindung getrennt, aio-pika wartet auf ready()
    watch = ConsumerWatch(timeout_s=0.01)
    watch.add("q1", queue, object(), "tag-1")

    errors = await asyncio.wait_for(watch.cancel_all(), 1.0)

    [(tag, exc)] = errors
    assert tag == "tag-1"
    assert isinstance(exc, TimeoutError)
    assert queue.cancels == []


async def test_cancel_all_gives_the_rpc_the_emergency_timeout() -> None:
    queue = _GatedQueue()
    watch = ConsumerWatch(rpc_timeout_s=7.5)
    watch.add("q1", queue, object(), "tag-1")

    assert await watch.cancel_all() == []

    assert queue.cancels == [("tag-1", 7.5)]


async def test_a_late_consume_answer_on_a_disconnected_channel_ends_its_cancel(
    short_stop: None, swarmq_logs: list[dict[str, Any]]
) -> None:
    queue = _GatedQueue()
    entered, answer = asyncio.Event(), asyncio.Event()
    plain_consume = queue.consume

    async def slow_consume(callback: Any, timeout: float | None = None) -> str:
        entered.set()
        await answer.wait()
        tag = await plain_consume(callback, timeout)
        queue.ready.clear()  # nach consume-ok bricht die Verbindung weg
        return tag

    queue.consume = slow_consume  # type: ignore[method-assign]
    watch = ConsumerWatch(timeout_s=0.01)
    watch.add("q1", queue, object(), "tag-1")
    watch._entries[0].tag = None
    before = asyncio.all_tasks()
    watch.start(interval_s=3600.0)
    watch.request_check()
    await asyncio.wait_for(entered.wait(), 1.0)
    await watch.cancel_all()
    [detached] = asyncio.all_tasks() - before

    answer.set()
    await asyncio.wait_for(detached, 1.0)

    [failed] = [e for e in swarmq_logs if e["event"] == "consumer_cancel_failed"]
    assert failed["consumer_tag"] == "gated-1"
    assert failed["error_type"] == "TimeoutError"


async def test_a_connection_lost_after_the_own_cancel_ends_the_reregistration(
    swarmq_logs: list[dict[str, Any]],
) -> None:
    queue = _GatedQueue()
    plain_cancel = queue.cancel

    async def cancel_then_disconnect(tag: str, timeout: float | None = None) -> None:
        await plain_cancel(tag, timeout)
        queue.ready.clear()  # nach cancel-ok bricht die Verbindung weg

    queue.cancel = cancel_then_disconnect  # type: ignore[method-assign]
    watch = ConsumerWatch(timeout_s=0.01)
    watch.add("q1", queue, object(), "tag-1")
    watch._entries[0].seen_on = queue.underlay  # Tag fehlt auf offenem, unverändertem Kanal

    await asyncio.wait_for(watch.tick(), 1.0)

    assert queue.consumes == 0
    [failed] = [e for e in swarmq_logs if e["event"] == "consumer_reregister_failed"]
    assert failed["error_type"] == "TimeoutError"
    assert watch.tags() == []
