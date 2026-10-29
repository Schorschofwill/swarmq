"""Kanal-Close ohne Abbruch (F4-30, ``broker/rabbitmq/channel_close.py``).

Bricht eine Frist ein laufendes ``channel.close()`` ab, schickt aiormq ein
zweites ``Channel.Close(504)`` auf dieselbe Kanalnummer, und RabbitMQ beendet
die ganze Verbindung, wenn es das erste Close schon beantwortet hat.
``close_without_abort`` begrenzt darum nur das Warten: Frist und Abbruch des
Aufrufers treffen den Aufrufer, das Close läuft weiter (Plan
docs/plans/2026-10-29-1945-chore-aiormq-7-2-anheben-plan.md, KTD1).

Test-Liste:
# [x] Close fertig in der Frist: kehrt zurück; ein Fehler des Close kommt beim Aufrufer an
# [x] Frist abgelaufen: ``TimeoutError`` beim Aufrufer, das Close wird nicht abgebrochen und endet später
# [x] Abbruch des Aufrufers: ``CancelledError`` kommt an, das Close läuft weiter
# [x] ohne Frist: wartet, bis das Close fertig ist
# [x] scheitert das Close nach der Frist: kein „exception was never retrieved“
"""

from __future__ import annotations

import asyncio
import gc

import pytest

from swarmq.broker.rabbitmq.channel_close import close_without_abort

pytestmark = pytest.mark.timeout(5)


class _SlowChannel:
    """Ein Kanal, dessen ``close()`` erst nach ``release`` fertig wird."""

    def __init__(self, error: BaseException | None = None) -> None:
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.cancelled = False
        self.closed = False
        self._error = error

    async def close(self) -> None:
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        if self._error is not None:
            raise self._error
        self.closed = True


async def test_a_close_within_the_deadline_returns() -> None:
    channel = _SlowChannel()
    channel.release.set()

    await close_without_abort(channel, timeout_s=1.0)

    assert channel.closed


async def test_an_error_of_a_close_within_the_deadline_reaches_the_caller() -> None:
    channel = _SlowChannel(error=RuntimeError("boom"))
    channel.release.set()

    with pytest.raises(RuntimeError, match="boom"):
        await close_without_abort(channel, timeout_s=1.0)


async def test_a_missed_deadline_raises_but_lets_the_close_finish() -> None:
    channel = _SlowChannel()

    with pytest.raises(TimeoutError):
        await close_without_abort(channel, timeout_s=0.05)
    channel.release.set()
    for _ in range(5):
        await asyncio.sleep(0)

    assert not channel.cancelled
    assert channel.closed


async def test_cancelling_the_caller_does_not_cancel_the_close() -> None:
    channel = _SlowChannel()
    caller = asyncio.create_task(close_without_abort(channel, timeout_s=None))
    await channel.started.wait()

    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    channel.release.set()
    for _ in range(5):
        await asyncio.sleep(0)

    assert not channel.cancelled
    assert channel.closed


async def test_without_a_deadline_it_waits_for_the_close() -> None:
    channel = _SlowChannel()
    caller = asyncio.create_task(close_without_abort(channel, timeout_s=None))
    await channel.started.wait()
    await asyncio.sleep(0.05)

    assert not caller.done()
    channel.release.set()
    await caller
    assert channel.closed


async def test_a_close_failing_after_the_deadline_leaves_no_unretrieved_exception() -> None:
    channel = _SlowChannel(error=RuntimeError("late"))
    unretrieved: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _loop, context: unretrieved.append(context))
    try:
        with pytest.raises(TimeoutError):
            await close_without_abort(channel, timeout_s=0.05)
        channel.release.set()
        for _ in range(5):
            await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(None)

    assert unretrieved == []
