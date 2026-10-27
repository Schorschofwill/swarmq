"""Sammelpause des Log-Listeners (Plan 2026-10-27-0800, U2).

Die Pause wird nie echt abgewartet (KTD6): ``_Pause`` ersetzt das
``Event`` in ``swarmq.logging_batch``, protokolliert jedes Warten mit Frist
und gibt den Listener erst frei, wenn der Test es sagt.
"""

from __future__ import annotations

# Test list:
# 1. [x] Pause 0.05: Nach der ersten INFO-Zeile wartet der Listener einmal
#        mit Frist 0.05; weitere INFO-Zeilen wecken ihn nicht und kommen nach
#        der Pause in Reihenfolge an (R1, R2).
# 2. [x] Pause 0: Der Listener wartet nie mit Frist (R3).
# 3. [x] Eine WARNING-Zeile in der Pause weckt; sie und die INFO-Zeilen
#        davor kommen ohne weiteres Warten an (R4).
# 4. [x] Kapazitaet 100: Der zehnte Eintrag weckt, der neunte nicht (R5).
# 5. [x] ``shutdown_logging`` in der Pause weckt; alle Zeilen kommen an (R6).
# 6. [x] ``stop()`` in der Pause weckt und kehrt zurueck (R6).
import logging
import queue
import threading
from typing import override

import pytest

from swarmq.logging import shutdown_logging
from swarmq.logging_queue import _StructlogQueueHandler

pytestmark = pytest.mark.timeout(5)


class _RecordingHandler(logging.Handler):
    """Ziel-Handler des Listeners: merkt sich die Nachrichten."""

    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    @override
    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class _Pause:
    """Ersatz fuer ``threading.Event``: ``wait`` protokolliert die Frist und
    blockiert, bis der Test ``release`` ruft oder jemand weckt (``set``)."""

    def __init__(self) -> None:
        self.timeouts: list[float | None] = []
        self.sets = 0
        self.waiting = threading.Event()
        self.started = threading.Condition()
        self._go = threading.Event()

    def wait(self, timeout: float | None = None) -> bool:
        with self.started:
            self.timeouts.append(timeout)
            self.started.notify_all()
        self.waiting.set()
        woken = self._go.wait(2)
        self._go.clear()
        self.waiting.clear()
        return woken

    def set(self) -> None:
        self.sets += 1
        self._go.set()

    def clear(self) -> None:
        pass

    def release(self) -> None:
        self._go.set()


@pytest.fixture
def pause(monkeypatch: pytest.MonkeyPatch) -> _Pause:
    fake = _Pause()
    monkeypatch.setattr("swarmq.logging_batch.Event", lambda: fake)
    return fake


def _pauses(pause: _Pause, count: int) -> bool:
    """Wartet, bis der Listener seine ``count``-te Pause beginnt."""
    with pause.started:
        return pause.started.wait_for(lambda: len(pause.timeouts) >= count, 2)


def _paused_handler(
    pause: _Pause,
    target: logging.Handler,
    maxsize: int = 100,
) -> _StructlogQueueHandler:
    """Handler mit Pause 0.05, dessen Listener schon laeuft und auf leerer
    Queue pausiert: Ab hier ist der Ablauf deterministisch."""
    handler = _StructlogQueueHandler(queue.Queue(maxsize=maxsize), target, batch_delay=0.05)
    assert handler.listener is not None
    handler.listener.start()
    assert _pauses(pause, 1)
    return handler


def _record(level: int = logging.INFO, msg: str = "msg") -> logging.LogRecord:
    return logging.LogRecord("test", level, __file__, 1, msg, None, None)


@pytest.mark.entscheidung("E-088")
def test_info_lines_wait_for_the_pause_without_waking_the_listener(pause: _Pause) -> None:
    target = _RecordingHandler()
    handler = _paused_handler(pause, target)

    handler.handle(_record(msg="a"))
    handler.handle(_record(msg="b"))
    handler.handle(_record(msg="c"))

    assert target.messages == []
    assert pause.timeouts == [0.05]
    assert pause.sets == 0
    pause.release()
    # Nach der Pause gibt er alles aus und pausiert erst wieder, wenn die
    # Queue leer ist.
    assert _pauses(pause, 2)
    assert target.messages == ["a", "b", "c"]
    pause.release()
    shutdown_logging()


def test_without_a_pause_the_listener_never_waits(pause: _Pause) -> None:
    target = _RecordingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=100), target, batch_delay=0.0)

    handler.handle(_record(msg="a"))
    handler.handle(_record(msg="b"))
    shutdown_logging()

    assert target.messages == ["a", "b"]
    assert pause.timeouts == []


@pytest.mark.entscheidung("E-088")
def test_a_warning_ends_the_pause_and_the_lines_before_it_follow(pause: _Pause) -> None:
    target = _RecordingHandler()
    handler = _paused_handler(pause, target)

    handler.handle(_record(msg="a"))
    handler.handle(_record(msg="b"))
    handler.handle(_record(logging.WARNING, msg="w"))

    assert pause.sets == 1
    assert _pauses(pause, 2)
    assert target.messages == ["a", "b", "w"]
    pause.release()
    shutdown_logging()


@pytest.mark.entscheidung("E-088")
def test_a_queue_filling_to_ten_percent_ends_the_pause(pause: _Pause) -> None:
    handler = _paused_handler(pause, _RecordingHandler(), maxsize=100)

    for _ in range(9):
        handler.handle(_record())
    assert pause.sets == 0
    handler.handle(_record())

    assert pause.sets == 1
    pause.release()
    shutdown_logging()


@pytest.mark.entscheidung("E-088")
def test_shutdown_ends_the_pause_and_writes_every_line(pause: _Pause) -> None:
    target = _RecordingHandler()
    handler = _paused_handler(pause, target)
    handler.handle(_record(msg="a"))
    handler.handle(_record(msg="b"))

    shutdown_logging()

    assert pause.sets == 1
    assert target.messages == ["a", "b"]


@pytest.mark.entscheidung("E-088")
def test_stop_ends_the_pause_and_writes_every_line(pause: _Pause) -> None:
    target = _RecordingHandler()
    handler = _paused_handler(pause, target)
    handler.handle(_record(msg="a"))
    assert handler.listener is not None

    handler.listener.stop()

    assert pause.sets == 1
    assert target.messages == ["a"]
