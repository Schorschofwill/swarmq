"""Tests fuer den Queue- und Listener-Kern von ``swarmq.logging``.

Plan 2026-10-06-0831, U3 (R6, R7, R15; KTD5).
"""

from __future__ import annotations

# Test list:
# 1. ``prepare`` gibt denselben Record mit erhaltenem ``exc_info`` zurueck;
#    formatiert wird im Listener-Thread (Thread-Name im Formatter).
# 2. Eine volle Queue blockiert nicht: ``dropped`` steigt, innerhalb einer
#    Sekunde erscheint genau eine Meldung, danach wieder eine (Zeit gemockt).
# 3. JSON-Modus: die Drop-Meldung ist parsebares JSON mit
#    ``event=log_records_dropped`` und ``count`` (AE7).
# 4. Nach ``queue.ShutDown`` wird gezaehlt statt geworfen.
# 5. Ein zweiter ``start`` wirft nicht und startet keinen zweiten Thread.
# 6. ``shutdown_logging()`` schreibt alle Eintraege der Queue und ist
#    mehrfach aufrufbar.
# 7. Nach ``shutdown_logging()`` startet der naechste Eintrag den Listener
#    wieder und wird geschrieben.
# 8. Ein atexit-Hook ist genau einmal registriert, auch nach mehrfacher
#    Konfiguration.
# 9. Nach einem echten Fork (``os.fork``, also ueber den registrierten Hook)
#    ist der Zustand im Kind zurueckgesetzt, der naechste Eintrag startet
#    einen Listener.
# 10. Ein vor dem Fork eingereihter Record wird vom Kind nicht ausgegeben.
# 11. Zwei Threads, die gleichzeitig den Listener starten (erster Eintrag
#     gegen expliziten ``start``), starten genau einen Listener.
#     ``Handler.handle`` serialisiert Eintraege desselben Handlers schon
#     ueber dessen Lock.
# 12. Bei 90 % Fuellstand wird ein INFO-Record verworfen, ein
#     WARNING-Record noch eingereiht; bei voller Queue auch WARNING nicht.
# 13. ``shutdown_logging()`` auf einer per ``Queue.shutdown`` geschlossenen
#     Queue wirft nicht und haengt nicht.
# 14. Ein neuer Handler startet keinen Thread, erst der erste Eintrag
#     (Forkserver-Check: Import und Konfiguration bleiben einfaedig).
# 15. Ein beim Fork gehaltenes Start-Lock blockiert das Kind nicht.
# 16. ``setup_logging`` nutzt den neustartbaren Kern.
# 17. Der Fork-Hook setzt auch den prozessweiten Drop-Zaehler zurueck (in 9).
# 18. Im echten Kind stoppt ``shutdown_logging`` den geerbten, beim Fork
#     laufenden Listener des Elternprozesses nicht: Auf dessen voller
#     Queue-Kopie haengte der blockierende Sentinel.
import atexit
import contextlib
import io
import json
import logging
import queue
import sys
import threading
import types
from typing import Any

import pytest

from swarmq.logging import (
    _StructlogQueueHandler,
    dropped_log_records,
    setup_logging,
    shutdown_logging,
)
from tests.unit._fork_child import run_in_forked_child

pytestmark = pytest.mark.timeout(5)

_LISTENER_THREAD = "swarmq-log-listener"


class _RecordingHandler(logging.Handler):
    """Ziel-Handler des Listeners: merkt sich Record und Thread je Eintrag."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []
        self.threads: list[str] = []
        self.written = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.threads.append(threading.current_thread().name)
        self.written.set()


def _record(level: int = logging.INFO, msg: str = "msg") -> logging.LogRecord:
    return logging.LogRecord("test", level, __file__, 1, msg, None, None)


def _listener_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == _LISTENER_THREAD]


def test_prepare_passes_record_through_and_listener_thread_formats():
    target = _RecordingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), target)
    try:
        raise ValueError("boom")
    except ValueError:
        record = logging.LogRecord(
            "test", logging.ERROR, __file__, 1, "msg", None, sys.exc_info()
        )

    prepared = handler.prepare(record)
    assert prepared is record
    assert prepared.exc_info is not None

    handler.handle(record)
    shutdown_logging()

    assert target.records == [record]
    assert target.threads == [_LISTENER_THREAD]


class _Clock:
    """Steuerbarer Ersatz fuer ``time.monotonic``."""

    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr("swarmq.logging.time.monotonic", fake)
    return fake


def test_full_queue_drops_without_blocking_and_notices_once_per_second(
    clock: _Clock, capsys: pytest.CaptureFixture[str]
):
    handler = _StructlogQueueHandler(queue.Queue(maxsize=1))
    handler.handle(_record())

    handler.handle(_record())
    clock.now += 0.5
    handler.handle(_record())
    assert handler.dropped == 2
    assert capsys.readouterr().err.count("dropped") == 1

    clock.now += 0.5
    handler.handle(_record())
    err = capsys.readouterr().err
    assert handler.dropped == 3
    assert err.count("dropped") == 1
    assert "3 record(s) dropped" in err


def test_json_mode_drop_notice_is_a_json_line(
    clock: _Clock, capsys: pytest.CaptureFixture[str]
):
    """Covers AE7."""
    handler = _StructlogQueueHandler(queue.Queue(maxsize=1), json=True)
    handler.handle(_record())
    handler.handle(_record())
    handler.handle(_record())

    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["event"] == "log_records_dropped"
    assert data["count"] == 1
    assert data["level"] == "warning"
    assert data["timestamp"].endswith("Z")


def test_shut_down_queue_counts_instead_of_raising(
    capsys: pytest.CaptureFixture[str],
):
    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=10)
    handler = _StructlogQueueHandler(log_queue)
    log_queue.shutdown()

    handler.handle(_record())

    assert handler.dropped == 1
    err = capsys.readouterr().err
    assert "Traceback" not in err
    assert "1 record(s) dropped" in err


def test_second_start_neither_raises_nor_starts_a_second_thread():
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), _RecordingHandler())
    assert handler.listener is not None
    before = len(_listener_threads())

    handler.listener.start()
    handler.listener.start()

    assert len(_listener_threads()) == before + 1


class _BlockingHandler(_RecordingHandler):
    """Haelt den Listener beim ersten Eintrag fest, bis ``unblock`` gesetzt
    ist; spaetere Eintraege laufen durch."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.unblock = threading.Event()

    def handle(self, record: logging.LogRecord) -> logging.LogRecord | bool:
        # Vor ``super().handle``: dort haelt der Handler sein Lock, und ein
        # zweiter Listener (simulierter Fork) kaeme nicht mehr durch.
        if not self.entered.is_set():
            self.entered.set()
            self.unblock.wait()
        return super().handle(record)


class _ReleasingQueue(queue.Queue[logging.LogRecord]):
    """Gibt den blockierten Listener frei, sobald blockierend eingereiht wird."""

    def __init__(self, maxsize: int, unblock: threading.Event) -> None:
        super().__init__(maxsize)
        self._unblock = unblock

    def put(self, item, block=True, timeout=None):  # type: ignore[no-untyped-def]
        if block:
            self._unblock.set()
        super().put(item, block, timeout)


def test_shutdown_logging_writes_every_queued_record_and_is_repeatable():
    target = _BlockingHandler()
    handler = _StructlogQueueHandler(_ReleasingQueue(2, target.unblock), target)
    try:
        handler.handle(_record(msg="a"))
        target.entered.wait()
        handler.handle(_record(msg="b"))
        handler.handle(_record(msg="c"))
        assert handler.queue.full()

        shutdown_logging()
        shutdown_logging()
    finally:
        target.unblock.set()

    assert [r.msg for r in target.records] == ["a", "b", "c"]
    assert handler.dropped == 0


def test_record_after_shutdown_restarts_listener_and_is_written():
    target = _RecordingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), target)
    handler.handle(_record(msg="before"))
    shutdown_logging()
    assert not _listener_threads()

    handler.handle(_record(msg="after"))
    assert len(_listener_threads()) == 1
    shutdown_logging()

    assert [r.msg for r in target.records] == ["before", "after"]


def test_atexit_hook_is_registered_exactly_once():
    setup_logging("INFO", True)
    setup_logging("INFO", True)
    _StructlogQueueHandler(queue.Queue(maxsize=10), _RecordingHandler())

    registered = atexit._ncallbacks()
    atexit.unregister(shutdown_logging)  # entfernt jede Registrierung
    try:
        assert registered - atexit._ncallbacks() == 1
    finally:
        atexit.register(shutdown_logging)


def test_after_fork_hook_resets_queue_and_drop_state(
    clock: _Clock, capsys: pytest.CaptureFixture[str]
):
    handler = _StructlogQueueHandler(queue.Queue(maxsize=1))
    handler.handle(_record())
    handler.handle(_record())
    assert capsys.readouterr().err.count("dropped") == 1

    assert dropped_log_records() >= 1

    def child() -> None:
        assert handler.dropped == 0
        assert dropped_log_records() == 0
        assert handler.queue.empty()
        handler.handle(_record())
        handler.handle(_record())
        assert handler.dropped == 1
        assert "1 record(s) dropped" in capsys.readouterr().err

    assert run_in_forked_child(child) == 0


def test_after_fork_next_record_starts_new_listener_without_inherited_records():
    target = _BlockingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), target)
    try:
        handler.handle(_record(msg="parent-busy"))
        target.entered.wait()
        handler.handle(_record(msg="inherited"))
        parent_listener = handler.listener

        def child() -> None:
            handler.handle(_record(msg="child"))
            child_listener = handler.listener
            assert child_listener is not parent_listener
            assert child_listener is not None
            assert child_listener.running
            child_listener.stop()
            assert [r.msg for r in target.records] == ["child"]

        assert run_in_forked_child(child) == 0
    finally:
        target.unblock.set()


def test_concurrent_first_record_and_start_run_exactly_one_listener(
    monkeypatch: pytest.MonkeyPatch,
):
    gate = threading.Barrier(2)

    class _GatedThread(threading.Thread):
        """Haelt den Listener-Start auf, bis ein zweiter Start ankommt; ohne
        Lock startet jeder der beiden einen Listener."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            with contextlib.suppress(threading.BrokenBarrierError):
                gate.wait(timeout=0.05)
            super().__init__(*args, **kwargs)

    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), _RecordingHandler())
    assert handler.listener is not None
    # ``Handler.handle`` serialisiert Eintraege desselben Handlers schon
    # ueber dessen Lock; konkurrieren kann der erste Eintrag aber mit einem
    # expliziten Start (etwa des Listeners, den ``setup_logging`` liefert).
    writers = [
        threading.Thread(target=handler.handle, args=(_record(),)),
        threading.Thread(target=handler.listener.start),
    ]
    monkeypatch.setattr(
        "swarmq.logging.threading", types.SimpleNamespace(Thread=_GatedThread)
    )
    for writer in writers:
        writer.start()
    for writer in writers:
        writer.join()

    assert len(_listener_threads()) == 1


def test_after_fork_hook_does_not_inherit_a_held_start_lock(
    monkeypatch: pytest.MonkeyPatch,
):
    """Forkt ein anderer Thread gerade waehrend eines Listener-Starts, erbt das
    Kind ein gehaltenes Lock ohne Besitzer."""
    inside = threading.Event()
    proceed = threading.Event()

    class _StuckThread(threading.Thread):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            if not inside.is_set():
                inside.set()
                proceed.wait()
            super().__init__(*args, **kwargs)

    target = _RecordingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), target)
    assert handler.listener is not None
    starter = threading.Thread(target=handler.listener.start)
    monkeypatch.setattr(
        "swarmq.logging.threading", types.SimpleNamespace(Thread=_StuckThread, Lock=threading.Lock)
    )
    try:
        starter.start()
        inside.wait()

        def child() -> None:
            handler.handle(_record(msg="child"))
            assert target.written.wait(timeout=1)

        assert run_in_forked_child(child) == 0
    finally:
        proceed.set()
        starter.join()


def test_last_tenth_of_the_queue_is_reserved_for_warnings():
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10))
    for _ in range(9):
        handler.handle(_record(logging.INFO))
    assert handler.dropped == 0

    handler.handle(_record(logging.INFO))
    assert handler.dropped == 1
    handler.handle(_record(logging.WARNING))
    assert handler.dropped == 1
    assert handler.queue.full()

    handler.handle(_record(logging.ERROR))
    assert handler.dropped == 2


def test_shutdown_logging_tolerates_a_shut_down_queue():
    target = _RecordingHandler()
    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=10)
    handler = _StructlogQueueHandler(log_queue, target)
    handler.handle(_record(msg="a"))
    assert target.written.wait(timeout=1)

    log_queue.shutdown()
    shutdown_logging()
    shutdown_logging()

    assert not _listener_threads()
    assert [r.msg for r in target.records] == ["a"]


def test_new_handler_starts_no_thread_until_first_record():
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), _RecordingHandler())
    assert not _listener_threads()

    handler.handle(_record())
    assert len(_listener_threads()) == 1


def test_setup_logging_uses_the_restartable_core():
    listener = setup_logging("INFO", True)
    stream = io.StringIO()
    listener.handlers[0].stream = stream  # type: ignore[attr-defined]
    log = logging.getLogger("swarmq.test")

    log.warning("first")
    shutdown_logging()
    log.warning("second")
    shutdown_logging()

    events = [json.loads(line)["event"] for line in stream.getvalue().splitlines()]
    assert events == ["first", "second"]


def test_child_shutdown_skips_the_running_listener_inherited_from_the_parent() -> None:
    target = _BlockingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=2), target)
    try:
        handler.handle(_record(msg="parent-busy"))
        target.entered.wait()
        handler.handle(_record(msg="a"))
        handler.handle(_record(logging.WARNING, msg="b"))
        assert handler.queue.full()

        assert run_in_forked_child(shutdown_logging) == 0
    finally:
        target.unblock.set()
