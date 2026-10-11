"""Tests fuer den Queue- und Listener-Kern (``swarmq.logging_queue``).

Plan 2026-10-06-0831, U3 (R6, R7, R15; KTD5); ausgelagert in
Plan 2026-10-06-1811, U1.
"""

from __future__ import annotations

# Test list:
# 1. ``prepare`` gibt denselben Record mit erhaltenem ``exc_info`` zurueck;
#    formatiert wird im Listener-Thread (Thread-Name im Formatter).
# 2. Eine volle Queue blockiert nicht: ``dropped`` steigt.
# 3. (ersetzt durch 26: die JSON-Meldung kommt seit U3 aus dem Listener)
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
# 19. Nach der Auslagerung (Plan 2026-10-06-1811, U1) bleiben die
#     oeffentlichen Namen aus ``swarmq.logging`` importierbar.
# Plan 2026-10-06-1811, U2 (R2, KTD1) -- der Listener stirbt nie:
# 20. Ein toter Listener-Thread (Fake mit ``is_alive()`` gleich False): Der
#     naechste Eintrag startet einen neuen Thread; der Record kommt an, und
#     die Records in der Queue des toten Listeners gehen nicht verloren und
#     zaehlen nicht als verworfen.
# 21. Ein Ziel-Handler, der wirft: Der Listener lebt weiter, der naechste
#     Record kommt an; kein Aufruf von ``threading.excepthook``.
# 22. Die Fehler werden gezaehlt; ``log_handler_errors`` erscheint ueber die
#     Handler des Listeners hoechstens einmal pro Sekunde (Zeit gemockt).
# 23. Ein ``stop``, waehrend dessen ein Eintrag den beendeten Thread durch
#     einen neuen ersetzt, laesst den neuen nicht verwaist weiterlaufen.
# Plan 2026-10-06-1811, U3 (R1, KTD2) -- die Drop-Meldung kommt aus dem
# Listener, der Aufrufer zaehlt nur:
# 24. Volle Queue, Ziel-Stream (zugleich ``sys.stderr``) blockiert im
#     ``write``: Ein ``log.warning`` in einem Hilfsthread kehrt sofort zurueck.
# 25. Nach dem Freigeben kommt genau eine Meldung ``log_records_dropped``
#     ueber den Listener an (Logger ``swarmq.logging``, WARNING, ``count``).
# 26. JSON-Modus: Die Meldung ist gueltiges JSON mit ``event`` und ``count``.
# 27. ``sys.stderr`` geschlossen: Ein Drop wirft nicht und wird gezaehlt;
#     die Meldung geht auch an einen Ziel-Handler auf dem geschlossenen
#     stderr, ohne den Listener zu beenden (Review B2).
# 28. Ratenbegrenzung: hoechstens eine Meldung pro Sekunde (Zeit gemockt),
#     die Zaehlung stimmt.
# 29. Beim Stoppen meldet der Listener noch nicht gemeldete Drops.
# Plan 2026-10-06-1811, U6 (R6, R7; KTD6, KTD7) -- kein Log-Thread im
# Fork-Quellprozess:
# 30. Als Fork-Quelle markiert, Opt-in aktiv, ein Record auf ``swarmq.*``:
#     kein Listener-Thread, der Record kommt synchron im Root-Handler an.
# 31. Nach einem echten Fork (Hook ``_after_fork_in_child``) startet der
#     naechste Record im Kind genau einen Listener.
# 32. ``LISTENER_THREAD_NAME`` hat hoechstens 15 Zeichen (``comm`` kuerzt).
# Review A5 -- zurueckgehaltene Handler-Fehler gehen nicht verloren:
# 33. [x] Nach Ablauf der Sekunde meldet der naechste fehlerfreie Record den
#         zurueckgehaltenen Fehlerstand.
# 34. [x] Beim Ende des Listeners wird ein zurueckgehaltener Fehlerstand
#         gemeldet, auch innerhalb der Sekunde (Datei-Modus: 34 in
#         ``test_logging_configure.py``).
import atexit
import collections
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
    dropped_log_records,
    setup_logging,
    shutdown_logging,
)
from swarmq.logging_file import RateLimitedReport
from swarmq.logging_forksource import mark_fork_source
from swarmq.logging_format import build_formatter
from swarmq.logging_queue import _StructlogQueueHandler, _SwarmqQueueListener
from tests.unit._fork_child import run_in_forked_child

pytestmark = pytest.mark.timeout(5)

_LISTENER_THREAD = "swarmq-log-lsnr"


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
        record = logging.LogRecord("test", logging.ERROR, __file__, 1, "msg", None, sys.exc_info())

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
    """Nur die Uhr der Ratenbegrenzung: ``time.monotonic`` bleibt echt, die
    Frist des Leerens laeuft also weiter ab (Review B1)."""
    fake = _Clock()
    monkeypatch.setattr(RateLimitedReport, "clock", fake)
    return fake


def test_full_queue_drops_without_blocking(capsys: pytest.CaptureFixture[str]):
    handler = _StructlogQueueHandler(queue.Queue(maxsize=1))
    handler.handle(_record())

    handler.handle(_record())
    handler.handle(_record())

    assert handler.dropped == 2
    # Der Aufrufer schreibt nie selbst (Plan 2026-10-06-1811, U3).
    assert capsys.readouterr().err == ""


def test_shut_down_queue_counts_instead_of_raising(
    capsys: pytest.CaptureFixture[str],
):
    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=10)
    handler = _StructlogQueueHandler(log_queue)
    log_queue.shutdown()

    handler.handle(_record())

    assert handler.dropped == 1
    assert capsys.readouterr().err == ""


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
    """Gibt den blockierten Listener frei, sobald das Leeren die Queue
    schliesst. Bewusst geaendert (Plan 2026-10-06-1811, U4): Das Leeren
    reiht keinen blockierenden Sentinel mehr ein, sondern schliesst per
    ``Queue.shutdown``."""

    def __init__(self, maxsize: int, unblock: threading.Event) -> None:
        super().__init__(maxsize)
        self._unblock = unblock

    def shutdown(self, immediate: bool = False) -> None:
        super().shutdown(immediate)
        self._unblock.set()


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


def test_after_fork_hook_resets_queue_and_drop_state():
    handler = _StructlogQueueHandler(queue.Queue(maxsize=1))
    handler.handle(_record())
    handler.handle(_record())
    assert handler.dropped == 1

    assert dropped_log_records() >= 1

    def child() -> None:
        assert handler.dropped == 0
        assert dropped_log_records() == 0
        assert handler.queue.empty()
        handler.handle(_record())
        handler.handle(_record())
        assert handler.dropped == 1

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
    monkeypatch.setattr("swarmq.logging_queue.threading", types.SimpleNamespace(Thread=_GatedThread))
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
        "swarmq.logging_queue.threading", types.SimpleNamespace(Thread=_StuckThread, Lock=threading.Lock)
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


def test_public_names_remain_importable_from_swarmq_logging() -> None:
    import swarmq.logging
    import swarmq.logging_queue

    for name in (
        "shutdown_logging",
        "dropped_log_records",
        "LISTENER_THREAD_NAME",
    ):
        assert getattr(swarmq.logging, name) is getattr(swarmq.logging_queue, name)
    assert swarmq.logging.flush_logging is swarmq.logging_queue.shutdown_logging


class _DeadThread:
    """Ein beendeter Listener-Thread, etwa nach einer ``BaseException`` aus
    einem Handler (Plan 2026-10-06-1811, U2)."""

    def is_alive(self) -> bool:
        return False

    def join(self, timeout: float | None = None) -> None:
        return None


def test_a_dead_listener_thread_restarts_on_the_next_record() -> None:
    target = _RecordingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), target)
    assert handler.listener is not None
    handler.listener._thread = _DeadThread()  # type: ignore[assignment]
    handler.queue.put_nowait(_record(msg="stranded"))

    handler.handle(_record(msg="after-death"))
    shutdown_logging()

    assert [r.getMessage() for r in target.records] == ["stranded", "after-death"]
    assert handler.dropped == 0


class _RaisingOnPoison(_RecordingHandler):
    """Ziel-Handler, dessen ``emit`` beim Record ``poison`` wirft, ohne
    ``handleError`` -- wie ein Log-Shipper ohne Verbindung."""

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage() == "poison":
            raise ConnectionError("log shipper down")
        super().emit(record)


def test_a_raising_target_handler_does_not_kill_the_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    thread_errors: list[Any] = []
    monkeypatch.setattr(threading, "excepthook", thread_errors.append)
    target = _RaisingOnPoison()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), target)

    handler.handle(_record(msg="poison"))
    handler.handle(_record(msg="after-poison"))
    shutdown_logging()

    messages = [r.getMessage() for r in target.records]
    assert messages == ["log_handler_errors", "after-poison"]
    assert thread_errors == []


def _reports(target: _RecordingHandler) -> list[logging.LogRecord]:
    return [r for r in target.records if r.getMessage() == "log_handler_errors"]


def test_handler_errors_are_counted_and_reported_at_most_once_per_second(
    clock: _Clock,
) -> None:
    target = _RaisingOnPoison()
    listener = _SwarmqQueueListener(queue.Queue(), target)

    listener.handle(_record(msg="poison"))
    clock.now += 0.5
    listener.handle(_record(msg="poison"))
    assert [r.count for r in _reports(target)] == [1]  # type: ignore[attr-defined]

    clock.now += 0.5
    listener.handle(_record(msg="poison"))
    reports = _reports(target)
    assert [r.count for r in reports] == [1, 3]  # type: ignore[attr-defined]
    assert reports[0].name == "swarmq.logging"
    assert reports[0].levelno == logging.WARNING


def test_held_back_handler_errors_are_reported_with_the_next_good_record(
    clock: _Clock,
) -> None:
    target = _RaisingOnPoison()
    listener = _SwarmqQueueListener(queue.Queue(), target)
    listener.handle(_record(msg="poison"))
    clock.now += 0.5
    listener.handle(_record(msg="poison"))

    clock.now += 0.5
    listener.handle(_record(msg="fine"))

    assert [r.count for r in _reports(target)] == [1, 2]  # type: ignore[attr-defined]


def test_stopping_reports_handler_errors_the_rate_limit_held_back(clock: _Clock) -> None:
    target = _RaisingOnPoison()
    handler = _StructlogQueueHandler(queue.Queue(), target)
    handler.handle(_record(msg="poison"))
    handler.handle(_record(msg="poison"))

    shutdown_logging()

    assert [r.count for r in _reports(target)] == [1, 2]  # type: ignore[attr-defined]


def test_a_stop_racing_a_restart_leaves_no_orphaned_listener_thread() -> None:
    target = _RecordingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), target)
    handler.handle(_record(msg="first"))
    assert handler.listener is not None
    thread = handler.listener._thread
    assert thread is not None
    join = thread.join

    def join_then_log(timeout: float | None = None) -> None:
        # Der Thread ist beendet, ``stop`` hat ihn aber noch nicht vergessen:
        # Ein Eintrag aus einem anderen Thread startet hier einen neuen.
        join(timeout)
        handler.handle(_record(msg="in-window"))

    thread.join = join_then_log  # type: ignore[method-assign]

    handler.listener.stop()
    shutdown_logging()

    assert not _listener_threads()
    assert [r.getMessage() for r in target.records] == ["first", "in-window"]


class _BlockingStream(io.StringIO):
    """Ziel-Stream, dessen ``write`` bis ``release`` blockiert -- wie stderr
    auf einer vollen Pipe (Analyse S1)."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def write(self, s: str) -> int:
        self.entered.set()
        self.release.wait()
        return super().write(s)


class _Blocked(types.SimpleNamespace):
    stream: _BlockingStream
    handler: _StructlogQueueHandler
    log: logging.Logger
    recorder: _RecordingHandler


def _blocked_listener_with_full_queue(monkeypatch: pytest.MonkeyPatch, *, json_mode: bool = False) -> _Blocked:
    """Listener haengt im ``write`` auf ``sys.stderr``, die Queue ist voll.
    Hinter dem Stream sammelt ein zweiter Ziel-Handler die Records."""
    stream = _BlockingStream()
    monkeypatch.setattr(sys, "stderr", stream)
    target = logging.StreamHandler(stream)
    target.setFormatter(build_formatter(json_mode, colors=False))
    recorder = _RecordingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=1), target, recorder, json=json_mode)
    log = logging.Logger("swarmq.tests.u3")  # noqa: LOG001 - bewusst isolierter Logger ausserhalb der globalen Hierarchie
    log.addHandler(handler)
    log.warning("first")
    assert stream.entered.wait(timeout=1)
    log.warning("fills-the-queue")
    return _Blocked(stream=stream, handler=handler, log=log, recorder=recorder)


def _drop_reports(target: _RecordingHandler) -> list[logging.LogRecord]:
    return [r for r in target.records if r.getMessage() == "log_records_dropped"]


def test_a_drop_returns_at_once_while_the_target_stream_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blocked = _blocked_listener_with_full_queue(monkeypatch)
    try:
        caller = threading.Thread(target=blocked.log.warning, args=("dropped",))
        caller.start()
        caller.join(timeout=1)

        assert not caller.is_alive()
        assert blocked.handler.dropped == 1
    finally:
        blocked.stream.release.set()


def test_after_release_the_listener_reports_the_drops_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blocked = _blocked_listener_with_full_queue(monkeypatch)
    blocked.log.warning("dropped-1")
    blocked.log.warning("dropped-2")

    blocked.stream.release.set()
    shutdown_logging()

    reports = _drop_reports(blocked.recorder)
    assert [r.count for r in reports] == [2]  # type: ignore[attr-defined]
    assert reports[0].name == "swarmq.logging"
    assert reports[0].levelno == logging.WARNING
    assert blocked.stream.getvalue().count("log_records_dropped") == 1


def test_json_mode_drop_report_is_a_valid_json_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers AE7."""
    blocked = _blocked_listener_with_full_queue(monkeypatch, json_mode=True)
    blocked.log.warning("dropped")

    blocked.stream.release.set()
    shutdown_logging()

    lines = blocked.stream.getvalue().splitlines()
    reports = [json.loads(line) for line in lines if "log_records_dropped" in line]
    assert len(reports) == 1
    assert reports[0]["event"] == "log_records_dropped"
    assert reports[0]["count"] == 1
    assert reports[0]["level"] == "warning"
    assert reports[0]["logger"] == "swarmq.logging"
    assert reports[0]["timestamp"].endswith("Z")


def test_a_drop_with_closed_stderr_neither_raises_nor_goes_uncounted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Die Drop-Meldung geht aus dem Listener auch an einen Ziel-Handler auf
    dem geschlossenen stderr (Review B2): Der Listener ueberlebt das."""
    thread_errors: list[Any] = []
    monkeypatch.setattr(threading, "excepthook", thread_errors.append)
    closed = io.StringIO()
    closed.close()
    monkeypatch.setattr(sys, "stderr", closed)
    blocker = _BlockingHandler()
    recorder = _RecordingHandler()
    handler = _StructlogQueueHandler(
        queue.Queue(maxsize=1),
        blocker,
        recorder,
        logging.StreamHandler(sys.stderr),
    )
    log = logging.Logger("swarmq.tests.u3")  # noqa: LOG001 - bewusst isolierter Logger ausserhalb der globalen Hierarchie
    log.addHandler(handler)
    log.warning("held")
    assert blocker.entered.wait(timeout=1)

    log.warning("fills-the-queue")
    log.warning("dropped")
    blocker.unblock.set()
    shutdown_logging()

    assert handler.dropped == 1
    assert [r.count for r in _drop_reports(recorder)] == [1]  # type: ignore[attr-defined]
    assert _reports(recorder)  # der Handler auf dem geschlossenen stderr warf
    assert thread_errors == []


class _AliveThread(_DeadThread):
    """Ein laufender Listener-Thread, ohne dass ein Thread die Queue leert:
    Der Test ruft ``listener.handle`` selbst auf."""

    def is_alive(self) -> bool:
        return True


def test_drop_reports_come_at_most_once_per_second_with_the_full_count(
    clock: _Clock,
) -> None:
    target = _RecordingHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=1), target)
    listener = handler.listener
    assert listener is not None
    listener._thread = _AliveThread()  # type: ignore[assignment]
    try:
        handler.handle(_record(msg="fills-the-queue"))
        handler.handle(_record(msg="dropped"))
        listener.handle(_record(msg="a"))
        assert [r.count for r in _drop_reports(target)] == [1]  # type: ignore[attr-defined]

        clock.now += 0.5
        handler.handle(_record(msg="dropped"))
        handler.handle(_record(msg="dropped"))
        listener.handle(_record(msg="b"))
        assert [r.count for r in _drop_reports(target)] == [1]  # type: ignore[attr-defined]

        clock.now += 0.5
        listener.handle(_record(msg="c"))
        assert [r.count for r in _drop_reports(target)] == [1, 3]  # type: ignore[attr-defined]
        listener.handle(_record(msg="d"))
        assert [r.count for r in _drop_reports(target)] == [1, 3]  # type: ignore[attr-defined]
    finally:
        listener._thread = None


class _GateHandler(_RecordingHandler):
    """Haelt den Listener bei jedem Record ``hold-*`` fest, bis dessen
    Freigabe gesetzt ist; meldet jeden geschriebenen Record."""

    def __init__(self) -> None:
        super().__init__()
        self.entered: collections.defaultdict[str, threading.Event] = collections.defaultdict(threading.Event)
        self.release: collections.defaultdict[str, threading.Event] = collections.defaultdict(threading.Event)
        self.seen = threading.Condition()

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        if msg.startswith("hold-"):
            self.entered[msg].set()
            self.release[msg].wait()
        with self.seen:
            super().emit(record)
            self.seen.notify_all()

    def wait_for(self, msg: str) -> bool:
        with self.seen:
            return self.seen.wait_for(lambda: msg in [r.getMessage() for r in self.records], timeout=1)


def test_stopping_reports_drops_the_rate_limit_held_back(clock: _Clock) -> None:
    target = _GateHandler()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=1), target)
    log = logging.Logger("swarmq.tests.u3")  # noqa: LOG001 - bewusst isolierter Logger ausserhalb der globalen Hierarchie
    log.addHandler(handler)
    try:
        for round_ in ("1", "2"):
            log.warning("hold-%s", round_)
            assert target.entered[f"hold-{round_}"].wait(timeout=1)
            log.warning("fill-%s", round_)
            log.warning("dropped")
            target.release[f"hold-{round_}"].set()
            assert target.wait_for(f"fill-{round_}")
        assert [r.count for r in _drop_reports(target)] == [1]  # type: ignore[attr-defined]

        shutdown_logging()
    finally:
        for event in target.release.values():
            event.set()

    assert [r.count for r in _drop_reports(target)] == [1, 2]  # type: ignore[attr-defined]


def test_fork_source_emits_opt_in_records_synchronously_without_a_thread() -> None:
    from swarmq.logging import _enable_log_queue

    root_target = _RecordingHandler()
    logging.getLogger().addHandler(root_target)
    mark_fork_source()
    _enable_log_queue("INFO")

    logging.getLogger("swarmq.tests.fork_source").info("in_fork_source")

    assert not _listener_threads()
    assert [r.getMessage() for r in root_target.records] == ["in_fork_source"]
    assert root_target.threads == [threading.current_thread().name]


def test_forked_child_of_the_fork_source_starts_exactly_one_listener() -> None:
    from swarmq.logging import _enable_log_queue

    root_target = _RecordingHandler()
    logging.getLogger().addHandler(root_target)
    mark_fork_source()
    _enable_log_queue("INFO")

    def in_child() -> None:
        logging.getLogger("swarmq.tests.fork_source").info("in_child")
        logging.getLogger("swarmq.tests.fork_source").info("in_child_again")
        assert len(_listener_threads()) == 1
        assert root_target.written.wait(timeout=1)

    assert run_in_forked_child(in_child) == 0


def test_listener_thread_name_survives_the_comm_truncation() -> None:
    from swarmq.logging import LISTENER_THREAD_NAME

    assert len(LISTENER_THREAD_NAME) <= 15
