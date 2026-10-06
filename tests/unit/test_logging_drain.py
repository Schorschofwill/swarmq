"""Begrenztes Leeren der Log-Queues (Plan 2026-10-06-1811, U4; R3, R5,
KTD3, KTD5).

Eigenes Testmodul neben ``test_logging_queue.py``, das am 800-Zeilen-Rahmen
steht; geprueft wird ueber die oeffentliche API (``shutdown_logging``,
``dropped_log_records``, Logaufrufe).
"""

from __future__ import annotations

# Test list:
# 1. [x] Ziel-Handler blockiert an einem nie gesetzten Event, Queue voll:
#        ``shutdown_logging(timeout=0.05)`` kehrt zurueck, ``dropped`` steigt
#        um die Zahl der aufgegebenen Records.
# 2. [x] Bei Fristueberschreitung erscheint auf fd 2 genau eine Zeile
#        ``log_drain_timeout_exceeded`` mit der Zahl der Records.
# 3. [x] JSON-Modus: die Abschlusszeile ist gueltiges JSON (Obvious
#        Implementation in Zyklus 2, ohne eigenes Rot).
# 4. [x] fd 2 ist eine volle Pipe: ``shutdown_logging`` kehrt innerhalb der
#        Frist zurueck, die Zeile wird uebersprungen.
# 5. [x] Zwei Listener, einer haengt: Der andere wird trotzdem geleert.
# 6. [x] Records, die ein anderer Thread waehrend des Leerens loggt, landen
#        in der frischen Queue und gehen nicht verloren.
# 7. [x] Nach Fristueberschreitung mit noch lebendem altem Thread: kein neuer
#        Listener auf dem aufgegebenen Handler, neue Records werden gezaehlt,
#        und ``logging.shutdown`` sieht den Ziel-Handler nicht mehr.
# 8. [x] Endet der aufgegebene Thread doch noch, startet der naechste Record
#        einen neuen Listener auf frischer Queue.
# 9. [x] Ohne ``timeout`` gilt die eingerichtete ``drain_timeout``.
# 9b. [x] Ebenso im Compute-Kind, wenn die App gewinnt
#         (``configure_child_logging``); der haengende App-Handler wird
#         ausgetragen.
# 10. [x] Datei-Modus: begrenzte dictConfig-Queue und blockierender Handler,
#         die Frist greift.
# 10b. [x] Datei-Modus nach dem Aufgeben: kein neuer Listener, neue Records
#          gezaehlt; nach dem Ende des alten Threads startet der naechste
#          Record neu (in Zyklus 10 mit umgesetzt).
# 11. [x] Finalisierung: ``sys.is_finalizing`` wahr -- der Record wird
#         synchron ausgegeben, ohne ``--- Logging error ---``.
# 12. [x] Thread-Start wirft ``PythonFinalizationError``: ebenso synchron
#         (13b: ebenso im Datei-Modus).
# 13. [x] Datei-Modus waehrend der Finalisierung: ebenso synchron.
# 14. [x] Finalisierung bei aufgegebenem Handler: gezaehlt statt ausgegeben.
# 15. [x] Hartes Ende: ``exit_if_log_thread_stuck(code)`` beendet per
#         ``os._exit(code)`` nur, wenn ein aufgegebener Thread noch lebt.
# 16. [x] ``exit_code_for``: der Exit-Code, mit dem der Prozess ohnehin
#         endete -- 0 ohne Ausnahme, der Code eines ``SystemExit``, sonst 1.
# 17. [x] (Review A1) Eine Frist ueber ``threading.TIMEOUT_MAX``: Das Leeren
#         wartet hoechstens ``TIMEOUT_MAX`` statt mit ``OverflowError`` aus
#         ``Thread.join`` abzubrechen.
# 18. [x] (Review A3) Ein Record, den ein anderer Thread waehrend des Leerens
#         loggt, ist geschrieben, wenn ``shutdown_logging`` zurueckkehrt --
#         auch ohne zweites Leeren (Compute-Kind endet per ``os._exit``).
# 19. [x] (Review A3) Loggt ein Thread ununterbrochen weiter, endet das
#         Leeren trotzdem kurz nach der Frist (ohne eigenes Rot: Nach der
#         Frist gibt ``_Draining.settle`` einen noch lebenden Listener auf,
#         der Handler startet dann keinen neuen -- die Schleife endet).
# Review B4 -- Austragen beim Aufgeben und Leeren vor dem harten Ende
# (Charakterisierung bestehenden Verhaltens, ohne eigenes Rot):
# 20. [x] ``abandon`` traegt den Ziel-Handler und jeden anderen Handler auf
#         demselben Stream (etwa ``logging.lastResort``) aus
#         ``logging._handlerList`` aus; Handler auf anderen Streams bleiben.
# 21. [x] ``exit_if_log_thread_stuck`` leert stdout, aber nicht den Stream,
#         an dem der aufgegebene Thread schreibt.
import contextlib
import fcntl
import io
import json
import logging
import os
import queue
import sys
import threading
from collections.abc import Callable, Iterator

import pytest

from swarmq.logging import dropped_log_records, shutdown_logging
from swarmq.logging_queue import _StructlogQueueHandler

pytestmark = pytest.mark.timeout(5)


@pytest.fixture(autouse=True)
def _join_released_threads() -> Iterator[None]:
    """Die Tests geben haengende Listener am Ende frei; ihre Threads enden
    dann asynchron. Hier wird auf sie gewartet, damit kein spaeterer Test
    einen noch laufenden Listener-Thread sieht."""
    before = set(threading.enumerate())
    yield
    for thread in set(threading.enumerate()) - before:
        if thread.daemon:
            thread.join(1)


class _Gate(logging.Handler):
    """Ziel-Handler: blockiert beim ersten Record, bis ``opened`` gesetzt
    ist; sammelt alle Records."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.opened = threading.Event()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.entered.set()
        self.opened.wait()
        self.messages.append(record.getMessage())


def _record(msg: str = "msg", level: int = logging.WARNING) -> logging.LogRecord:
    return logging.LogRecord("swarmq.tests.drain", level, __file__, 1, msg, None, None)


def _returns_within(func: Callable[[], object], seconds: float = 2.0) -> bool:
    """Fuehrt ``func`` in einem Hilfsthread aus: Ein Haenger blockiert den
    Test nicht, sondern ergibt ``False``."""
    done = threading.Event()

    def run() -> None:
        func()
        done.set()

    threading.Thread(target=run, daemon=True).start()
    return done.wait(seconds)


def _stuck_handler(
    maxsize: int = 3, *, json: bool = False,
) -> tuple[_StructlogQueueHandler, _Gate]:
    """Listener haengt im ersten Record, die Queue dahinter ist voll."""
    gate = _Gate()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=maxsize), gate, json=json)
    handler.handle(_record("first"))
    assert gate.entered.wait(1)
    for i in range(maxsize):
        handler.handle(_record(f"queued-{i}"))
    assert handler.queue.full()
    return handler, gate


def test_a_drain_against_a_blocked_target_returns_and_counts_the_rest() -> None:
    handler, gate = _stuck_handler(maxsize=3)
    before = dropped_log_records()
    try:
        assert _returns_within(lambda: shutdown_logging(timeout=0.05))

        assert handler.dropped == 3
        assert dropped_log_records() == before + 3
    finally:
        gate.opened.set()


def test_a_deadline_beyond_timeout_max_still_drains() -> None:
    gate = _Gate()
    gate.opened.set()
    handler = _StructlogQueueHandler(queue.Queue(), gate)
    handler.handle(_record("kept"))

    shutdown_logging(timeout=threading.TIMEOUT_MAX * 10)

    assert gate.messages == ["kept"]


def _notices(err: str) -> list[str]:
    return [line for line in err.splitlines() if "log_drain_timeout_exceeded" in line]


def test_an_exceeded_drain_writes_one_closing_line_to_fd_2(
    capfd: pytest.CaptureFixture[str],
) -> None:
    _handler, gate = _stuck_handler(maxsize=3)
    try:
        assert _returns_within(lambda: shutdown_logging(timeout=0.05))
    finally:
        gate.opened.set()

    [line] = _notices(capfd.readouterr().err)
    assert "3" in line


def test_in_json_mode_the_closing_line_is_valid_json(
    capfd: pytest.CaptureFixture[str],
) -> None:
    _handler, gate = _stuck_handler(maxsize=2, json=True)
    try:
        assert _returns_within(lambda: shutdown_logging(timeout=0.05))
    finally:
        gate.opened.set()

    [line] = _notices(capfd.readouterr().err)
    notice = json.loads(line)
    assert (notice["event"], notice["count"], notice["level"]) == (
        "log_drain_timeout_exceeded", 2, "warning",
    )


@contextlib.contextmanager
def _fd2_on_a_full_pipe() -> Iterator[int]:
    """fd 2 zeigt auf eine volle, nie geleerte Pipe (Analyse S2.2); liefert
    das Leseende. Danach ist fd 2 wiederhergestellt."""
    read_end, write_end = os.pipe()
    flags = fcntl.fcntl(write_end, fcntl.F_GETFL)
    fcntl.fcntl(write_end, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    with contextlib.suppress(BlockingIOError):
        while True:
            os.write(write_end, b"x" * 65536)
    with contextlib.suppress(BlockingIOError):
        while True:
            os.write(write_end, b"x")
    fcntl.fcntl(write_end, fcntl.F_SETFL, flags)
    saved = os.dup(2)
    os.dup2(write_end, 2)
    try:
        yield read_end
    finally:
        os.dup2(saved, 2)
        for fd in (saved, write_end, read_end):
            os.close(fd)


def test_the_closing_line_is_skipped_while_fd_2_is_a_full_pipe() -> None:
    _handler, gate = _stuck_handler(maxsize=2)
    try:
        with _fd2_on_a_full_pipe() as read_end:
            assert _returns_within(lambda: shutdown_logging(timeout=0.05))
            pending = os.read(read_end, 1 << 20)
    finally:
        gate.opened.set()

    assert b"log_drain_timeout_exceeded" not in pending


class _Collector(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def test_a_hanging_listener_does_not_keep_the_others_from_draining() -> None:
    _stuck, gate = _stuck_handler(maxsize=2)
    collector = _Collector()
    healthy = _StructlogQueueHandler(queue.Queue(maxsize=100), collector)
    for i in range(20):
        healthy.handle(_record(f"healthy-{i}"))
    try:
        assert _returns_within(lambda: shutdown_logging(timeout=0.2))
    finally:
        gate.opened.set()

    assert collector.messages == [f"healthy-{i}" for i in range(20)]
    assert healthy.dropped == 0


class _SignallingQueue(queue.Queue[logging.LogRecord]):
    """Meldet, wann das Leeren sie schliesst."""

    def __init__(self, maxsize: int) -> None:
        super().__init__(maxsize)
        self.closing = threading.Event()

    def shutdown(self, immediate: bool = False) -> None:
        super().shutdown(immediate)
        self.closing.set()


def test_records_logged_while_draining_land_in_a_fresh_queue() -> None:
    gate = _Gate()
    old_queue = _SignallingQueue(maxsize=10)
    handler = _StructlogQueueHandler(old_queue, gate)
    handler.handle(_record("before"))
    assert gate.entered.wait(1)
    drained = threading.Event()

    def drain() -> None:
        shutdown_logging(timeout=2)
        drained.set()

    threading.Thread(target=drain, daemon=True).start()
    try:
        assert old_queue.closing.wait(1)
        handler.handle(_record("during"))
    finally:
        gate.opened.set()
    assert drained.wait(2)
    assert _returns_within(lambda: shutdown_logging(timeout=2))

    assert gate.messages == ["before", "during"]
    assert handler.dropped == 0


class _SlowGate(_Gate):
    """Wie ``_Gate``; ``late`` braucht danach noch 50 ms."""

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage() == "late":
            threading.Event().wait(0.05)
        super().emit(record)


def test_a_drain_returns_only_after_records_logged_while_draining_are_written() -> None:
    gate = _SlowGate()
    old_queue = _SignallingQueue(maxsize=10)
    handler = _StructlogQueueHandler(old_queue, gate)
    handler.handle(_record("before"))
    assert gate.entered.wait(1)
    drained = threading.Event()

    def drain() -> None:
        shutdown_logging(timeout=2)
        drained.set()

    threading.Thread(target=drain, daemon=True).start()
    try:
        assert old_queue.closing.wait(1)
        handler.handle(_record("late"))
    finally:
        gate.opened.set()
    assert drained.wait(2)

    assert gate.messages == ["before", "late"]
    assert handler.dropped == 0


def test_a_drain_ends_soon_after_its_deadline_while_a_thread_keeps_logging() -> None:
    gate = _Gate()
    gate.opened.set()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=100), gate)
    stop = threading.Event()

    def keep_logging() -> None:
        while not stop.is_set():
            handler.handle(_record("chatter"))

    logger = threading.Thread(target=keep_logging, daemon=True)
    logger.start()
    try:
        assert gate.entered.wait(1)
        assert _returns_within(lambda: shutdown_logging(timeout=0.05), seconds=1)
    finally:
        stop.set()
        logger.join(1)
    assert _returns_within(lambda: shutdown_logging(timeout=1))


def _listener_threads() -> list[threading.Thread]:
    from swarmq.logging import LISTENER_THREAD_NAME

    return [t for t in threading.enumerate() if t.name == LISTENER_THREAD_NAME]


def test_an_abandoned_handler_counts_new_records_instead_of_starting_a_listener() -> None:
    handler, gate = _stuck_handler(maxsize=2)
    try:
        assert _returns_within(lambda: shutdown_logging(timeout=0.05))
        threads = set(_listener_threads())
        dropped = handler.dropped

        handler.handle(_record("after-abandon"))

        assert not set(_listener_threads()) - threads
        assert handler.dropped == dropped + 1
        # ``logging.shutdown`` (atexit, Kind-Ende) geht ueber diese Liste und
        # haengte sonst am Lock des haengenden Ziel-Handlers.
        assert gate not in [ref() for ref in logging._handlerList]  # type: ignore[attr-defined]
    finally:
        gate.opened.set()


def test_once_the_abandoned_thread_ends_the_next_record_starts_a_fresh_listener() -> None:
    others = set(_listener_threads())
    handler, gate = _stuck_handler(maxsize=2)
    [old] = set(_listener_threads()) - others
    try:
        assert _returns_within(lambda: shutdown_logging(timeout=0.05))
    finally:
        gate.opened.set()
    old.join(1)
    assert not old.is_alive()

    handler.handle(_record("next"))
    assert _returns_within(lambda: shutdown_logging(timeout=2))

    # Der Rest der alten Queue ist verworfen und gezaehlt, nicht geschrieben.
    assert gate.messages == ["first", "next"]
    assert handler.dropped == 2


class _BlockingStream(io.StringIO):
    """stderr auf einer vollen Pipe: ``write`` blockiert bis ``opened``."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.opened = threading.Event()

    def write(self, s: str) -> int:
        self.entered.set()
        self.opened.wait()
        return super().write(s)


def test_without_a_timeout_the_configured_drain_timeout_applies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swarmq.logging import configure_logging
    from swarmq.logging_settings import LoggingSettings

    stream = _BlockingStream()
    monkeypatch.setattr(sys, "stderr", stream)
    configure_logging(LoggingSettings(queue_size=4, drain_timeout=0.05))
    log = logging.getLogger("swarmq.tests.drain")
    try:
        log.warning("first")
        assert stream.entered.wait(1)
        log.warning("second")

        assert _returns_within(shutdown_logging)
    finally:
        stream.opened.set()


def test_in_a_compute_child_where_the_app_wins_the_drain_is_bounded_too() -> None:
    from swarmq.logging import configure_child_logging
    from swarmq.logging_settings import LoggingSettings

    gate = _Gate()
    logging.getLogger().addHandler(gate)  # Root-Handler der App aus dem Vorladen
    assert configure_child_logging(LoggingSettings(drain_timeout=0.05))  # App gewinnt
    log = logging.getLogger("swarmq.tests.drain")
    try:
        log.warning("first")
        assert gate.entered.wait(1)
        log.warning("second")

        assert _returns_within(shutdown_logging)
        assert gate not in [ref() for ref in logging._handlerList]  # type: ignore[attr-defined]
    finally:
        gate.opened.set()


class _GateState:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.opened = threading.Event()
        self.written = threading.Condition()
        self.messages: list[str] = []
        self.threads: list[str] = []


class FileModeGate(logging.Handler):
    """Ziel-Handler einer dictConfig (Datei-Modus): blockiert jeden Record,
    bis ``opened`` gesetzt ist. ``dictConfig`` legt das Exemplar an; es
    bindet den Zustand des laufenden Tests, damit ein spaeter frei kommender
    Thread eines frueheren Tests nicht hineinschreibt."""

    current = _GateState()

    def __init__(self) -> None:
        super().__init__()
        self.state = FileModeGate.current

    def emit(self, record: logging.LogRecord) -> None:
        state = self.state
        state.entered.set()
        state.opened.wait()
        with state.written:
            state.messages.append(record.getMessage())
            state.threads.append(threading.current_thread().name)
            state.written.notify_all()


@pytest.fixture
def file_mode_gate() -> Iterator[_GateState]:
    FileModeGate.current = state = _GateState()
    try:
        yield state
    finally:
        state.opened.set()


def _configure_file_mode(drain_timeout: float = 5.0) -> logging.Logger:
    from swarmq.logging import configure_logging
    from swarmq.logging_settings import LoggingSettings

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "gate": {"class": f"{__name__}.FileModeGate"},
            "q": {
                "class": "logging.handlers.QueueHandler",
                "handlers": ["gate"],
                "queue": {"()": "queue.Queue", "maxsize": 3},
            },
        },
        "root": {"handlers": ["q"], "level": "INFO"},
    }
    configure_logging(LoggingSettings(
        log_config="logging.json", log_config_json=json.dumps(config),
        drain_timeout=drain_timeout,
    ))
    return logging.getLogger("swarmq.tests.drain")


def test_file_mode_listeners_are_drained_within_the_same_deadline(
    file_mode_gate: _GateState,
) -> None:
    log = _configure_file_mode()
    log.warning("first")
    assert file_mode_gate.entered.wait(1)
    for i in range(3):
        log.warning("queued-%d", i)
    before = dropped_log_records()

    assert _returns_within(lambda: shutdown_logging(timeout=0.05))
    assert dropped_log_records() == before + 3


def test_an_abandoned_file_mode_listener_counts_new_records_until_its_thread_ends(
    file_mode_gate: _GateState,
) -> None:
    before_start = set(threading.enumerate())
    log = _configure_file_mode()
    log.warning("first")
    assert file_mode_gate.entered.wait(1)
    [old] = set(threading.enumerate()) - before_start
    for i in range(3):
        log.warning("queued-%d", i)
    assert _returns_within(lambda: shutdown_logging(timeout=0.05))
    before = dropped_log_records()

    log.warning("while-abandoned")

    assert dropped_log_records() == before + 1
    assert set(threading.enumerate()) - before_start == {old}

    # Frei gekommen arbeitet der alte Thread seine Queue ab und endet.
    file_mode_gate.opened.set()
    old.join(1)
    assert not old.is_alive()
    log.warning("after")
    assert _returns_within(lambda: shutdown_logging(timeout=2))
    written = [m for m in file_mode_gate.messages if m != "log_records_dropped"]
    assert written == ["first", "queued-0", "queued-1", "queued-2", "after"]


class _ThreadRecorder(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.seen: list[tuple[str, str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.seen.append((record.getMessage(), threading.current_thread().name))


def test_during_finalization_a_record_is_written_synchronously(
    monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str],
) -> None:
    target = _ThreadRecorder()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), target)
    others = set(_listener_threads())

    with monkeypatch.context() as patch:
        patch.setattr(sys, "is_finalizing", lambda: True)
        handler.handle(_record("at-finalization"))

    assert target.seen == [("at-finalization", threading.current_thread().name)]
    assert not set(_listener_threads()) - others
    assert "Logging error" not in capfd.readouterr().err


def test_a_thread_start_refused_at_shutdown_writes_the_record_synchronously(
    monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str],
) -> None:
    target = _ThreadRecorder()
    handler = _StructlogQueueHandler(queue.Queue(maxsize=10), target)

    def refuse(self: threading.Thread) -> None:
        raise PythonFinalizationError("can't create new thread at interpreter shutdown")

    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", refuse)
        handler.handle(_record("refused-start"))

    assert target.seen == [("refused-start", threading.current_thread().name)]
    assert "Logging error" not in capfd.readouterr().err


def test_a_freed_file_mode_thread_ends_by_itself_without_a_sentinel(
    file_mode_gate: _GateState,
) -> None:
    before_start = set(threading.enumerate())
    log = _configure_file_mode()
    log.warning("first")
    assert file_mode_gate.entered.wait(1)
    [old] = set(threading.enumerate()) - before_start
    for i in range(3):
        log.warning("queued-%d", i)
    assert _returns_within(lambda: shutdown_logging(timeout=0.05))

    file_mode_gate.opened.set()
    old.join(1)

    assert not old.is_alive()


def test_file_mode_writes_synchronously_during_finalization(
    file_mode_gate: _GateState, monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    file_mode_gate.opened.set()
    log = _configure_file_mode()
    log.warning("before")
    assert _returns_within(lambda: shutdown_logging(timeout=2))

    with monkeypatch.context() as patch:
        patch.setattr(sys, "is_finalizing", lambda: True)
        log.warning("at-finalization")

    assert file_mode_gate.messages[-1] == "at-finalization"
    assert file_mode_gate.threads[-1] == threading.current_thread().name
    assert "Logging error" not in capfd.readouterr().err


def test_file_mode_writes_synchronously_when_the_thread_start_is_refused(
    file_mode_gate: _GateState, monkeypatch: pytest.MonkeyPatch,
) -> None:
    file_mode_gate.opened.set()
    log = _configure_file_mode()
    log.warning("before")
    assert _returns_within(lambda: shutdown_logging(timeout=2))

    def refuse(self: threading.Thread) -> None:
        raise PythonFinalizationError("can't create new thread at interpreter shutdown")

    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", refuse)
        log.warning("refused-start")

    assert file_mode_gate.messages[-1] == "refused-start"
    assert file_mode_gate.threads[-1] == threading.current_thread().name


def test_during_finalization_an_abandoned_handler_counts_instead_of_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler, gate = _stuck_handler(maxsize=2)
    try:
        assert _returns_within(lambda: shutdown_logging(timeout=0.05))
        dropped = handler.dropped

        with monkeypatch.context() as patch:
            patch.setattr(sys, "is_finalizing", lambda: True)
            assert _returns_within(lambda: handler.handle(_record("at-finalization")))

        assert handler.dropped == dropped + 1
    finally:
        gate.opened.set()


class _Exited(Exception):
    """Ersatz fuer ``os._exit`` im Test."""


def _fake_exit(code: int) -> None:
    raise _Exited(code)


def test_a_process_exits_hard_with_its_code_while_an_abandoned_thread_hangs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swarmq.logging_drain import exit_if_log_thread_stuck

    monkeypatch.setattr("swarmq.logging_drain._hard_exit", _fake_exit)
    for earlier in _listener_threads():  # frei gegebene aus frueheren Tests
        earlier.join(1)
    exit_if_log_thread_stuck(3)  # nichts haengt: kehrt zurueck

    _handler, gate = _stuck_handler(maxsize=2)
    try:
        assert _returns_within(lambda: shutdown_logging(timeout=0.05))
        with pytest.raises(_Exited) as exited:
            exit_if_log_thread_stuck(3)
        assert exited.value.args == (3,)
    finally:
        gate.opened.set()


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (None, 0),
        (SystemExit(78), 78),
        (SystemExit(None), 0),
        (SystemExit("message"), 1),
        (ValueError("boom"), 1),
    ],
)
def test_the_hard_exit_keeps_the_exit_code_the_process_would_have_had(
    exc: BaseException | None, code: int,
) -> None:
    from swarmq.logging_drain import exit_code_for

    assert exit_code_for(exc) == code


class _FlushRecorder(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.flushed = 0

    def flush(self) -> None:
        self.flushed += 1


@contextlib.contextmanager
def _hanging_thread() -> Iterator[threading.Thread]:
    release = threading.Event()
    thread = threading.Thread(target=release.wait, daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        release.set()
        thread.join(1)


def test_abandoning_forgets_every_handler_on_the_stuck_stream() -> None:
    from swarmq.logging_drain import abandon

    stuck, other = _FlushRecorder(), _FlushRecorder()
    target = logging.StreamHandler(stuck)
    same_stream = logging.StreamHandler(stuck)  # wie ``lastResort`` auf stderr
    elsewhere = logging.StreamHandler(other)

    with _hanging_thread() as thread:
        abandon(thread, [target])

    registered = [ref() for ref in logging._handlerList]  # type: ignore[attr-defined]
    assert target not in registered
    assert same_stream not in registered
    assert elsewhere in registered


def test_the_hard_exit_flushes_stdout_but_not_the_stuck_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swarmq.logging_drain import abandon, exit_if_log_thread_stuck

    stdout, stderr = _FlushRecorder(), _FlushRecorder()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    monkeypatch.setattr("swarmq.logging_drain._hard_exit", _fake_exit)

    with _hanging_thread() as thread:
        abandon(thread, [logging.StreamHandler(stderr)])
        with pytest.raises(_Exited):
            exit_if_log_thread_stuck(3)

    assert (stdout.flushed, stderr.flushed) == (1, 0)
