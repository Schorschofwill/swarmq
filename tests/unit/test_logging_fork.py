"""Fork-Sicherheit der Log-Listener (Plan 2026-10-06-1811, U7; R8, KTD10).

Ein ``os.fork()`` aus Task-Code: Im Kind schreiben die Queue-Handler der
dictConfig (Datei-Modus) weiter, ohne die geerbten Records des
Elternprozesses ein zweites Mal auszugeben, und kein Ziel-Handler steckt in
einem halb geschriebenen Datensatz. Geprueft wird ueber echte Forks
(``tests.unit._fork_child``): So laufen die registrierten Fork-Hooks wie im
Betrieb, statt dass ein Test den privaten Hook aufruft.
"""

from __future__ import annotations

# Test list:
# 1. [x] Datei-Modus, Listener im Elternprozess blockiert, ein Record wartet
#        in der Queue: Das Kind gibt den geerbten Record nicht aus, sein
#        eigener kommt an.
# 2. [x] Multiprocessing-Queue: Das Kind behaelt dieselbe Queue (kein Ersatz).
# 3. [x] Eigene Queue-Klasse (nicht rekonstruierbares ``"()"``): bleibt.
# 4. [x] ``queue.SimpleQueue``: wird wie ``queue.Queue`` ersetzt.
# 5. [x] ``before``-Hook: Der fork-ende Thread erwirbt das Lock des
#        Ziel-Handlers eines laufenden Datei-Modus-Listeners und gibt es im
#        Elternprozess wieder frei.
# 6. [x] Ebenso fuer den eingebauten Listener.
# 7. [x] Opt-in-Listener: die Root-Handler, an die er weitergibt.
# 8. [x] Ein Lock, das ein anderer Thread haelt: Der Fork laeuft nach dem
#        Budget trotzdem, das Kind lebt.
# 9. [x] Das Budget gilt ueber alle Handler zusammen, nicht pro Handler.
# 10. [x] Ein aufgegebener Ziel-Handler (KTD3) wird gar nicht erst versucht.
# 11. [x] Ein Thread haelt ``logging._lock`` und will danach ein Ziel-Lock
#         (Reihenfolge von ``dictConfig``): kein Deadlock beim Fork.
# 12. [x] Ein beim Fork gehaltenes Wiederstart-Lock der Datei-Modus-Listener
#         haelt das Kind nicht auf.
# 13. [x] Das Kind kann selbst wieder forken, ohne dass ein Hook wirft
#         (Fund waehrend Zyklus 11).
# 14. [x] Drops des Elternprozesses meldet das Kind nicht noch einmal (Fund
#         im Prozesstest mit begrenzter Queue).
# Schluss-Review (P1, Regression gegenueber main):
# 15. [x] Multiprocessing-Queue: Das Kind startet keinen Listener; sein
#         Record kommt ueber den Listener des Elternprozesses an (auch unter
#         rohem ``os.fork``, also mit frischem Feeder-Thread im Kind).
# 16. [x] Eigene Queue-Klasse der App: Der Listener startet im Kind weiter
#         lazy (Gegenstueck zu 15; Schutztest, war schon gruen).
# 17. [x] Zwei Threads forken gleichzeitig: Das ``after_in_parent`` des
#         einen gibt nicht die Locks frei, die der ``before``-Hook des
#         anderen haelt.
# Restpunkte (Release 0.9.0):
# 18. [x] Ein Ziel-Lock, das beim letzten Fork nicht zu bekommen war und noch
#         immer belegt ist (Listener haengt an einer vollen Pipe, ohne schon
#         aufgegeben zu sein): Der naechste Fork wartet nicht noch einmal das
#         ganze Budget.
# 19. [x] Multiprocessing-Queue am Prozessende: Die atexit-Funktion von
#         ``multiprocessing`` (nach swarmq registriert, laeuft also zuerst)
#         schliesst die Pipe der Queue. Davor sind alle Records geschrieben,
#         kein Listener stirbt mit Traceback (``EOFError``/``OSError``).
# 20. [x] Ein Record nach dem Schliessen der Pipe (etwa aus einer spaeteren
#         atexit-Funktion der App): kein Traceback, kein Neustart-Sturm, der
#         Record zaehlt als verworfen.
# Flake unter CPU-Last (Fund nach 0.9.0):
# 21. [x] Wie 20, aber der vom Record neu gestartete Listener liest erst nach
#         dem Sentinel von ``shutdown_logging``: Der Sentinel bleibt in der
#         geschlossenen Queue liegen und zaehlt nicht als verworfener Record.
import json
import logging
import logging.handlers
import pathlib
import queue
import sys
import threading
import time
import weakref
from collections.abc import Iterator
from typing import Any, override
from unittest.mock import MagicMock

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.logging import configure_logging, shutdown_logging
from swarmq.logging_settings import LoggingSettings
from tests.unit._fork_child import run_in_forked_child

pytestmark = pytest.mark.timeout(5)

RECORDS: list[logging.LogRecord] = []
"""Ziel des ``RecordingHandler`` aus den dictConfig-Dateien der Tests."""

RECORDED = threading.Condition()
"""Meldet jeden neuen Eintrag in ``RECORDS``."""

ENTERED = threading.Event()
RELEASE = threading.Event()


class RecordingHandler(logging.Handler):
    """Per dictConfig (``"()"``) referenzierbarer Sammel-Handler."""

    @override
    def emit(self, record: logging.LogRecord) -> None:
        with RECORDED:
            RECORDS.append(record)
            RECORDED.notify_all()


class BlockingHandler(RecordingHandler):
    """Haelt den Listener beim ersten Record fest, bis ``RELEASE`` gesetzt
    ist -- im Elternprozess. Im Kind ist ``ENTERED`` schon gesetzt."""

    @override
    def emit(self, record: logging.LogRecord) -> None:
        if not ENTERED.is_set():
            ENTERED.set()
            RELEASE.wait()
        super().emit(record)


class SpyLock:
    """``RLock``, das jeden Aufruf mit dem Thread des Aufrufers festhaelt."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.calls: list[tuple[str, int]] = []

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        self.calls.append(("acquire", threading.get_ident()))
        return self._lock.acquire(blocking, timeout)

    def release(self) -> None:
        self.calls.append(("release", threading.get_ident()))
        self._lock.release()

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()

    def _at_fork_reinit(self) -> None:
        self._lock._at_fork_reinit()  # type: ignore[attr-defined]

    def by_this_thread(self) -> list[str]:
        me = threading.get_ident()
        return [op for op, ident in self.calls if ident == me]


def _spy_on(handler: logging.Handler) -> SpyLock:
    spy = SpyLock()
    handler.lock = spy  # type: ignore[assignment]
    return spy


@pytest.fixture(autouse=True)
def isolated_logging(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """``dictConfig`` schliesst alle bekannten Handler: Die Registry wird fuer
    den Test geleert. Root und ``shutdown_logging`` uebernimmt das
    Autouse-Fixture der Unit-Tests; ein blockierter Listener wird am Ende
    freigegeben."""
    monkeypatch.setattr(logging, "_handlerList", [])
    monkeypatch.setattr(logging, "_handlers", weakref.WeakValueDictionary())
    RECORDS.clear()
    ENTERED.clear()
    RELEASE.clear()
    yield
    RELEASE.set()


def _configure_file_mode(
    tmp_path: pathlib.Path,
    handler: str = "RecordingHandler",
    **queue_handler: object,
) -> logging.handlers.QueueHandler:
    config = {
        "version": 1,
        "handlers": {
            "rec": {"()": f"{__name__}.{handler}"},
            "q": {"class": "logging.handlers.QueueHandler", "handlers": ["rec"], **queue_handler},
        },
        "root": {"level": "INFO", "handlers": ["q"]},
    }
    path = tmp_path / "log.json"
    path.write_text(json.dumps(config))
    configure_logging(LoggingSettings(log_config=str(path)))
    (handler_,) = logging.getLogger().handlers
    assert isinstance(handler_, logging.handlers.QueueHandler)
    return handler_


def _messages() -> list[str]:
    return [r.getMessage() for r in RECORDS]


def _wait_for_message(message: str, timeout: float = 2.0) -> bool:
    with RECORDED:
        return RECORDED.wait_for(lambda: message in _messages(), timeout)


@pytest.mark.parametrize(
    "queue_config",
    [{}, {"queue": {"()": "queue.SimpleQueue"}}],
    ids=["queue.Queue", "queue.SimpleQueue"],
)
def test_the_child_does_not_replay_records_inherited_in_the_file_mode_queue(
    tmp_path: pathlib.Path,
    queue_config: dict[str, object],
) -> None:
    _configure_file_mode(tmp_path, "BlockingHandler", **queue_config)
    log = logging.getLogger("app")
    log.warning("evt_parent_busy")
    assert ENTERED.wait(2)
    log.warning("evt_inherited")

    def child() -> None:
        log.warning("evt_child")
        shutdown_logging()
        assert _messages() == ["evt_child"]

    assert run_in_forked_child(child) == 0


def test_a_multiprocessing_queue_is_kept_in_the_child(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _configure_file_mode(tmp_path, queue={"()": "multiprocessing.Queue"})
    inherited = handler.queue
    # Ein werfender Fork-Hook ("Exception ignored in atfork callback") bricht
    # den Reset der uebrigen Handler ab.
    hook_errors: list[object] = []
    monkeypatch.setattr(sys, "unraisablehook", hook_errors.append)

    def child() -> None:
        assert hook_errors == []
        assert handler.queue is inherited

    assert run_in_forked_child(child) == 0


def test_the_child_only_produces_into_a_multiprocessing_queue(tmp_path: pathlib.Path) -> None:
    handler = _configure_file_mode(tmp_path, queue={"()": "multiprocessing.Queue"})
    log = logging.getLogger("app")
    log.warning("evt_parent")
    assert _wait_for_message("evt_parent")

    def child() -> None:
        log.warning("evt_child")
        # Ein Kind, das per ``os._exit`` endet, leert den Feeder-Thread der
        # Queue selbst (multiprocessing-Doku: "joining processes that use
        # queues").
        handler.queue.close()  # type: ignore[attr-defined]
        handler.queue.join_thread()  # type: ignore[attr-defined]
        # Der Feeder ist beendet: Jeder weitere Thread waere ein Listener.
        assert [t.name for t in threading.enumerate()] == [threading.current_thread().name]

    assert run_in_forked_child(child) == 0
    assert _wait_for_message("evt_child")


class AppQueue(queue.Queue[object]):
    """Eigene Queue-Klasse der App: Ihren Konstruktor kennt swarmq nicht."""


def test_an_app_queue_class_is_kept_in_the_child(tmp_path: pathlib.Path) -> None:
    handler = _configure_file_mode(tmp_path, queue={"()": f"{__name__}.AppQueue", "maxsize": 3})
    inherited = handler.queue

    def child() -> None:
        assert handler.queue is inherited

    assert run_in_forked_child(child) == 0


def test_an_app_queue_class_still_gets_a_listener_in_the_child(tmp_path: pathlib.Path) -> None:
    _configure_file_mode(tmp_path, "BlockingHandler", queue={"()": f"{__name__}.AppQueue"})
    log = logging.getLogger("app")
    log.warning("evt_parent_busy")
    assert ENTERED.wait(2)

    def child() -> None:
        # Die Queue-Kopie gehoert dem Kind: Sein eigener Listener schreibt.
        log.warning("evt_child")
        assert _wait_for_message("evt_child")

    assert run_in_forked_child(child) == 0


def test_fork_holds_the_target_lock_of_a_running_file_mode_listener(
    tmp_path: pathlib.Path,
) -> None:
    handler = _configure_file_mode(tmp_path)
    (target,) = handler.listener.handlers  # type: ignore[union-attr]
    spy = _spy_on(target)

    assert run_in_forked_child(lambda: None) == 0

    assert spy.by_this_thread() == ["acquire", "release"]


def test_a_target_lock_held_by_a_stuck_emit_does_not_stop_the_fork(
    tmp_path: pathlib.Path,
) -> None:
    _configure_file_mode(tmp_path, "BlockingHandler")
    logging.getLogger("app").warning("evt_stuck")
    assert ENTERED.wait(2)

    started = time.monotonic()
    assert run_in_forked_child(lambda: None) == 0

    assert time.monotonic() - started < 1.0


def _hold_in_other_thread(locks: list[threading.RLock]) -> threading.Thread:
    """Haelt ``locks`` in einem eigenen Thread, bis ``RELEASE`` gesetzt ist."""
    holding = threading.Event()

    def hold() -> None:
        for lock in locks:
            lock.acquire()
        holding.set()
        RELEASE.wait()
        for lock in locks:
            lock.release()

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert holding.wait(2)
    return holder


def test_the_lock_budget_covers_all_targets_together(tmp_path: pathlib.Path) -> None:
    names = [f"rec{i}" for i in range(10)]
    config = {
        "version": 1,
        "handlers": {
            **{name: {"()": f"{__name__}.RecordingHandler"} for name in names},
            "q": {"class": "logging.handlers.QueueHandler", "handlers": names},
        },
        "root": {"level": "INFO", "handlers": ["q"]},
    }
    path = tmp_path / "log.json"
    path.write_text(json.dumps(config))
    configure_logging(LoggingSettings(log_config=str(path)))
    (handler,) = logging.getLogger().handlers
    targets = handler.listener.handlers  # type: ignore[attr-defined]
    holder = _hold_in_other_thread([t.lock for t in targets])

    started = time.monotonic()
    assert run_in_forked_child(lambda: None) == 0
    elapsed = time.monotonic() - started

    RELEASE.set()
    holder.join(2)
    # Pro Handler waeren es mindestens 10 x 50 ms.
    assert elapsed < 0.3


def test_fork_holds_the_target_lock_of_the_running_builtin_listener() -> None:
    configure_logging(LoggingSettings())
    (handler,) = logging.getLogger().handlers
    (target,) = handler.listener.handlers  # type: ignore[attr-defined]
    logging.getLogger("app").warning("evt_starts_the_listener")
    spy = _spy_on(target)

    assert run_in_forked_child(lambda: None) == 0

    assert spy.by_this_thread() == ["acquire", "release"]


def test_fork_holds_the_root_handler_locks_of_the_running_optin_listener() -> None:
    root_handlers = [RecordingHandler(), RecordingHandler()]
    logging.getLogger().handlers[:] = root_handlers
    broker = MagicMock(spec=RabbitMQBroker, _url="amqp://guest:guest@localhost/")
    backend = MagicMock(spec=ValkeyBackend, _url="valkey://localhost:6379/0")
    SwarmQ(broker, backend, log_queue=True)
    logging.getLogger("swarmq.tests.fork").warning("evt_starts_the_listener")
    spies = [_spy_on(h) for h in root_handlers]

    assert run_in_forked_child(lambda: None) == 0

    assert [spy.by_this_thread() for spy in spies] == [["acquire", "release"]] * 2


def test_an_abandoned_target_is_not_even_tried(tmp_path: pathlib.Path) -> None:
    handler = _configure_file_mode(tmp_path, "BlockingHandler")
    logging.getLogger("app").warning("evt_stuck")
    assert ENTERED.wait(2)
    shutdown_logging(timeout=0.05)  # gibt den haengenden Thread auf (KTD3)
    (target,) = handler.listener.handlers  # type: ignore[union-attr]
    spy = _spy_on(target)

    assert run_in_forked_child(lambda: None) == 0

    assert spy.by_this_thread() == []


class SignallingLock(SpyLock):
    """Meldet, sobald der Thread ``owner`` das Lock erwirbt."""

    def __init__(self, owner: int) -> None:
        super().__init__()
        self.owner = owner
        self.taken_by_owner = threading.Event()

    @override
    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        acquired = super().acquire(blocking, timeout)
        if acquired and threading.get_ident() == self.owner:
            self.taken_by_owner.set()
        return acquired


def test_fork_does_not_deadlock_with_a_thread_holding_the_logging_lock(
    tmp_path: pathlib.Path,
) -> None:
    handler = _configure_file_mode(tmp_path)
    (target,) = handler.listener.handlers  # type: ignore[union-attr]
    lock = SignallingLock(owner=threading.get_ident())
    target.lock = lock  # type: ignore[assignment]
    holds_module_lock = threading.Event()

    def reconfigure() -> None:
        # Wie ``dictConfig``: erst ``logging._lock``, dann die Handler-Locks
        # (``logging.shutdown`` der alten Handler).
        with logging._lock:  # type: ignore[attr-defined]
            holds_module_lock.set()
            lock.taken_by_owner.wait(0.15)
            with lock:
                pass

    other = threading.Thread(target=reconfigure, daemon=True)
    other.start()
    assert holds_module_lock.wait(2)

    started = time.monotonic()
    assert run_in_forked_child(lambda: None) == 0
    elapsed = time.monotonic() - started

    other.join(2)
    assert elapsed < 2.0


def test_the_child_can_fork_again_without_hook_errors(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_file_mode(tmp_path)
    hook_errors: list[object] = []
    monkeypatch.setattr(sys, "unraisablehook", hook_errors.append)

    def child() -> None:
        logging.getLogger("app").warning("evt_starts_the_child_listener")
        assert run_in_forked_child(lambda: None) == 0
        assert hook_errors == []

    assert run_in_forked_child(child) == 0


def test_a_restart_lock_held_at_fork_does_not_block_the_child(tmp_path: pathlib.Path) -> None:
    import swarmq.logging_file

    _configure_file_mode(tmp_path)
    # Ein anderer Thread startet gerade einen Datei-Modus-Listener neu.
    holder = _hold_in_other_thread([swarmq.logging_file._CONFIG_LISTENER_LOCK])  # type: ignore[list-item]

    def child() -> None:
        logging.getLogger("app").warning("evt_child")
        shutdown_logging()
        assert _messages() == ["evt_child"]

    try:
        assert run_in_forked_child(child) == 0
    finally:
        RELEASE.set()
        holder.join(2)


def test_the_child_does_not_report_the_drops_of_the_parent(tmp_path: pathlib.Path) -> None:
    _configure_file_mode(tmp_path, "BlockingHandler", queue={"()": "queue.Queue", "maxsize": 1})
    log = logging.getLogger("app")
    log.warning("evt_parent_busy")
    assert ENTERED.wait(2)
    log.warning("evt_queued")
    log.warning("evt_dropped_in_the_parent")

    def child() -> None:
        log.warning("evt_child")
        shutdown_logging()
        assert _messages() == ["evt_child"]

    assert run_in_forked_child(child) == 0


def test_a_forking_thread_releases_only_the_locks_it_holds(tmp_path: pathlib.Path) -> None:
    from swarmq.logging_fork import hold_handler_locks, release_handler_locks

    handler = _configure_file_mode(tmp_path)
    (target,) = handler.listener.handlers  # type: ignore[union-attr]
    held = threading.Event()
    errors: list[BaseException] = []

    def fork_in_progress() -> None:
        # Thread A: ``before``-Hook gelaufen, der Fork dauert noch.
        hold_handler_locks([target])
        held.set()
        RELEASE.wait()
        release_handler_locks()

    other = threading.Thread(target=fork_in_progress, daemon=True)
    other.start()
    assert held.wait(2)
    try:
        # Thread B: sein ``before``-Hook bekam nichts (Budget), sein
        # ``after_in_parent`` laeuft jetzt.
        release_handler_locks()
    except RuntimeError as exc:  # "cannot release un-acquired lock"
        errors.append(exc)
    acquired_by_b = target.lock.acquire(timeout=0.05)  # type: ignore[union-attr]
    if acquired_by_b:
        target.lock.release()  # type: ignore[union-attr]
    RELEASE.set()
    other.join(2)

    assert errors == []
    assert not acquired_by_b  # Thread A haelt sein Lock bis zu seinem Fork-Ende
    assert target.lock.acquire(timeout=0.5)  # type: ignore[union-attr]
    target.lock.release()  # type: ignore[union-attr]


class _BusyLock:
    """Lock-Attrappe, die nie zu bekommen ist und die Wartezeit jedes
    Versuchs festhaelt -- statt die Wanduhr zu messen (deterministisch)."""

    def __init__(self) -> None:
        self.timeouts: list[float] = []

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        self.timeouts.append(timeout)
        return False

    def release(self) -> None:
        raise AssertionError("never acquired")


def test_a_target_still_busy_since_the_last_fork_is_not_waited_for_again() -> None:
    from swarmq.logging_fork import hold_handler_locks, release_handler_locks

    target = logging.Handler()
    busy = _BusyLock()
    target.lock = busy  # type: ignore[assignment]

    hold_handler_locks([target])  # erster Fork: wartet bis zum Budget
    release_handler_locks()
    hold_handler_locks([target])  # zweiter Fork: nur ein Versuch ohne Warten
    release_handler_locks()

    first, second = busy.timeouts
    assert first > 0
    assert second == 0


def test_multiprocessing_exit_before_shutdown_logging_writes_all_records(
    tmp_path: pathlib.Path,
) -> None:
    def child() -> None:
        import multiprocessing.process
        import multiprocessing.util

        # Wie im Kind von ``multiprocessing.Process``: keine geerbten
        # Finalizer und Kinder des Testprozesses.
        multiprocessing.util._finalizer_registry.clear()
        multiprocessing.process._children.clear()  # type: ignore[attr-defined]
        thread_errors: list[object] = []
        threading.excepthook = thread_errors.append
        _configure_file_mode(tmp_path, queue={"()": "multiprocessing.Queue"})
        log = logging.getLogger("app")
        for i in range(3):
            log.warning("evt_exit_%d", i)

        # Interpreterende: ``multiprocessing`` wurde nach swarmq importiert,
        # seine atexit-Funktion laeuft vor ``shutdown_logging``.
        multiprocessing.util._exit_function()  # type: ignore[attr-defined]
        shutdown_logging()

        assert thread_errors == []
        assert _messages() == [f"evt_exit_{i}" for i in range(3)]

    assert run_in_forked_child(child, timeout=3.0) == 0


def test_a_record_after_multiprocessing_closed_the_pipe_is_counted_as_dropped(
    tmp_path: pathlib.Path,
) -> None:
    def child() -> None:
        import multiprocessing.process
        import multiprocessing.util

        from swarmq.logging import dropped_log_records

        multiprocessing.util._finalizer_registry.clear()
        multiprocessing.process._children.clear()  # type: ignore[attr-defined]
        thread_errors: list[object] = []
        threading.excepthook = thread_errors.append
        _configure_file_mode(tmp_path, queue={"()": "multiprocessing.Queue"})
        log = logging.getLogger("app")
        log.warning("evt_before_exit")
        multiprocessing.util._exit_function()  # type: ignore[attr-defined]
        before = dropped_log_records()

        log.warning("evt_after_exit")  # etwa aus einer spaeteren atexit-Funktion
        shutdown_logging()

        assert thread_errors == []
        assert dropped_log_records() - before == 1
        assert _messages() == ["evt_before_exit"]

    assert run_in_forked_child(child, timeout=3.0) == 0


def test_a_sentinel_left_in_a_closed_queue_is_not_counted_as_dropped(
    tmp_path: pathlib.Path,
) -> None:
    def child() -> None:
        import multiprocessing.process
        import multiprocessing.util

        from swarmq.logging import dropped_log_records

        multiprocessing.util._finalizer_registry.clear()
        multiprocessing.process._children.clear()  # type: ignore[attr-defined]
        handler = _configure_file_mode(tmp_path, queue={"()": "multiprocessing.Queue"})
        listener = handler.listener
        assert listener is not None
        log = logging.getLogger("app")
        log.warning("evt_before_exit")
        multiprocessing.util._exit_function()  # type: ignore[attr-defined]
        before = dropped_log_records()

        # Reihenfolge wie unter CPU-Last: Der neu gestartete Listener liest
        # (und bemerkt die geschlossene Pipe) erst, nachdem
        # ``shutdown_logging`` ihm den Sentinel angeboten hat.
        mp_queue: Any = handler.queue
        sentinel_offered = threading.Event()
        put_nowait, get = mp_queue.put_nowait, mp_queue.get

        def put_nowait_and_signal(item: object) -> None:
            put_nowait(item)
            if item is listener._sentinel:  # type: ignore[attr-defined]
                sentinel_offered.set()

        def get_after_the_sentinel(*args: object, **kwargs: object) -> object:
            assert sentinel_offered.wait(2)
            return get(*args, **kwargs)

        mp_queue.put_nowait = put_nowait_and_signal
        mp_queue.get = get_after_the_sentinel

        log.warning("evt_after_exit")
        shutdown_logging()

        assert sentinel_offered.is_set()
        assert dropped_log_records() - before == 1
        assert _messages() == ["evt_before_exit"]

    assert run_in_forked_child(child, timeout=3.0) == 0
