"""Logging-Konfiguration aus einer dictConfig-Datei (Plan 2026-10-06-0831, KTD9).

Ausgelagert aus ``swarmq.logging`` (800-Zeilen-Guard): Laden und Pruefen der
Datei sowie die Listener der ``QueueHandler``, die ``dictConfig`` anlegt, aber
nicht startet. ``HandlerErrors`` teilen sie mit den eingebauten Listenern in
``swarmq.logging_queue`` (Plan 2026-10-06-1811, KTD1).
"""

from __future__ import annotations

import contextlib
import json as jsonlib
import logging
import logging.handlers
import queue
import subprocess
import sys
import threading
import time
import weakref
from collections.abc import Callable, Iterator
from typing import Any, ClassVar

from swarmq.exceptions import LoggingConfigError
from swarmq.logging_drain import Deadline, abandon

CONFIG_LISTENERS: list[logging.handlers.QueueListener] = []
"""Von ``configure_logging`` gestartete Listener aus einer dictConfig-Datei;
``shutdown_logging`` stoppt und leert sie."""


class RateLimitedReport:
    """Ein Zaehler, den ein Listener hoechstens einmal pro Sekunde als
    Record ``event`` meldet (Plan 2026-10-06-1811, KTD1, KTD2).

    Logger ``swarmq.logging``, Level WARNING, Feld ``count``. Der Record geht
    an ``emit`` -- die eigenen Handler des Listeners, nie die Queue: Der
    Aufrufer bleibt unberuehrt, und eine volle Queue verschluckt ihn nicht.
    """

    INTERVAL_S = 1.0

    clock: ClassVar[Callable[[], float]] = time.monotonic
    """Uhr der Ratenbegrenzung; Tests ersetzen sie hier, ohne
    ``time.monotonic`` prozessweit umzuhaengen (die Frist des Leerens,
    ``Deadline``, laeuft weiter auf der echten Uhr)."""

    def __init__(self, event: str, emit: Callable[[logging.LogRecord], None]) -> None:
        self.event = event
        self.count = 0
        self.reported = 0
        self._emit = emit
        self._last_report = float("-inf")

    def report_due(self, *, final: bool = False) -> bool:
        """Meldet den Zaehler, wenn er seit der letzten Meldung gestiegen und
        die Frist abgelaufen ist; ``True``, wenn gemeldet wurde. ``final``
        (Ende des Listeners) meldet ohne Frist: Danach kaeme keine Meldung
        mehr."""
        count = self.count
        if count <= self.reported:
            return False
        now = type(self).clock()  # ueber die Klasse: eine Funktion bindet nicht
        if not final and now - self._last_report < self.INTERVAL_S:
            return False
        self._last_report = now
        self.reported = count
        report = logging.LogRecord(
            "swarmq.logging", logging.WARNING, __file__, 0, self.event, None, None
        )
        report.count = count
        self._emit(report)
        return True


class HandlerErrors:
    """Fehler aus Handlern, Filtern und Formattern eines Listeners (Plan
    2026-10-06-1811, KTD1): gefangen, gezaehlt und hoechstens einmal pro
    Sekunde als ``log_handler_errors`` gemeldet.

    Die Meldung entsteht im Listener-Thread und laeuft ueber dessen eigene
    Handler, nie ueber die Queue: Der Aufrufer bleibt unberuehrt, und eine
    volle Queue verschluckt sie nicht. Scheitert sie selbst, zaehlt das als
    weiterer Fehler; eine Schleife entsteht nicht, weil die Frist dann noch
    laeuft. Dann geht sie best effort direkt auf stderr, weiterhin aus dem
    Listener-Thread: Ein blockiertes stderr haelt hoechstens ihn auf, nie
    einen Aufrufer.
    """

    def __init__(self, dispatch: Callable[[logging.LogRecord], None]) -> None:
        self._dispatch = dispatch
        self._report = RateLimitedReport("log_handler_errors", self.handle)

    @property
    def count(self) -> int:
        """Bisher gefangene Fehler."""
        return self._report.count

    def handle(self, record: logging.LogRecord) -> None:
        """Gibt den Record weiter; eine ``Exception`` wird gezaehlt. Danach
        meldet er einen zurueckgehaltenen Fehlerstand, sobald die Frist
        abgelaufen ist -- auch wenn keine Fehler mehr folgen."""
        try:
            self._dispatch(record)
        except Exception:
            self.failed()
        else:
            self.report_due()

    def failed(self) -> None:
        """Zaehlt einen Fehler und meldet, sobald die Frist abgelaufen ist."""
        self._report.count += 1
        self.report_due()

    def report_due(self, *, final: bool = False) -> None:
        """Meldet den Fehlerstand wie :meth:`RateLimitedReport.report_due`."""
        count = self.count
        if self._report.report_due(final=final) and self.count != count:
            with contextlib.suppress(Exception):
                print(
                    f"swarmq: log_handler_errors — {count} error(s) in log handlers so far",
                    file=sys.stderr,
                )


def load_log_config(path: str) -> str:
    """Laedt eine dictConfig-Datei und liefert sie als JSON-String (KTD9).

    Fehlt ``disable_existing_loggers``, gilt ``False``: sonst wuerden
    bereits importierte ``swarmq.*``- und ``aio_pika``-Logger stumm. Der
    String ist pickle-sicher und geht an die Kindprozesse, die die Datei
    selbst nie lesen.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            config = _parse_log_config(path, fh.read())
    except (OSError, ValueError, ImportError) as exc:
        # ValueError deckt JSONDecodeError und UnicodeDecodeError ab.
        raise LoggingConfigError(path, exc) from exc
    if not isinstance(config, dict):
        cause = TypeError(f"expected a mapping, got {type(config).__name__}")
        raise LoggingConfigError(path, cause) from cause
    config.setdefault("disable_existing_loggers", False)
    return jsonlib.dumps(config)


_DICT_CONFIG_PROBE = """
import json, logging.config, sys
try:
    logging.config.dictConfig(json.loads(sys.stdin.read()))
except Exception as exc:
    cause = exc.__cause__
    print(f"{exc}" + (f": {cause}" if cause else ""), file=sys.stderr)
    sys.exit(1)
"""


class DictConfigProbe:
    """Laufende ``dictConfig``-Probe in einem eigenen Interpreter.

    Startet sofort und blockiert nie auf das Ende: Der Supervisor pollt sie
    aus dem Monitor-Loop (Plan 2026-10-06-1811, KTD8). Eigene Sitzung
    (``start_new_session``), damit ein Ctrl-C an die Vordergrund-Prozessgruppe
    sie nicht abbricht und als "ungueltig" erscheinen laesst.
    """

    def __init__(self, raw: str) -> None:
        self._proc: subprocess.Popen[str] = subprocess.Popen(
            [sys.executable, "-c", _DICT_CONFIG_PROBE],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            text=True, start_new_session=True,
        )
        assert self._proc.stdin is not None
        try:
            self._proc.stdin.write(raw)
            self._proc.stdin.close()
        except OSError:
            pass  # Kind schon tot: ``poll`` liefert seinen Exit-Code

    def poll(self) -> int | None:
        """Exit-Code oder ``None``, solange die Probe laeuft."""
        return self._proc.poll()

    def wait(self, timeout: float) -> int:
        """Auf das Ende warten (``swarmq worker --check``); ``TimeoutExpired``."""
        return self._proc.wait(timeout=timeout)

    def kill(self) -> None:
        """Hart beenden und einsammeln; kein Zombie im Supervisor."""
        self._proc.kill()
        self._proc.wait()
        self._close()

    def error(self) -> str:
        """Fehlertext der beendeten Probe (stderr)."""
        assert self._proc.stderr is not None
        try:
            text: str = self._proc.stderr.read()
            return text.strip()
        finally:
            self._close()

    def _close(self) -> None:
        if self._proc.stderr is not None:
            self._proc.stderr.close()


def probe_dict_config(path: str, raw: str) -> None:
    """Wendet die dictConfig in einem eigenen Interpreter an und wartet darauf.

    Tippfehler in Handlern oder Formattern sieht erst ``dictConfig``; im
    pruefenden Prozess angewandt (``swarmq worker --check``), baute sie
    dessen Logging um. Der Supervisor vor dem Hot-Reload wartet nicht,
    sondern pollt :class:`DictConfigProbe` (KTD8). Das Probe-Kind
    hat Arbeitsverzeichnis und Env des Aufrufers, findet also dieselben
    Handler-Klassen wie der spaetere Start.

    Raises:
        LoggingConfigError: ``dictConfig`` lehnt die Konfiguration ab oder
            das Probe-Kind laesst sich nicht starten.
    """
    try:
        probe = DictConfigProbe(raw)
    except OSError as exc:
        raise LoggingConfigError(path, exc) from exc
    try:
        returncode = probe.wait(timeout=30)
    except subprocess.TimeoutExpired as exc:
        probe.kill()
        raise LoggingConfigError(path, exc) from exc
    if returncode != 0:
        cause = ValueError(probe.error() or f"dictConfig exited {returncode}")
        raise LoggingConfigError(path, cause) from cause
    probe.error()  # Pipe schliessen


def _parse_log_config(path: str, text: str) -> Any:
    """JSON immer, YAML nur mit installiertem PyYAML (KTD9)."""
    if not path.endswith((".yaml", ".yml")):
        return jsonlib.loads(text)
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:
        msg = "YAML logging configs need PyYAML (pip install pyyaml)"
        raise ImportError(msg) from exc
    return yaml.safe_load(text)


def _iter_loggers() -> Iterator[tuple[str, logging.Logger]]:
    """Name und Logger aller angelegten Logger ohne Root und Platzhalter;
    ueber eine Kopie, weil andere Threads Logger anlegen koennen."""
    for name, lg in list(logging.Logger.manager.loggerDict.items()):
        if isinstance(lg, logging.Logger):
            yield name, lg


_STARTED_HANDLERS: weakref.WeakKeyDictionary[logging.handlers.QueueHandler, RateLimitedReport] = (
    weakref.WeakKeyDictionary()
)
"""Die Queue-Handler, deren Listener ``start_queue_listeners`` gepatcht und
gestartet hat, mit dem Drop-Zaehler des Listeners; der Fork-Hook setzt sie im
Kind zurueck (Plan 2026-10-06-1811, KTD10)."""


def start_queue_listeners(on_drop: Callable[[], None]) -> None:
    """Startet die Listener aller ``QueueHandler`` aus der dictConfig (KTD9).

    ``dictConfig`` legt sie an, startet sie aber nicht. Gestoppt -- und damit
    geleert -- werden sie per ``shutdown_logging``, also auch bei
    Prozessende (``atexit``). ``on_drop`` zaehlt einen wegen voller Queue
    verworfenen Eintrag (Plan 2026-10-06-1811, KTD1).
    """
    loggers = [logging.getLogger()] + [lg for _, lg in _iter_loggers()]
    for handler in {h for lg in loggers for h in lg.handlers}:
        listener = getattr(handler, "listener", None)
        if (
            isinstance(handler, logging.handlers.QueueHandler)
            and isinstance(listener, logging.handlers.QueueListener)
            and getattr(listener, "_thread", None) is None
        ):
            drops = _report_from_listener(listener)
            listener.start()
            CONFIG_LISTENERS.append(listener)
            _restart_lazily(handler, listener, on_drop, drops)
            _STARTED_HANDLERS[handler] = drops


def running_targets() -> Iterator[logging.Handler]:
    """Die Ziel-Handler der laufenden dictConfig-Listener; ihre Locks nimmt
    der ``before``-Hook vor einem Fork (Plan 2026-10-06-1811, KTD10)."""
    for handler in list(_STARTED_HANDLERS.keys()):
        listener = handler.listener
        if listener is not None and _alive(listener):
            yield from listener.handlers


def reset_queue_listeners_after_fork() -> None:
    """Im Kind nach ``os.fork()`` (Plan 2026-10-06-1811, KTD10): Die
    geerbten Listener gehoeren dem Elternprozess, ihr Thread fehlt im Kind.
    Jeder gestartete Queue-Handler bekommt eine frische Queue, sein Listener
    gilt als nicht gestartet; der naechste Record startet ihn. Die geerbten
    Records schreibt der Elternprozess -- im Kind erschienen sie doppelt.
    Ein beim Fork gehaltenes Wiederstart-Lock hat im Kind keinen Besitzer
    mehr und wird ersetzt."""
    global _CONFIG_LISTENER_LOCK
    _CONFIG_LISTENER_LOCK = threading.Lock()
    CONFIG_LISTENERS.clear()
    for handler, drops in list(_STARTED_HANDLERS.items()):
        # Die Drops des Elternprozesses meldet der Elternprozess.
        drops.count = drops.reported = 0
        listener = handler.listener
        fresh = _fresh_queue(handler.queue)
        if listener is None or fresh is None:
            continue
        handler.queue = listener.queue = fresh
        setattr(listener, "_thread", None)  # noqa: B010


def _fresh_queue(inherited: object) -> Any:
    """Leere Queue gleichen Typs und gleicher ``maxsize`` (KTD10); ``None``
    fuer Queues, die swarmq nicht nachbauen kann oder darf: eine
    ``multiprocessing``-Queue verbindet das Kind mit dem Elternprozess, eine
    eigene Klasse der App hat einen unbekannten Konstruktor."""
    if type(inherited) is queue.SimpleQueue:
        return queue.SimpleQueue()
    if type(inherited) is queue.Queue:
        return queue.Queue(inherited.maxsize)
    return None


class FileDraining:
    """Ein dictConfig-Listener, der beim Leeren seine Queue abarbeitet
    (Plan 2026-10-06-1811, KTD3): Sentinel per ``put(timeout)``, Warten per
    ``join(timeout)`` -- beides innerhalb der gemeinsamen Frist.

    Wer aufgibt, kann die Queue einer fremden Klasse nicht schliessen. Was
    in ihr liegt, zaehlt als verworfen; schreibt der Thread spaeter doch noch,
    ist die Zahl eine Obergrenze.
    """

    def __init__(self, listener: logging.handlers.QueueListener, thread: threading.Thread) -> None:
        self.listener = listener
        self.thread = thread
        self.lost = 0
        self.sentinel_pending = not _offer_sentinel(listener, 0)

    def wait(self, deadline: Deadline) -> bool:
        """Bis zur Frist warten; ``False``, wenn aufgegeben wurde."""
        if self.sentinel_pending:
            self.sentinel_pending = not _offer_sentinel(self.listener, deadline.remaining())
        if self.sentinel_pending:
            # Ohne Sentinel liefe der Thread nach dem Abarbeiten ewig weiter:
            # Er endet stattdessen an der leeren Queue (``_report_from_listener``).
            setattr(self.listener, "_swarmq_stop", True)  # noqa: B010
            self.sentinel_pending = not _offer_sentinel(self.listener, 0)
        self.thread.join(deadline.remaining())
        if not self.thread.is_alive():
            with _CONFIG_LISTENER_LOCK:
                if getattr(self.listener, "_thread", None) is self.thread:
                    setattr(self.listener, "_thread", None)  # noqa: B010
            return True
        self.lost = _qsize(self.listener.queue)
        # Solange der Thread lebt, startet ``enqueue_restarting`` keinen
        # neuen auf dieser Queue und zaehlt neue Records als verworfen.
        setattr(self.listener, "_swarmq_abandoned", self)  # noqa: B010
        abandon(self.thread, self.listener.handlers)
        return False


def _offer_sentinel(listener: logging.handlers.QueueListener, timeout: float) -> bool:
    """Reiht den Sentinel ein, hoechstens ``timeout`` Sekunden lang; ``False``
    bei voller Queue. ``put_nowait`` des stdlib-``stop`` wuerfe dort."""
    sentinel = listener._sentinel  # type: ignore[attr-defined]
    target: Any = listener.queue  # ``_QueueLike`` kennt nur ``put_nowait``/``get``
    try:
        if timeout > 0:
            target.put(sentinel, True, timeout)
        else:
            target.put_nowait(sentinel)
    except queue.Full:
        return False
    return True


def _qsize(queue_: Any) -> int:
    try:
        return int(queue_.qsize())
    except (NotImplementedError, AttributeError):  # etwa ``multiprocessing`` auf macOS
        return 0


def close_queue_listeners() -> list[FileDraining]:
    """Schliesst die gestarteten dictConfig-Listener, ohne zu warten (KTD5,
    Plan 2026-10-06-1811 KTD3); gewartet wird gemeinsam mit den eingebauten.

    Wie der stdlib-``stop``, aber auf dem eigenen Thread: Zwischen dessen
    Ende und dem Vergessen kann ein Eintrag schon einen neuen gestartet haben
    (KTD1). Der bleibt eingetragen und wird in dieser Schleife geschlossen,
    statt verwaist weiterzulaufen.
    """
    closing: list[FileDraining] = []
    while CONFIG_LISTENERS:
        listener = CONFIG_LISTENERS.pop()
        thread: threading.Thread | None = getattr(listener, "_thread", None)
        if thread is not None and thread.is_alive():
            closing.append(FileDraining(listener, thread))
    return closing


def _report_from_listener(listener: logging.handlers.QueueListener) -> RateLimitedReport:
    """Der stdlib-``_monitor`` faengt nichts: Ein werfender Handler, Filter
    oder Formatter beendete den Thread (KTD1). Am Exemplar wie bei
    ``_restart_lazily``; gefangen und gemeldet wird pro Record. Nach jedem
    Record und beim Ende meldet der Listener neue Drops wie der eingebaute
    (KTD2); deren Zaehler liefert er zurueck."""
    errors = HandlerErrors(listener.handle)
    drops = RateLimitedReport("log_records_dropped", errors.handle)

    def handle(record: logging.LogRecord) -> None:
        errors.handle(record)
        drops.report_due()

    monitor = listener._monitor  # type: ignore[attr-defined]

    def serve() -> None:
        # Thread-Ziel des stdlib-``start``: beim Ende noch nicht gemeldete
        # Drops und Handler-Fehler melden, auch innerhalb der Frist.
        monitor()
        drops.report_due(final=True)
        errors.report_due(final=True)

    dequeue = listener.dequeue

    def dequeue_or_stop(block: bool) -> Any:
        # Beim Aufgeben ohne Platz fuer den Sentinel (KTD3): an der leeren
        # Queue enden. ``queue.Empty`` statt Sentinel -- dessen ``task_done``
        # waere einer zu viel.
        source: Any = listener.queue
        if getattr(listener, "_swarmq_stop", False) and source.empty():
            raise queue.Empty
        return dequeue(block)

    setattr(listener, "handle", handle)  # noqa: B010
    setattr(listener, "_monitor", serve)  # noqa: B010
    setattr(listener, "dequeue", dequeue_or_stop)  # noqa: B010
    return drops


_CONFIG_LISTENER_LOCK = threading.Lock()
"""Schuetzt den Wiederstart der dictConfig-Listener; eigenes Lock, weil ein
``_SwarmqQueueListener`` aus ``swarmq.logging_queue`` in der Datei beim Start
dessen ``_LISTENER_LOCK`` nimmt."""


def _alive(listener: logging.handlers.QueueListener) -> bool:
    """Lebt der Thread des Listeners? ``running`` des eingebauten (KTD1)."""
    thread: threading.Thread | None = getattr(listener, "_thread", None)
    return thread is not None and thread.is_alive()


def _still_abandoned(listener: logging.handlers.QueueListener) -> bool:
    """Haengt der aufgegebene Thread noch (KTD3)?"""
    draining: FileDraining | None = getattr(listener, "_swarmq_abandoned", None)
    if draining is None:
        return False
    if not draining.thread.is_alive():
        setattr(listener, "_swarmq_abandoned", None)  # noqa: B010
        return False
    return True


def _restart_lazily(
    handler: logging.handlers.QueueHandler,
    listener: logging.handlers.QueueListener,
    on_drop: Callable[[], None],
    drops: RateLimitedReport,
) -> None:
    """Wie beim eingebauten Handler (KTD5): Nach ``shutdown_logging`` startet
    der naechste Eintrag den Listener wieder, und das naechste
    ``shutdown_logging`` leert ihn. Das ist die Leerung pro Job im
    wiederverwendeten Compute-Kind (KTD14). Ebenso startet er einen Listener
    neu, dessen Thread gestorben ist (Plan 2026-10-06-1811, KTD1).

    Eine volle begrenzte Queue zaehlt als Drop: Der stdlib-``emit`` gaebe
    ``queue.Full`` an ``handleError`` -- ein Traceback im Aufrufer. Melden
    wird ihn der Listener (``drops``, KTD2), nie der Aufrufer."""
    enqueue = handler.enqueue

    def enqueue_restarting(record: logging.LogRecord) -> None:
        if _still_abandoned(listener):
            on_drop()
            drops.count += 1
            return
        if sys.is_finalizing():
            # Finalisierung (Plan 2026-10-06-1811, KTD5): synchron ausgeben.
            listener.handle(record)
            return
        if not _alive(listener):
            with _CONFIG_LISTENER_LOCK:
                if not _alive(listener):
                    # Ein toter Thread zaehlt wie keiner (KTD1); der
                    # stdlib-``start`` wirft sonst. Die Queue bleibt.
                    setattr(listener, "_thread", None)  # noqa: B010
                    setattr(listener, "_swarmq_stop", False)  # noqa: B010
                    try:
                        listener.start()
                    except PythonFinalizationError:
                        setattr(listener, "_thread", None)  # noqa: B010
                        listener.handle(record)
                        return
                    CONFIG_LISTENERS.append(listener)
        try:
            enqueue(record)
        except queue.Full:
            on_drop()
            drops.count += 1

    # Am Exemplar: die Klasse des Handlers gehoert der dictConfig der App.
    setattr(handler, "enqueue", enqueue_restarting)  # noqa: B010
