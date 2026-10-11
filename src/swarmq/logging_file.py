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
import os
import queue
import subprocess
import sys
import threading
import time
import weakref
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

from swarmq.exceptions import LoggingConfigError
from swarmq.logging_drain import Deadline, abandon, drain_expired

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

    def add(self, n: int = 1) -> None:
        """Zaehlt ``n`` weitere Ereignisse; ohne Lock wie der Aufrufer
        (``_StructlogQueueHandler._drop``): Unter Konkurrenz darf er
        untertreiben."""
        self.count += n

    def reset(self) -> None:
        """Vergisst Zaehler und gemeldeten Stand (Kind nach dem Fork, KTD10)."""
        self.count = self.reported = 0

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
        report = logging.LogRecord("swarmq.logging", logging.WARNING, __file__, 0, self.event, None, None)
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
        self._report.add()
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


class ListenerReports:
    """Die Meldungen eines Listeners (Plan 2026-10-06-1811, KTD1, KTD2):
    Handler-Fehler (``errors``) und verworfene Records (``drops``), beide
    aus dem Listener-Thread ueber dessen eigene Handler gemeldet. Gemeinsam
    fuer die eingebauten und die dictConfig-Listener."""

    def __init__(self, dispatch: Callable[[logging.LogRecord], None]) -> None:
        self.errors = HandlerErrors(dispatch)
        self.drops = RateLimitedReport("log_records_dropped", self.errors.handle)

    def handle(self, record: logging.LogRecord) -> None:
        """Gibt den Record weiter und faengt jede ``Exception`` (KTD1);
        danach meldet er neue Drops (KTD2)."""
        self.errors.handle(record)
        self.drops.report_due()

    def serve(self, monitor: Callable[[], None]) -> None:
        """Thread-Ziel: ``monitor`` laufen lassen; beim Ende noch nicht
        gemeldete Drops und Handler-Fehler melden, auch innerhalb der Frist."""
        monitor()
        self.drops.report_due(final=True)
        self.errors.report_due(final=True)


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
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
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


def iter_loggers() -> Iterator[tuple[str, logging.Logger]]:
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


def start_queue_listeners(
    on_drop: Callable[[], None],
    *,
    skip: tuple[type[logging.Handler], ...] = (),
    drain: Callable[[], None] | None = None,
) -> None:
    """Startet die Listener aller ``QueueHandler`` aus der dictConfig (KTD9).

    ``dictConfig`` legt sie an, startet sie aber nicht. Gestoppt -- und damit
    geleert -- werden sie per ``shutdown_logging``, also auch bei
    Prozessende (``atexit``). ``on_drop`` zaehlt einen wegen voller Queue
    verworfenen Eintrag (Plan 2026-10-06-1811, KTD1). Handler der Typen
    ``skip`` -- swarmqs eigene, deren Listener lazy starten -- bleiben aussen
    vor (Plan 2026-10-06-1811, U9). ``drain`` leert alle Listener; mit einer
    ``multiprocessing``-Queue laeuft es vor deren Finalizer
    (:func:`_drain_before_multiprocessing_exit`).
    """
    loggers = [logging.getLogger()] + [lg for _, lg in iter_loggers()]
    for handler in {h for lg in loggers for h in lg.handlers}:
        listener = getattr(handler, "listener", None)
        if (
            isinstance(handler, logging.handlers.QueueHandler)
            and not isinstance(handler, skip)
            and isinstance(listener, logging.handlers.QueueListener)
            and _thread_of(listener) is None
        ):
            drops = _report_from_listener(listener)
            listener.start()
            CONFIG_LISTENERS.append(listener)
            _restart_lazily(handler, listener, on_drop, drops)
            _STARTED_HANDLERS[handler] = drops
            if drain is not None and _shared_with_parent(handler.queue):
                _drain_before_multiprocessing_exit(drain)


_BEFORE_MULTIPROCESSING_QUEUES = 100
"""Prioritaet des Leerens unter den ``multiprocessing``-Finalizern: hoeher
als die der Queue (10), die beim Prozessende ihre Pipe schliesst."""

_MP_EXIT_DRAIN_PID: int | None = None
"""Prozess, in dem das Leeren vor ``multiprocessing`` registriert ist."""


def _drain_before_multiprocessing_exit(drain: Callable[[], None]) -> None:
    """Registriert ``drain`` einmal je Prozess vor den Finalizern von
    ``multiprocessing``.

    Dessen atexit-Funktion wird beim Import von ``multiprocessing``
    registriert, meist nach swarmqs ``shutdown_logging``, und laeuft damit
    zuerst (atexit ist LIFO). Ihr Finalizer der Queue laesst den Feeder die
    Pipe schliessen -- vorher noch ungelesene Records waeren verloren, und
    der Listener stuerbe mit ``EOFError``/``OSError``-Traceback. Ein
    ``Finalize`` gilt nur im Prozess, der es angelegt hat."""
    global _MP_EXIT_DRAIN_PID
    pid = os.getpid()
    if pid == _MP_EXIT_DRAIN_PID:
        return
    _MP_EXIT_DRAIN_PID = pid
    from multiprocessing import util  # schon importiert: Die Queue braucht es

    util.Finalize(None, drain, exitpriority=_BEFORE_MULTIPROCESSING_QUEUES)


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
        drops.reset()
        listener = handler.listener
        if listener is None:
            continue
        if _shared_with_parent(handler.queue):
            # Schluss-Review (P1): Der Listener des Elternprozesses bleibt
            # der einzige Konsument; das Kind reiht nur ein.
            _state(listener).parent_owned = True
            _reattach_in_child(handler.queue)
            continue
        fresh = _fresh_queue(handler.queue)
        if fresh is None:
            continue
        handler.queue = listener.queue = fresh
        _forget_thread(listener)


def _shared_with_parent(inherited: object) -> bool:
    """Verbindet die Queue das Kind mit dem Elternprozess
    (``multiprocessing``-Queue oder Manager-Proxy)? Ein zweiter Listener im
    Kind konkurrierte mit dem des Elternprozesses um dieselbe Pipe, und ein
    Kind, das mitten im ``get`` endet, hinterliesse deren Lese-Lock belegt.
    Ohne importiertes ``multiprocessing`` gibt es keine solche Queue."""
    queues = sys.modules.get("multiprocessing.queues")
    managers = sys.modules.get("multiprocessing.managers")
    if queues is not None and isinstance(inherited, (queues.Queue, queues.SimpleQueue)):
        return True
    return managers is not None and isinstance(inherited, managers.BaseProxy)


def _reattach_in_child(inherited: Any) -> None:
    """Unter rohem ``os.fork`` tut das Kind fuer die geteilte Queue, was sonst
    nur ``multiprocessing.Process`` beim Start tut (``_run_after_forkers``);
    ein zweiter Aufruf durch ``multiprocessing`` ist harmlos.

    - ``multiprocessing.Queue``: Feeder-Thread zuruecksetzen
      (``_after_fork``). Der geerbte fehlt im Kind, und ``put`` legte sonst
      nur in einen Puffer, den niemand sendet.
    - Manager-Proxy: Die thread-lokale Verbindung (``_tls``) und die Menge
      der Referenzen vergessen, dann eine eigene Referenz anmelden
      (``_after_fork``). Sonst benutzte das Kind die Socket-Verbindung des
      Elternprozesses mit, und gleichzeitige Aufrufe beider laesen einander
      die Antworten weg -- beide haengen.

    ``SimpleQueue`` hat keinen Feeder."""
    queues = sys.modules.get("multiprocessing.queues")
    if queues is not None and isinstance(inherited, queues.Queue):
        inherited._after_fork()
        return
    managers = sys.modules.get("multiprocessing.managers")
    if managers is not None and isinstance(inherited, managers.BaseProxy):
        inherited._tls.__dict__.clear()
        inherited._all_serials.clear()
        inherited._after_fork()


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


def _thread_of(listener: logging.handlers.QueueListener) -> threading.Thread | None:
    """Der Thread des stdlib-``start`` (``_thread``, seit 3.2 unveraendert;
    typeshed kennt ihn nicht)."""
    thread: threading.Thread | None = getattr(listener, "_thread", None)
    return thread


def _forget_thread(listener: logging.handlers.QueueListener) -> None:
    """Der Listener gilt als nicht gestartet; der stdlib-``start`` wirft
    sonst."""
    setattr(listener, "_thread", None)  # noqa: B010


@dataclass
class _ListenerState:
    """swarmqs Zustand zu einem dictConfig-Listener; die Klasse gehoert der
    App, deshalb neben statt an ihm (Plan 2026-10-06-1811, KTD3)."""

    stop: bool = False
    """Beim Aufgeben ohne Platz fuer den Sentinel: an der leeren Queue enden."""
    abandoned: FileDraining | None = None
    """Das aufgegebene Leeren, solange sein Thread noch haengen kann."""
    parent_owned: bool = False
    """Im Kind nach ``os.fork``: Die Queue teilt es mit dem Elternprozess,
    dessen Listener sie leert; hier startet keiner (Schluss-Review, P1)."""
    closed: bool = False
    """Die Queue ist geschlossen (``EOFError``/``OSError`` beim Lesen): Es
    startet kein Listener mehr, jeder Record zaehlt als verworfen."""
    counted: int = 0
    """Wie viele Records der geschlossenen Queue schon als verworfen zaehlen;
    lesen laesst sie sich nicht mehr."""


_STATES: weakref.WeakKeyDictionary[logging.handlers.QueueListener, _ListenerState] = weakref.WeakKeyDictionary()


def _state(listener: logging.handlers.QueueListener) -> _ListenerState:
    """Der Zustand des Listeners; beim ersten Zugriff angelegt."""
    state = _STATES.get(listener)
    if state is None:
        state = _STATES.setdefault(listener, _ListenerState())
    return state


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
            _state(self.listener).stop = True
            self.sentinel_pending = not _offer_sentinel(self.listener, 0)
        self.thread.join(deadline.remaining())
        if not self.thread.is_alive():
            state = _state(self.listener)
            if state.closed and not self.sentinel_pending:
                # Der Thread endete an der geschlossenen Pipe, bevor er den
                # Sentinel las: Der liegt noch in ``qsize``, ist aber kein
                # verworfener Record (``_count_closed``).
                state.counted += 1
            with _CONFIG_LISTENER_LOCK:
                if _thread_of(self.listener) is self.thread:
                    _forget_thread(self.listener)
            return True
        self.lost = _qsize(self.listener.queue)
        # Solange der Thread lebt, startet ``enqueue_restarting`` keinen
        # neuen auf dieser Queue und zaehlt neue Records als verworfen.
        _state(self.listener).abandoned = self
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
    except NotImplementedError, AttributeError:  # etwa ``multiprocessing`` auf macOS
        return 0
    except EOFError, OSError:  # Proxy eines beendeten Managers
        return 0


def start_pending_queue_listeners(deadline: Deadline) -> int:
    """Startet die Listener gestarteter dictConfig-Handler, deren Thread
    nicht mehr laeuft, in deren Queue aber noch Records liegen (Plan
    2026-10-06-1811, KTD3): Ein Aufrufer, der den alten Thread noch leben
    sah, reihte hinter dessen Sentinel ein. Geteilte Queues des
    Elternprozesses und aufgegebene Listener bleiben aussen vor.

    Nach Ablauf der Frist startet keiner mehr; die Records werden
    verworfen. Liefert ihre Zahl -- zaehlen muss sie der Aufrufer."""
    discarded = 0
    for handler, drops in list(_STARTED_HANDLERS.items()):
        listener = handler.listener
        if listener is not None and _state(listener).closed:
            lost = _count_closed(listener)
            drops.add(lost)
            discarded += lost
            continue
        if (
            listener is None
            or _alive(listener)
            or _state(listener).parent_owned
            or _still_abandoned(listener)
            or not _qsize(handler.queue)
        ):
            continue
        if deadline.remaining() <= 0:
            lost = discard_pending(handler.queue)
            drops.add(lost)
            discarded += lost
            continue
        with _CONFIG_LISTENER_LOCK:
            if not _alive(listener):
                _restart(listener)
    return discarded


def _count_closed(listener: logging.handlers.QueueListener) -> int:
    """Die noch nicht gezaehlten Records einer geschlossenen Queue."""
    state = _state(listener)
    lost = max(_qsize(listener.queue) - state.counted, 0)
    state.counted += lost
    return lost


def discard_pending(queue_: Any) -> int:
    """Leert ``queue_`` ohne zu warten; liefert die Zahl der Eintraege.
    Eine abgeschaltete (``queue.ShutDown``) oder geschlossene Queue (Proxy
    eines beendeten Managers, geschlossene Pipe) gilt als leer -- das Leeren
    wirft nie in den Aufrufer. Zaehlen muss der Aufrufer."""
    count = 0
    while True:
        try:
            queue_.get_nowait()
        except queue.Empty, queue.ShutDown, EOFError, OSError:
            return count
        count += 1


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
        thread = _thread_of(listener)
        if thread is not None and thread.is_alive():
            closing.append(FileDraining(listener, thread))
    return closing


def _report_from_listener(listener: logging.handlers.QueueListener) -> RateLimitedReport:
    """Der stdlib-``_monitor`` faengt nichts: Ein werfender Handler, Filter
    oder Formatter beendete den Thread (KTD1). Am Exemplar wie bei
    ``_restart_lazily``; gefangen und gemeldet wird pro Record. Nach jedem
    Record und beim Ende meldet der Listener neue Drops wie der eingebaute
    (KTD2); deren Zaehler liefert er zurueck.

    Ist die Queue geschlossen -- die Pipe einer ``multiprocessing``-Queue
    nach deren Finalizer, ein beendeter Manager --, endet er ohne Traceback;
    was noch in ihr liegt, zaehlt das naechste Leeren als verworfen."""
    reports = ListenerReports(listener.handle)
    monitor = listener._monitor  # type: ignore[attr-defined]

    def serve() -> None:
        # Thread-Ziel des stdlib-``start``.
        reports.serve(monitor)

    dequeue = listener.dequeue

    def dequeue_or_stop(block: bool) -> Any:
        # Beim Aufgeben ohne Platz fuer den Sentinel (KTD3): an der leeren
        # Queue enden. ``queue.Empty`` statt Sentinel -- dessen ``task_done``
        # waere einer zu viel.
        source: Any = listener.queue
        if _state(listener).stop and source.empty():
            raise queue.Empty
        try:
            return dequeue(block)
        except EOFError, OSError:
            _state(listener).closed = True
            raise queue.Empty from None

    setattr(listener, "handle", reports.handle)  # noqa: B010
    setattr(listener, "_monitor", serve)  # noqa: B010
    setattr(listener, "dequeue", dequeue_or_stop)  # noqa: B010
    return reports.drops


_CONFIG_LISTENER_LOCK = threading.Lock()
"""Schuetzt den Wiederstart der dictConfig-Listener; eigenes Lock, weil ein
``_SwarmqQueueListener`` aus ``swarmq.logging_queue`` in der Datei beim Start
dessen ``_LISTENER_LOCK`` nimmt."""


def _alive(listener: logging.handlers.QueueListener) -> bool:
    """Lebt der Thread des Listeners? ``running`` des eingebauten (KTD1)."""
    thread = _thread_of(listener)
    return thread is not None and thread.is_alive()


def _restart(listener: logging.handlers.QueueListener) -> None:
    """Startet den Listener neu, unter ``_CONFIG_LISTENER_LOCK``. Ein toter
    Thread zaehlt wie keiner (KTD1); der stdlib-``start`` wirft sonst. Die
    Queue bleibt."""
    _forget_thread(listener)
    _state(listener).stop = False
    listener.start()
    CONFIG_LISTENERS.append(listener)


def _still_abandoned(listener: logging.handlers.QueueListener) -> bool:
    """Haengt der aufgegebene Thread noch (KTD3)?"""
    state = _state(listener)
    draining = state.abandoned
    if draining is None:
        return False
    if not draining.thread.is_alive():
        state.abandoned = None
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

    def dropped() -> None:
        on_drop()
        drops.add()

    def enqueue_restarting(record: logging.LogRecord) -> None:
        if _state(listener).closed or _still_abandoned(listener):
            dropped()
            return
        if sys.is_finalizing():
            # Finalisierung (Plan 2026-10-06-1811, KTD5): synchron ausgeben.
            listener.handle(record)
            return
        if not _alive(listener) and not _state(listener).parent_owned:
            if drain_expired():
                # Ein Leeren ist ueber seiner Frist: kein neuer Thread, den
                # es sofort wieder aufgaebe.
                dropped()
                return
            with _CONFIG_LISTENER_LOCK:
                if not _alive(listener):
                    try:
                        _restart(listener)
                    except PythonFinalizationError:
                        _forget_thread(listener)
                        listener.handle(record)
                        return
        try:
            enqueue(record)
        except queue.Full:
            dropped()

    # Am Exemplar: die Klasse des Handlers gehoert der dictConfig der App.
    setattr(handler, "enqueue", enqueue_restarting)  # noqa: B010
