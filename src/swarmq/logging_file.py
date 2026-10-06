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
from collections.abc import Callable, Iterator
from typing import Any

from swarmq.exceptions import LoggingConfigError

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
        now = time.monotonic()
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
        """Gibt den Record weiter; eine ``Exception`` wird gezaehlt."""
        try:
            self._dispatch(record)
        except Exception:
            self.failed()

    def failed(self) -> None:
        """Zaehlt einen Fehler und meldet, sobald die Frist abgelaufen ist."""
        self._report.count += 1
        count = self.count
        if self._report.report_due() and self.count != count:
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


def stop_queue_listeners() -> None:
    """Stoppt und leert die gestarteten dictConfig-Listener (KTD5).

    Der stdlib-``stop`` reiht den Sentinel per ``put_nowait`` ein und wirft
    bei voller Queue ``queue.Full`` -- mitten in ``atexit`` oder einem
    ``finally``. Dann wird blockierend nachgereicht: Der laufende Listener
    schafft Platz, wie bei ``_SwarmqQueueListener.enqueue_sentinel``.

    Wie der stdlib-``stop``, aber auf dem eigenen Thread: Zwischen dessen
    Ende und dem Vergessen kann ein Eintrag schon einen neuen gestartet haben
    (KTD1). Der bleibt eingetragen und wird in dieser Schleife gestoppt,
    statt verwaist weiterzulaufen.
    """
    while CONFIG_LISTENERS:
        listener = CONFIG_LISTENERS.pop()
        thread: threading.Thread | None = getattr(listener, "_thread", None)
        if thread is None:
            continue
        try:
            listener.enqueue_sentinel()
        except queue.Full:
            listener.queue.put(listener._sentinel)  # type: ignore[attr-defined]
        thread.join()
        with _CONFIG_LISTENER_LOCK:
            if getattr(listener, "_thread", None) is thread:
                setattr(listener, "_thread", None)  # noqa: B010


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
        # Drops melden, auch innerhalb der Frist.
        monitor()
        drops.report_due(final=True)

    setattr(listener, "handle", handle)  # noqa: B010
    setattr(listener, "_monitor", serve)  # noqa: B010
    return drops


_CONFIG_LISTENER_LOCK = threading.Lock()
"""Schuetzt den Wiederstart der dictConfig-Listener; eigenes Lock, weil ein
``_SwarmqQueueListener`` aus ``swarmq.logging`` in der Datei beim Start
dessen ``_LISTENER_LOCK`` nimmt."""


def _alive(listener: logging.handlers.QueueListener) -> bool:
    """Lebt der Thread des Listeners? ``running`` des eingebauten (KTD1)."""
    thread: threading.Thread | None = getattr(listener, "_thread", None)
    return thread is not None and thread.is_alive()


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
        if not _alive(listener):
            with _CONFIG_LISTENER_LOCK:
                if not _alive(listener):
                    # Ein toter Thread zaehlt wie keiner (KTD1); der
                    # stdlib-``start`` wirft sonst. Die Queue bleibt.
                    setattr(listener, "_thread", None)  # noqa: B010
                    listener.start()
                    CONFIG_LISTENERS.append(listener)
        try:
            enqueue(record)
        except queue.Full:
            on_drop()
            drops.count += 1

    # Am Exemplar: die Klasse des Handlers gehoert der dictConfig der App.
    setattr(handler, "enqueue", enqueue_restarting)  # noqa: B010
