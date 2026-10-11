"""SwarmQ logging — structlog setup with non-blocking I/O."""

from __future__ import annotations

import contextlib
import json as jsonlib
import logging
import logging.config
import logging.handlers
import sys
from collections.abc import Generator
from typing import Any
from urllib.parse import urlparse, urlunparse

import structlog

from swarmq import logging_queue as _queue_core
from swarmq.exceptions import LoggingConfigError
from swarmq.logging_file import iter_loggers, start_queue_listeners
from swarmq.logging_file import load_log_config as load_log_config
from swarmq.logging_forksource import take_fork_source_sentinel
from swarmq.logging_format import build_formatter as build_formatter
from swarmq.logging_queue import LISTENER_THREAD_NAME as LISTENER_THREAD_NAME
from swarmq.logging_queue import (
    _activate_swarmq_queue_after_app_takeover,
    _LogQueue,
    _reset_log_queue,
    _stop_listeners,
)
from swarmq.logging_queue import _await_log_queue_drained as _await_log_queue_drained
from swarmq.logging_queue import _disable_log_queue as _disable_log_queue
from swarmq.logging_queue import _enable_log_queue as _enable_log_queue
from swarmq.logging_queue import _StructlogQueueHandler as _StructlogQueueHandler
from swarmq.logging_queue import _SwarmqQueueListener as _SwarmqQueueListener
from swarmq.logging_queue import dropped_log_records as dropped_log_records
from swarmq.logging_queue import shutdown_logging as shutdown_logging
from swarmq.logging_settings import LoggingSettings
from swarmq.logging_stdlib import handler_refs, module_lock

# Der Queue-Kern liegt in ``swarmq.logging_queue`` (Plan 2026-10-06-1811,
# U1); die Namen oben bleiben aus ``swarmq.logging`` importierbar. Die
# Prozessflags ``_STATE.configured`` und ``_STATE.app_takeover_reported``
# gehoeren dem Kern, weil ``shutdown_logging`` sie zuruecksetzt.


def root_handler_snapshot() -> tuple[logging.Handler, ...]:
    """Die Root-Handler vor dem App-Import (Regel "App gewinnt", KTD4)."""
    return tuple(logging.getLogger().handlers)


APP_LOGGING_REPORTS: dict[str, str] = {
    "logging_reconfigured_by_app": "info",
    "logging_handlers_added_by_app": "warning",
}
"""Meldungen ueber das Logging der App mit ihrem Level: im Worker
(:func:`react_to_app_logging`) und je Pool fuer die Compute-Kinder
(``swarmq.worker.compute.pool``)."""


def report_app_logging(log: Any, event: str, **fields: Any) -> None:
    """Meldet ``event`` aus :data:`APP_LOGGING_REPORTS` mit dessen Level."""
    getattr(log, APP_LOGGING_REPORTS[event])(event, **fields)


def react_to_app_logging(before: tuple[logging.Handler, ...]) -> None:
    """Regel "App gewinnt" (Plan 2026-10-06-0831, U7, KTD4).

    Verglichen wird mit ``before`` aus :func:`root_handler_snapshot`, nach dem
    App-Import und noch einmal nach einem ``--configure``-Hook. Fehlt ein
    Handler von swarmq, hat die App ersetzt: swarmq stoppt seinen
    Root-Listener und meldet ``logging_reconfigured_by_app`` einmal. Hat eine
    dictConfig der App ``swarmq.*``-Logger stummgeschaltet, schaltet swarmq
    sie wieder ein und nennt die Zahl im selben Event.
    """
    if not _queue_core._STATE.configured:
        # Nichts eingerichtet (R12): das Logging gehoert ganz der App.
        return
    replaced, added = _app_changes(before, tuple(logging.getLogger().handlers))
    reenabled = _reenable_swarmq_loggers()
    if (replaced or reenabled) and not _queue_core._STATE.app_takeover_reported:
        if replaced:
            _stop_listeners()
            _activate_swarmq_queue_after_app_takeover(**_queue_options(before))
        _queue_core._STATE.app_takeover_reported = True
        report_app_logging(
            get_logger(__name__),
            "logging_reconfigured_by_app",
            reenabled_loggers=reenabled,
        )
    if added and not replaced:
        # Neben swarmqs Handler: beide schreiben -- swarmq entfernt nichts.
        report_app_logging(
            get_logger(__name__),
            "logging_handlers_added_by_app",
            handler_types=[type(handler).__name__ for handler in added],
        )


def _queue_options(handlers: tuple[logging.Handler, ...]) -> dict[str, Any]:
    """Format und Queue-Groesse aus swarmqs ersetztem Root-Handler."""
    for handler in handlers:
        if isinstance(handler, _StructlogQueueHandler):
            return {"json": handler.json, "queue_size": handler.queue.maxsize}
    return {"json": False, "queue_size": LoggingSettings().queue_size}


def configure_child_logging(settings: LoggingSettings) -> list[tuple[str, dict[str, Any]]]:
    """Einstiegspunkt Compute-Kind (Plan 2026-10-06-0831, U7, KTD4, KTD14).

    Wie ``configure_logging``, aber mit der Regel "App gewinnt" des Workers
    (Plan 2026-10-06-1811, U9, KTD9): Verglichen wird mit dem Sentinel, den
    der Forkserver vor dem App-Import an Root gehaengt hat.

    - nur der Sentinel: swarmq richtet ein, eingebaut oder per ``log_config``;
    - zusaetzliche Handler daneben: swarmq richtet ein und behaelt sie;
    - Sentinel ersetzt: Die Handler der App bleiben, und die swarmq-Logs
      laufen wie im Worker an sie.

    Ohne Sentinel (eigenes Hauptmodul per Pfad, KTD6-Grenze) gewinnen
    vorgefundene Root-Handler wie bisher.

    Liefert die Meldungen ``(event, felder)``. Sie schreibt nicht das Kind,
    sondern der Pool, einmal je Pool statt einmal je Kind.
    """
    _queue_core.set_drain_timeout(settings.drain_timeout)
    sentinel, detached = take_fork_source_sentinel()
    root = logging.getLogger()
    if sentinel is not None:
        # Im per Fork entstandenen Kind hat der Fork-Hook den Sentinel schon
        # abgehaengt; hing er dort, zaehlt das wie seine Anwesenheit.
        current = (sentinel, *root.handlers) if detached else tuple(root.handlers)
        replaced, added = _app_changes((sentinel,), current)
        root.removeHandler(sentinel)
        if not replaced:
            return _configure_next_to_app_handlers(settings, added)
    if not settings.configure or not root_handler_snapshot():
        configure_logging(settings)
        return []
    _queue_core._STATE.configured = _queue_core._STATE.app_takeover_reported = True
    if settings.log_config is None:
        # Wie im Worker, der structlog vor dem App-Import einrichtet: Nutzer-
        # Tasks loggen per structlog an die Handler der App (R10, KTD6).
        _configure_structlog_for_user_code()
    # Ausdrueckliche Level je Logger gelten weiter; Root-Handler und
    # Root-Level gehoeren der App.
    _apply_named_levels(settings)
    # Geerbte, ungestartete Queue-Listener der App (Cookbook-Muster, Start
    # im ``--configure``-Hook, der nur im Worker laeuft): ohne Start gingen
    # alle Kindzeilen verloren (R13).
    _start_app_queue_listeners()
    _activate_swarmq_queue_after_app_takeover(json=settings.format == "json", queue_size=settings.queue_size)
    return [("logging_reconfigured_by_app", {"reenabled_loggers": _reenable_swarmq_loggers()})]


def _configure_next_to_app_handlers(
    settings: LoggingSettings, added: list[logging.Handler]
) -> list[tuple[str, dict[str, Any]]]:
    """Die App hat Root nicht ersetzt: swarmq richtet ein, und Handler, die
    sie daneben angehaengt hat, schreiben weiter mit -- wie im Worker."""
    root = logging.getLogger()
    for handler in added:
        root.removeHandler(handler)
    with _hidden_from_dictconfig(added):
        configure_logging(settings)
    for handler in added:
        root.addHandler(handler)
    if not settings.configure:
        return []
    # Ungestartete Queue-Listener der App-Handler (R13), wie bei der Uebernahme.
    _start_app_queue_listeners()
    reports: list[tuple[str, dict[str, Any]]] = []
    if reenabled := _reenable_swarmq_loggers():
        reports.append(("logging_reconfigured_by_app", {"reenabled_loggers": reenabled}))
    if added:
        reports.append(("logging_handlers_added_by_app", {"handler_types": [type(h).__name__ for h in added]}))
    return reports


@contextlib.contextmanager
def _hidden_from_dictconfig(handlers: list[logging.Handler]) -> Generator[None]:
    """Nimmt ``handlers`` voruebergehend aus ``logging._handlerList``
    (Schluss-Review): Eine ``dictConfig`` schliesst sonst jeden bekannten
    Handler (``_clearExistingHandlers``), auch die eben abgehaengten der App.
    Ein ``FileHandler(mode="w")`` oeffnete danach nie wieder, ein Handler mit
    Hintergrund-Thread waere gestoppt. Danach stehen sie wieder in der Liste,
    damit ``logging.shutdown`` sie bei Prozessende leert."""
    lock = module_lock()
    with lock:
        refs = handler_refs()
        hidden = [ref for ref in refs if ref() in handlers]
        refs[:] = [ref for ref in refs if ref not in hidden]
    try:
        yield
    finally:
        with lock:
            handler_refs().extend(hidden)


def _start_app_queue_listeners() -> None:
    """Startet geerbte, ungestartete Queue-Listener der App (R13); swarmqs
    eigene Listener starten weiter lazy beim ersten Eintrag (KTD5)."""
    start_queue_listeners(
        on_drop=_queue_core.count_dropped,
        skip=(_StructlogQueueHandler,),
        drain=_stop_listeners,
    )


def _app_changes(
    before: tuple[logging.Handler, ...], current: tuple[logging.Handler, ...]
) -> tuple[bool, list[logging.Handler]]:
    """Regel "App gewinnt" (KTD4, KTD9): Hat die App einen Root-Handler aus
    ``before`` ersetzt, und welche hat sie hinzugefuegt?"""
    replaced = any(handler not in current for handler in before)
    return replaced, [handler for handler in current if handler not in before]


def _reenable_swarmq_loggers() -> int:
    """Schaltet von der App deaktivierte ``swarmq.*``-Logger wieder ein."""
    count = 0
    for name, lg in iter_loggers():
        if lg.disabled and (name == "swarmq" or name.startswith("swarmq.")):
            lg.disabled = False
            count += 1
    return count


_RESERVED_RECORD_KEYS = frozenset(
    (
        *logging.LogRecord("", 0, "", 0, "", None, None).__dict__,
        "message",
        "asctime",
    )
) - frozenset(structlog.stdlib.LOG_KWARG_NAMES)
"""Felder, die ``Logger.makeRecord`` als ``extra`` ablehnt (``KeyError``).
``exc_info``/``stack_info``/``stacklevel`` sind ausgenommen: Als Aufruf-
kwargs gehen sie bestimmungsgemaess an die stdlib-Logmethode."""


def _guard_reserved_keys(_: Any, __: str, event_dict: structlog.typing.EventDict) -> structlog.typing.EventDict:
    """Benennt LogRecord-Attributnamen in ``swarmq_<key>`` um.

    Ohne das wirft ``log.info("evt", name=...)`` im stdlib-``makeRecord``
    einen ``KeyError`` statt zu loggen.
    """
    for key in _RESERVED_RECORD_KEYS.intersection(event_dict):
        event_dict[f"swarmq_{key}"] = event_dict.pop(key)
    return event_dict


def _merge_contextvars_guarded(
    logger: Any,  # noqa: ARG001 - structlog-Prozessor-Signatur vorgegeben
    method_name: str,  # noqa: ARG001 - structlog-Prozessor-Signatur vorgegeben
    event_dict: structlog.typing.EventDict,
) -> structlog.typing.EventDict:
    """``merge_contextvars`` mit Schutz der stdlib-Logkwargs.

    Eine Kontextvariable ``exc_info``/``stack_info``/``stacklevel`` ist ein
    Feld der App, kein Argument dieses Aufrufs: Sie wird als
    ``swarmq_<key>`` gemergt, sonst laege sie ``render_to_log_kwargs`` als
    kwarg an die stdlib-Methode. Felder des Aufrufs haben wie bei
    ``merge_contextvars`` Vorrang.
    """
    for key, value in structlog.contextvars.get_contextvars().items():
        name = f"swarmq_{key}" if key in structlog.stdlib.LOG_KWARG_NAMES else key
        event_dict.setdefault(name, value)
    return event_dict


_CALLER_STACKLEVEL = 4
"""Abstand von ``Logger._log`` zum Aufrufer: ``BoundLogger.<methode>`` ->
``BoundLogger._proxy_to_logger`` -> ``BoundLoggerBase._proxy_to_logger``
-> stdlib-Logmethode (ermittelt fuer structlog 26.1)."""


def _set_caller_stacklevel(logger: Any, __: str, event_dict: structlog.typing.EventDict) -> structlog.typing.EventDict:
    """Laesst ``funcName``/``lineno`` auf den Aufrufer zeigen statt auf
    structlogs Weiterleitung.

    Nur fuer Logger mit dem ``findCaller`` der stdlib: Hat die App per
    ``structlog.stdlib.LoggerFactory`` die Logger-Klasse umgestellt,
    ueberspringt deren ``findCaller`` die structlog-Frames schon selbst,
    und ein zusaetzlicher ``stacklevel`` schoesse ueber den Aufrufer hinaus.
    """
    if getattr(logger.findCaller, "__func__", None) is logging.Logger.findCaller:
        event_dict["stacklevel"] = _CALLER_STACKLEVEL
    return event_dict


LOGGER_PROCESSORS: list[Any] = [
    structlog.stdlib.filter_by_level,
    _merge_contextvars_guarded,
    _guard_reserved_keys,
    _set_caller_stacklevel,
    structlog.stdlib.render_to_log_kwargs,
]
"""Aufruferseitige Prozessorkette der Bibliotheks-Logger (Plan
2026-10-06-0831, KTD2). Nur billige Schritte; gerendert wird erst im
Handler der App bzw. im Listener-Thread."""


class _LazyStdlibLogger:
    """Holt den stdlib-Logger erst beim ersten Logaufruf (KTD1).

    Beim Modulimport angelegt, bliebe der Logger ein "existing logger", den
    eine dictConfig der App mit dem Default ``disable_existing_loggers=True``
    stumm schaltet. Attributzugriffe (``isEnabledFor``, ``info`` ...) gehen
    an den echten Logger; der liefert gebundene Methoden, also entsteht
    kein zusaetzlicher Stack-Frame.
    """

    __slots__ = ("_logger", "_name")

    def __init__(self, name: str) -> None:
        self._name = name
        self._logger: logging.Logger | None = None

    def __getattr__(self, attr: str) -> Any:
        logger = self._logger
        if logger is None:
            logger = self._logger = logging.getLogger(self._name)
        return getattr(logger, attr)


def get_logger(name: str) -> Any:
    """Liefert den Bibliotheks-Logger ``name`` (Plan 2026-10-06-0831, U1).

    KTD1: ``structlog.wrap_logger`` mit expliziten ``processors`` und
    ``wrapper_class`` -- ein ``structlog.configure`` der App greift damit
    nicht. Uebergeben wird ans Standard-``logging``: Der Event-Name wird
    zur Nachricht, alle Felder werden zu LogRecord-Attributen (R2, R3).
    """
    return structlog.wrap_logger(
        _LazyStdlibLogger(name),
        processors=LOGGER_PROCESSORS,
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def _error_fields(exc: BaseException) -> dict[str, str]:
    """Einheitliches Fehlerschema fuer Logeintraege (R17, KTD11):
    ``error_type`` ist der Klassenname, ``error`` der Text -- bei leerer
    Nachricht (``TimeoutError()``) ebenfalls der Klassenname."""
    error_type = type(exc).__name__
    return {"error_type": error_type, "error": str(exc) or error_type}


def _task_log_context(**fields: Any) -> contextlib.AbstractContextManager[None]:
    """Bindet den Task-Kontext fuer einen Block (Plan 2026-10-06-0831, KTD13).

    Beim Verlassen werden die vorigen Werte wiederhergestellt; Variablen
    der App bleiben unberuehrt. ``None``-Werte entfallen: ein fehlendes
    ``workflow_id`` fehlt im Eintrag, statt als ``None`` zu erscheinen.
    """
    return structlog.contextvars.bound_contextvars(**{key: value for key, value in fields.items() if value is not None})


def configure_logging(settings: LoggingSettings) -> None:
    """Richtet das Logging eines swarmq-Prozesses ein (Plan 2026-10-06-0831,
    U6, KTD4).

    Nur fuer Einstiegspunkte: Supervisor, Worker-Subprozess, Compute-Kind.
    Die Bibliothek selbst ruft das nie auf. Drei Modi:

    - eingebaut: Root schreibt ueber Queue und Listener nach stderr, und
      ``structlog.get_logger()`` im Nutzercode rendert genauso (R9, R10);
    - Datei (``log_config``): die dictConfig der App (R11, KTD9);
    - aus (``configure=False``): nichts (R12).

    Zuletzt gelten die Level je Logger-Name. Ein zweiter Aufruf im selben
    Prozess ist ein No-op mit DEBUG-Event; neu aufgebaut wird nur ueber
    ``shutdown_logging()`` und danach ``configure_logging()``.

    Raises:
        LoggingConfigError: Die Konfigurationsdatei ist nicht ladbar oder
            ungueltig; ``path`` nennt sie, ``__cause__`` die Ursache.
    """
    # Auch ohne Einrichtung: Die Prozesse gehoeren swarmq, ihr Leeren ist
    # begrenzt (Plan 2026-10-06-1811, KTD3/KTD11).
    _queue_core.set_drain_timeout(settings.drain_timeout)
    if not settings.configure:
        # "Logging nicht konfigurieren" (R12): die App ist zustaendig.
        return
    if _queue_core._STATE.configured:
        get_logger(__name__).debug("logging_already_configured")
        return
    # KTD10: Eingebaut und per Datei ist der Opt-in wirkungslos. Ein bereits
    # aktiver -- etwa aus dem Vorladen im Forkserver geerbt -- wird
    # aufgehoben, ``swarmq`` wiederhergestellt.
    _reset_log_queue()
    if settings.log_config is not None:
        _configure_from_file(settings)
    else:
        _configure_builtin(settings)
    _apply_logger_levels(settings)
    _queue_core._STATE.configured = True


def _configure_from_file(settings: LoggingSettings) -> None:
    """Datei-Modus: die dictConfig der App statt der eingebauten Einrichtung
    (R11). structlog bleibt unkonfiguriert (KTD6)."""
    assert settings.log_config is not None
    # Vom Supervisor geprueft und weitergegeben (KTD9): Kinder lesen die
    # Datei nie.
    raw = settings.log_config_json or load_log_config(settings.log_config)
    try:
        logging.config.dictConfig(jsonlib.loads(raw))
    except Exception as exc:
        # dictConfig meldet Tippfehler als ValueError, TypeError,
        # AttributeError oder ImportError -- alle mit Pfad nach oben (AE4).
        raise LoggingConfigError(settings.log_config, exc) from exc
    start_queue_listeners(on_drop=_queue_core.count_dropped, drain=_stop_listeners)


def _apply_logger_levels(settings: LoggingSettings) -> None:
    """Level je Logger-Name (``--logger-level``) -- zuletzt, damit sie auch
    nach einer dictConfig gelten (KTD8). Ein ausdrueckliches Level gilt dort
    auch fuer Root; ein Default ueberschriebe das Level aus der Datei."""
    if settings.log_config is not None and settings.level_explicit:
        logging.getLogger().setLevel(settings.level)
    _apply_named_levels(settings)


def _apply_named_levels(settings: LoggingSettings) -> None:
    for name, level in settings.logger_levels:
        logging.getLogger(name).setLevel(level)


def _configure_builtin(settings: LoggingSettings) -> None:
    """Eingebauter Modus: Root schreibt ueber Queue und Listener nach stderr."""
    log_json = settings.format == "json"
    _configure_structlog_for_user_code()

    # Non-blocking I/O via QueueHandler + QueueListener
    log_queue = _LogQueue(maxsize=settings.queue_size)

    # Plan 2026-04-23-001 CLI-03: bind ``colors`` to the TTY state of
    # stderr at configure time. Keeps ANSI escapes out of piped /
    # captured stderr (log aggregators, CI output, pytest fixtures).
    # The decision is static — callers that redirect stderr AFTER
    # configuring must rebuild.
    formatter = build_formatter(log_json, sys.stderr.isatty())

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)

    # Der Listener startet lazy beim ersten Eintrag (KTD5).
    queue_handler = _StructlogQueueHandler(log_queue, stream_handler, json=log_json)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(settings.level)


def _configure_structlog_for_user_code() -> None:
    """``structlog.get_logger()`` in Nutzer-Tasks rendert ueber dieselbe
    Ausgabe wie swarmq (R10, KTD6).

    Dieselbe schlanke Kette wie die Bibliotheks-Logger: uebergeben wird ans
    Standard-``logging``, gerendert erst im Listener-Thread. Eine bereits
    bestehende Konfiguration -- etwa einer im Forkserver vorgeladenen App --
    bleibt unangetastet.
    """
    if structlog.is_configured():
        return
    structlog.configure(
        processors=LOGGER_PROCESSORS,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def setup_logging(log_level: str = "INFO", log_json: bool = True) -> logging.handlers.QueueListener:
    """Deprecated: Einstiegspunkte rufen ``configure_logging`` auf (U6).

    Baut bei jedem Aufruf neu auf -- ``shutdown_logging()`` und danach
    ``configure_logging()`` im eingebauten Modus -- und liefert den Listener.
    Ein zweiter Aufruf stoppt damit den Listener des ersten (B6: doppelte
    Logzeilen bei zwei Listenern). Wie bisher wird structlog dabei immer neu
    konfiguriert, auch ueber eine bestehende Konfiguration hinweg.
    """
    shutdown_logging()
    structlog.reset_defaults()
    configure_logging(LoggingSettings(level=log_level, format="json" if log_json else "human"))
    handler = logging.getLogger().handlers[0]
    assert isinstance(handler, _StructlogQueueHandler)
    assert handler.listener is not None
    return handler.listener


flush_logging = shutdown_logging
"""Deprecated: Alias fuer ``shutdown_logging`` (Plan 2026-10-06-0831, U3).

Vor einem ``SystemExit`` noetig: die Logzeile liegt bis dahin nur in der
Queue, und der Listener-Thread kommt nicht mehr dazu, sie zu schreiben."""


def sanitize_url(url: str) -> str:
    """Replace passwords in URLs with '***'."""
    parsed = urlparse(url)
    if parsed.password:
        # Replace password in netloc
        replaced = parsed._replace(netloc=parsed.netloc.replace(f":{parsed.password}@", ":***@", 1))
        return urlunparse(replaced)
    return url
