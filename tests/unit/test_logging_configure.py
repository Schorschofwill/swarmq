"""Tests fuer ``swarmq.logging.configure_logging`` -- die Einrichtung des
Loggings in den Einstiegspunkten.

Plan 2026-10-06-0831, U6 (R9, R10, R11, R12, R14; KTD4, KTD6, KTD9).
"""

from __future__ import annotations

# Test list:
# 1. Eingebaut mit JSON: genau ein Root-Handler, genau ein Listener-Thread,
#    ein swarmq-Event ergibt genau eine JSON-Zeile (Regression B6).
# 2. Eingebaut: ``structlog.get_logger("user").info("x", a=1)`` erscheint im
#    selben Format auf stderr (R10).
# 3. Eingebaut: eine bereits bestehende structlog-Konfiguration bleibt
#    erhalten (KTD6).
# 4. Eingebaut: das Root-Level folgt ``settings.level``.
# 5. Ein zweiter Aufruf ist ein No-op mit DEBUG-Event und erhoeht die Zahl
#    der Listener-Threads nicht (KTD4).
# 6. Nach ``shutdown_logging()`` baut ``configure_logging`` neu auf.
# 7. Die Queue-Groesse kommt aus ``settings.queue_size``.
# 8. Textmodus auf einen Nicht-TTY-Stream: kein ANSI (Regression CLI-03).
# 9. Aus: Root-Handler und ``structlog.get_config()`` sind unveraendert.
# 10. Logger-Level-Overrides setzen das Level des genannten Loggers.
# 11. Datei als JSON: Handler aus der Datei sind aktiv, ``swarmq.*``-Logger
#     sind nicht deaktiviert, obwohl ``disable_existing_loggers`` fehlt.
# 12. Datei mit ausdruecklich ``disable_existing_loggers: true``: respektiert.
# 13. Datei mit ``QueueHandler`` samt ``listener``: der Listener laeuft danach,
#     die Eintraege erscheinen; ``shutdown_logging`` stoppt ihn.
# 14. Logger-Level-Overrides wirken auch nach einer dictConfig.
# 15. ``log_config_json`` hat Vorrang: die Datei wird nicht gelesen.
# 16. YAML ohne PyYAML: ``LoggingConfigError`` mit Installationshinweis.
#     Mit PyYAML wird sie geladen.
# 17. Datei fehlt / ungueltiges JSON / kein Objekt: ``LoggingConfigError`` mit
#     ``path`` und Ursache in ``__cause__`` (AE4); ein ``ConfigurationError``.
# 18. Handler-Tippfehler: ``configure_logging`` wirft ``LoggingConfigError``
#     mit ``path`` und ``__cause__`` (AE4).
# 19. ``load_log_config`` liefert die gepruefte Konfiguration als JSON-String
#     mit ``disable_existing_loggers: false`` (abgedeckt ueber 16).
# 20. ``setup_logging`` baut bei jedem Aufruf neu auf und uebernimmt den
#     Prozess (deprecated Wrapper).
# 21. Datei-Modus laesst structlog unkonfiguriert (KTD6).
# 22. ``setup_logging`` konfiguriert structlog wie bisher in jedem Fall neu,
#     auch ueber eine bestehende Konfiguration hinweg.
# 23. (U7, KTD8) Datei-Modus: ein ausdrueckliches Level gilt nach der
#     dictConfig fuer Root; ohne ausdrueckliches Level bleibt das der Datei.
# 24. (U7) entfallen mit U8: ``logging_is_configured()`` war die Bruecke fuer
#     ``SwarmQ``, das seit U8 nie mehr einrichtet.
# 25. (U7, KTD5/KTD14) Datei-Modus mit ``QueueHandler``: nach
#     ``shutdown_logging`` startet der naechste Eintrag den Listener wieder,
#     das folgende ``shutdown_logging`` leert ihn -- die Leerung pro Job im
#     wiederverwendeten Compute-Kind.
# 26. (KTD5) Datei-Modus mit ``QueueHandler``: Im echten Kind gehoert der
#     beim Fork laufende Listener dem Elternprozess; ``shutdown_logging``
#     stoppt ihn dort nicht und wirft nicht, auch bei voller Queue-Kopie.
# 27. (KTD5) Datei-Modus mit ``QueueHandler``: ``shutdown_logging`` auf einer
#     vollen Queue wirft kein ``queue.Full`` (stdlib-Sentinel per
#     ``put_nowait``), sondern wartet, bis der Listener Platz schafft, und
#     schreibt alle Eintraege.
# Plan 2026-10-06-1811, U2 (R2, KTD1) -- Datei-Modus-Listener sterben nie:
# 28. dictConfig-Queue mit werfendem Filter am Ziel-Handler: Der Listener
#     lebt weiter, der naechste Record kommt an, ``log_handler_errors`` geht
#     ueber die Handler des Listeners.
# 29. Ein toter Datei-Modus-Thread (beendet, ohne dass der Listener es merkt)
#     wird beim naechsten Record neu gestartet; der Record kommt an.
# 30. Volle begrenzte dictConfig-Queue: Der Drop wird gezaehlt, kein
#     Traceback auf stderr (kein ``handleError`` im Aufrufer).
# 31. Ein ``shutdown_logging``, waehrend dessen ein Eintrag den beendeten
#     Datei-Modus-Thread durch einen neuen ersetzt, laesst den neuen nicht
#     verwaist weiterlaufen.
# Plan 2026-10-06-1811, U3 (R1, KTD2) -- Drop-Meldung im Datei-Modus-Listener:
# 32. Volle begrenzte dictConfig-Queue: Nach dem Freigeben kommt genau eine
#     Meldung ``log_records_dropped`` ueber die Handler des Listeners.
# 33. Beim Stoppen meldet der Datei-Modus-Listener noch nicht gemeldete
#     Drops, auch innerhalb der Frist (Zeit gemockt).
import io
import json
import logging
import logging.handlers
import pathlib
import queue
import sys
import threading
import types
import weakref
from collections.abc import Iterator
from typing import Any

import pytest
import structlog

from swarmq.logging import (
    LISTENER_THREAD_NAME,
    configure_logging,
    get_logger,
    load_log_config,
)
from swarmq.logging_settings import LoggingSettings


def _listener_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == LISTENER_THREAD_NAME]


FILE_RECORDS: list[logging.LogRecord] = []
"""Ziel des ``RecordingHandler`` aus den dictConfig-Dateien der Tests."""


class RecordingHandler(logging.Handler):
    """Per dictConfig-Datei (``"()"``) referenzierbarer Sammel-Handler."""

    def emit(self, record: logging.LogRecord) -> None:
        FILE_RECORDS.append(record)


_RECORDING = f"{__name__}.RecordingHandler"

FILE_HANDLER_ENTERED = threading.Event()
FILE_HANDLER_RELEASE = threading.Event()


class BlockingRecordingHandler(RecordingHandler):
    """Haelt den Listener der dictConfig beim ersten Eintrag fest, bis
    ``FILE_HANDLER_RELEASE`` gesetzt ist."""

    def emit(self, record: logging.LogRecord) -> None:
        if not FILE_HANDLER_ENTERED.is_set():
            FILE_HANDLER_ENTERED.set()
            FILE_HANDLER_RELEASE.wait()
        super().emit(record)


_BLOCKING_RECORDING = f"{__name__}.BlockingRecordingHandler"


class PoisonFilter(logging.Filter):
    """Filter am Ziel-Handler, der beim Record ``evt_poison`` wirft -- etwa
    weil eine Kontextvariable fehlt (Analyse S2)."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.getMessage() == "evt_poison":
            raise KeyError("correlation_id")
        return True


def _queue_config(**queue_handler: object) -> dict[str, object]:
    """dictConfig mit ``QueueHandler`` an Root vor einem ``RecordingHandler``
    hinter einem ``PoisonFilter``."""
    return {
        "version": 1,
        "filters": {"poison": {"()": f"{__name__}.PoisonFilter"}},
        "handlers": {
            "rec": {"()": _RECORDING, "filters": ["poison"]},
            "q": {
                "class": "logging.handlers.QueueHandler",
                "handlers": ["rec"],
                **queue_handler,
            },
        },
        "root": {"level": "INFO", "handlers": ["q"]},
    }


class ReleasingQueue(queue.Queue[object]):
    """Gibt den blockierten Listener frei, sobald blockierend eingereiht
    wird -- ``put_nowait`` (``block=False``) gibt nicht frei."""

    def put(self, item: object, block: bool = True, timeout: float | None = None) -> None:
        if block:
            FILE_HANDLER_RELEASE.set()
        super().put(item, block, timeout)


def _config_file(tmp_path: pathlib.Path, config: dict[str, object], name: str = "log.json") -> str:
    path = tmp_path / name
    path.write_text(json.dumps(config))
    return str(path)


class _Stderr(io.StringIO):
    def __init__(self, tty: bool = False) -> None:
        super().__init__()
        self._tty = tty

    def isatty(self) -> bool:
        return self._tty


@pytest.fixture(autouse=True)
def isolated_logging(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Globaler Logging- und structlog-Zustand ohne Nebenwirkung.

    ``dictConfig`` schliesst alle bekannten Handler und deaktiviert
    vorhandene Logger: Die Handler-Registry wird fuer den Test geleert,
    ``disabled``-Flags und Level werden danach zurueckgesetzt. Root und
    ``shutdown_logging`` uebernimmt das Autouse-Fixture der Unit-Tests. Die
    structlog-Konfiguration startet leer und wird wiederhergestellt.
    """
    monkeypatch.setattr(logging, "_handlerList", [])
    monkeypatch.setattr(logging, "_handlers", weakref.WeakValueDictionary())
    loggers = logging.Logger.manager.loggerDict
    saved = {
        name: (lg.disabled, lg.level)
        for name, lg in loggers.items()
        if isinstance(lg, logging.Logger)
    }
    FILE_RECORDS.clear()
    logger_class = logging.getLoggerClass()
    structlog_config = structlog.get_config()
    structlog_configured = structlog.is_configured()
    structlog.reset_defaults()
    yield
    for name, lg in list(loggers.items()):
        if isinstance(lg, logging.Logger):
            disabled, level = saved.get(name, (False, logging.NOTSET))
            lg.disabled = disabled
            lg.setLevel(level)
    logging.setLoggerClass(logger_class)
    structlog.reset_defaults()
    if structlog_configured:
        structlog.configure(**structlog_config)


def _stderr(monkeypatch: pytest.MonkeyPatch, tty: bool = False) -> _Stderr:
    """Ersetzt ``sys.stderr`` im Testkoerper -- ein Fixture waere zu frueh:
    pytest setzt seine Capture erst zur Call-Phase ein."""
    stream = _Stderr(tty)
    monkeypatch.setattr(sys, "stderr", stream)
    return stream


def _lines(stream: io.StringIO) -> list[str]:
    from swarmq.logging import shutdown_logging

    shutdown_logging()
    return stream.getvalue().splitlines()


def test_builtin_json_has_one_root_handler_one_listener_one_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stderr = _stderr(monkeypatch)
    configure_logging(LoggingSettings(format="json"))

    get_logger("swarmq.tests.configure").info("evt_one", a=1)

    assert len(logging.getLogger().handlers) == 1
    assert len(_listener_threads()) == 1
    (line,) = _lines(stderr)
    data = json.loads(line)
    assert data["event"] == "evt_one"
    assert data["a"] == 1


def test_builtin_renders_user_structlog_events_like_swarmq_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stderr = _stderr(monkeypatch)
    configure_logging(LoggingSettings(format="json"))

    structlog.get_logger("user").info("x", a=1)
    get_logger("swarmq.tests.configure").info("y", a=1)

    user, lib = (json.loads(line) for line in _lines(stderr))
    assert user.keys() == lib.keys()
    assert (user["event"], user["logger"], user["level"], user["a"]) == (
        "x", "user", "info", 1,
    )


def test_builtin_keeps_an_existing_structlog_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stderr(monkeypatch)
    app_processors = [structlog.processors.KeyValueRenderer()]
    structlog.configure(processors=app_processors)

    configure_logging(LoggingSettings())

    assert structlog.get_config()["processors"] == app_processors


def test_builtin_sets_the_root_level(monkeypatch: pytest.MonkeyPatch) -> None:
    _stderr(monkeypatch)

    configure_logging(LoggingSettings(level="WARNING"))

    assert logging.getLogger().level == logging.WARNING


def test_second_call_is_a_noop_with_debug_event(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, object]]
) -> None:
    _stderr(monkeypatch)
    configure_logging(LoggingSettings(format="json"))
    first = logging.getLogger().handlers[:]
    get_logger("swarmq.tests.configure").warning("evt")

    configure_logging(LoggingSettings(format="human", level="DEBUG"))
    get_logger("swarmq.tests.configure").warning("evt")

    assert logging.getLogger().handlers == first
    assert logging.getLogger().level == logging.INFO
    assert len(_listener_threads()) == 1
    assert [e["log_level"] for e in swarmq_logs if e["event"] == "logging_already_configured"] == ["debug"]


def test_configure_after_shutdown_rebuilds(monkeypatch: pytest.MonkeyPatch) -> None:
    from swarmq.logging import shutdown_logging

    stderr = _stderr(monkeypatch)
    configure_logging(LoggingSettings(format="json"))
    first = logging.getLogger().handlers[:]

    shutdown_logging()
    configure_logging(LoggingSettings(format="human"))
    get_logger("swarmq.tests.configure").warning("evt_rebuilt")

    assert logging.getLogger().handlers != first
    assert len(logging.getLogger().handlers) == 1
    (line,) = _lines(stderr)
    assert "evt_rebuilt" in line
    assert not line.startswith("{")


def test_builtin_queue_size_comes_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    _stderr(monkeypatch)

    configure_logging(LoggingSettings(queue_size=5))

    (handler,) = logging.getLogger().handlers
    assert isinstance(handler, logging.handlers.QueueHandler)
    assert handler.queue.maxsize == 5  # type: ignore[attr-defined]


@pytest.mark.parametrize(("tty", "ansi"), [(False, False), (True, True)])
def test_text_mode_uses_ansi_only_on_a_tty(
    monkeypatch: pytest.MonkeyPatch, tty: bool, ansi: bool
) -> None:
    stderr = _stderr(monkeypatch, tty=tty)
    configure_logging(LoggingSettings(format="human"))

    get_logger("swarmq.tests.configure").warning("evt_text", a=1)

    (line,) = _lines(stderr)
    assert "evt_text" in line
    assert ("\x1b[" in line) is ansi


def test_configure_false_changes_nothing() -> None:
    app_handler = logging.NullHandler()
    logging.getLogger().handlers[:] = [app_handler]
    before = structlog.get_config()

    configure_logging(LoggingSettings(configure=False))

    assert logging.getLogger().handlers == [app_handler]
    assert structlog.get_config() == before
    assert not structlog.is_configured()


def test_logger_level_overrides_set_the_named_logger_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stderr(monkeypatch)

    configure_logging(
        LoggingSettings(
            logger_levels=(("swarmq.tests.noisy", "ERROR"), ("swarmq.tests.chatty", "DEBUG"))
        )
    )

    assert logging.getLogger("swarmq.tests.noisy").level == logging.ERROR
    assert logging.getLogger("swarmq.tests.chatty").level == logging.DEBUG


def test_file_mode_applies_handlers_and_keeps_existing_loggers(
    tmp_path: pathlib.Path,
) -> None:
    existing = logging.getLogger("swarmq.tests.existing")
    path = _config_file(
        tmp_path,
        {
            "version": 1,
            "handlers": {"rec": {"()": _RECORDING}},
            "root": {"level": "INFO", "handlers": ["rec"]},
        },
    )

    configure_logging(LoggingSettings(log_config=path))
    existing.warning("evt_file")

    assert not existing.disabled
    assert [r.getMessage() for r in FILE_RECORDS] == ["evt_file"]


def test_file_mode_respects_explicit_disable_existing_loggers(
    tmp_path: pathlib.Path,
) -> None:
    existing = logging.getLogger("swarmq.tests.existing")
    path = _config_file(
        tmp_path,
        {
            "version": 1,
            "disable_existing_loggers": True,
            "handlers": {"rec": {"()": _RECORDING}},
            "root": {"level": "INFO", "handlers": ["rec"]},
        },
    )

    configure_logging(LoggingSettings(log_config=path))

    assert existing.disabled


def test_file_mode_starts_queue_handler_listeners_and_shutdown_stops_them(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.logging import shutdown_logging

    path = _config_file(
        tmp_path,
        {
            "version": 1,
            "handlers": {
                "rec": {"()": _RECORDING},
                "q": {"class": "logging.handlers.QueueHandler", "handlers": ["rec"]},
            },
            "root": {"level": "INFO", "handlers": ["q"]},
        },
    )

    configure_logging(LoggingSettings(log_config=path))
    logging.getLogger("swarmq.tests.queued").warning("evt_queued")
    (handler,) = logging.getLogger().handlers
    listener = handler.listener  # type: ignore[attr-defined]
    shutdown_logging()

    assert [r.getMessage() for r in FILE_RECORDS] == ["evt_queued"]
    assert listener._thread is None


def test_logger_level_overrides_win_over_the_config_file(
    tmp_path: pathlib.Path,
) -> None:
    path = _config_file(
        tmp_path,
        {
            "version": 1,
            "loggers": {"swarmq.tests.noisy": {"level": "DEBUG"}},
        },
    )

    configure_logging(
        LoggingSettings(log_config=path, logger_levels=(("swarmq.tests.noisy", "ERROR"),))
    )

    assert logging.getLogger("swarmq.tests.noisy").level == logging.ERROR


def test_file_mode_uses_the_preloaded_json_without_reading_the_file(
    tmp_path: pathlib.Path,
) -> None:
    preloaded = json.dumps(
        {
            "version": 1,
            "handlers": {"rec": {"()": _RECORDING}},
            "root": {"level": "INFO", "handlers": ["rec"]},
        }
    )

    configure_logging(
        LoggingSettings(log_config=str(tmp_path / "gone.json"), log_config_json=preloaded)
    )
    logging.getLogger("swarmq.tests.preloaded").warning("evt_preloaded")

    assert [r.getMessage() for r in FILE_RECORDS] == ["evt_preloaded"]


def test_missing_file_raises_logging_config_error_with_path_and_cause(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.exceptions import ConfigurationError, LoggingConfigError

    path = str(tmp_path / "missing.json")

    with pytest.raises(LoggingConfigError) as excinfo:
        load_log_config(path)

    assert isinstance(excinfo.value, ConfigurationError)
    assert excinfo.value.path == path
    assert isinstance(excinfo.value.__cause__, FileNotFoundError)
    assert path in str(excinfo.value)


def test_invalid_json_raises_logging_config_error_with_cause(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.exceptions import LoggingConfigError

    path = tmp_path / "bad.json"
    path.write_text('{"version": 1,')

    with pytest.raises(LoggingConfigError) as excinfo:
        load_log_config(str(path))

    assert excinfo.value.path == str(path)
    assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)


def test_non_object_config_raises_logging_config_error(tmp_path: pathlib.Path) -> None:
    from swarmq.exceptions import LoggingConfigError

    path = tmp_path / "list.json"
    path.write_text("[1, 2]")

    with pytest.raises(LoggingConfigError) as excinfo:
        load_log_config(str(path))

    assert isinstance(excinfo.value.__cause__, TypeError)


def test_handler_typo_raises_logging_config_error_with_path_and_cause(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.exceptions import LoggingConfigError

    path = _config_file(
        tmp_path,
        {
            "version": 1,
            "handlers": {"err": {"class": "logging.StreamHandlr"}},
            "root": {"handlers": ["err"]},
        },
    )

    with pytest.raises(LoggingConfigError) as excinfo:
        configure_logging(LoggingSettings(log_config=path))

    assert excinfo.value.path == path
    assert isinstance(excinfo.value.__cause__, ValueError)
    assert "err" in str(excinfo.value)


def test_yaml_without_pyyaml_raises_with_install_hint(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from swarmq.exceptions import LoggingConfigError

    path = tmp_path / "log.yaml"
    path.write_text("version: 1\n")
    monkeypatch.setitem(sys.modules, "yaml", None)  # import yaml -> ImportError

    with pytest.raises(LoggingConfigError) as excinfo:
        load_log_config(str(path))

    assert isinstance(excinfo.value.__cause__, ImportError)
    assert "pip install pyyaml" in str(excinfo.value).lower()


def test_yaml_is_loaded_with_pyyaml(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # PyYAML ist keine Abhaengigkeit von swarmq; ein Ersatzmodul haelt den
    # Test unabhaengig davon, ob es zufaellig installiert ist.
    fake_yaml = types.ModuleType("yaml")
    fake_yaml.safe_load = lambda text: {"version": 1, "root": {"level": text.strip()}}  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "yaml", fake_yaml)
    path = tmp_path / "log.yml"
    path.write_text("WARNING\n")

    config = json.loads(load_log_config(str(path)))

    assert config == {
        "version": 1,
        "root": {"level": "WARNING"},
        "disable_existing_loggers": False,
    }


def test_setup_logging_rebuilds_and_owns_the_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swarmq.logging import setup_logging

    _stderr(monkeypatch)
    first = setup_logging("INFO", True)
    second = setup_logging("WARNING", False)

    configure_logging(LoggingSettings(level="DEBUG"))

    assert first is not second
    assert logging.getLogger().handlers[0].listener is second  # type: ignore[attr-defined]
    assert logging.getLogger().level == logging.WARNING


def test_file_mode_leaves_structlog_unconfigured(tmp_path: pathlib.Path) -> None:
    path = _config_file(tmp_path, {"version": 1})

    configure_logging(LoggingSettings(log_config=path))

    assert not structlog.is_configured()


def test_setup_logging_still_reconfigures_structlog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swarmq.logging import LOGGER_PROCESSORS, setup_logging

    _stderr(monkeypatch)
    structlog.configure(processors=[structlog.processors.KeyValueRenderer()])

    setup_logging("INFO", True)

    assert structlog.get_config()["processors"] == LOGGER_PROCESSORS


def test_file_mode_applies_an_explicit_level_to_root(tmp_path: pathlib.Path) -> None:
    path = _config_file(tmp_path, {"version": 1, "root": {"level": "DEBUG"}})

    configure_logging(LoggingSettings(log_config=path, level="ERROR", level_explicit=True))

    assert logging.getLogger().level == logging.ERROR


def test_file_mode_keeps_the_file_level_without_an_explicit_level(
    tmp_path: pathlib.Path,
) -> None:
    path = _config_file(tmp_path, {"version": 1, "root": {"level": "DEBUG"}})

    configure_logging(LoggingSettings(log_config=path, level="INFO"))

    assert logging.getLogger().level == logging.DEBUG


def test_file_mode_queue_listener_restarts_after_shutdown_for_the_next_job(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.logging import shutdown_logging

    path = _config_file(
        tmp_path,
        {
            "version": 1,
            "handlers": {
                "rec": {"()": _RECORDING},
                "q": {"class": "logging.handlers.QueueHandler", "handlers": ["rec"]},
            },
            "root": {"level": "INFO", "handlers": ["q"]},
        },
    )
    configure_logging(LoggingSettings(log_config=path))

    logging.getLogger("swarmq.tests.queued").warning("evt_job_1")
    shutdown_logging()
    logging.getLogger("swarmq.tests.queued").warning("evt_job_2")
    shutdown_logging()

    assert [r.getMessage() for r in FILE_RECORDS] == ["evt_job_1", "evt_job_2"]


def test_child_shutdown_skips_the_file_mode_listener_inherited_from_the_parent(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.logging import shutdown_logging
    from tests.unit._fork_child import run_in_forked_child

    FILE_HANDLER_ENTERED.clear()
    FILE_HANDLER_RELEASE.clear()
    path = _config_file(
        tmp_path,
        {
            "version": 1,
            "handlers": {
                "rec": {"()": _BLOCKING_RECORDING},
                "q": {
                    "class": "logging.handlers.QueueHandler",
                    "handlers": ["rec"],
                    "queue": {"()": "queue.Queue", "maxsize": 1},
                },
            },
            "root": {"level": "INFO", "handlers": ["q"]},
        },
    )
    configure_logging(LoggingSettings(log_config=path))
    log = logging.getLogger("swarmq.tests.queued")
    try:
        log.warning("evt_parent_busy")
        assert FILE_HANDLER_ENTERED.wait(5)
        log.warning("evt_inherited")
        (handler,) = logging.getLogger().handlers
        assert handler.queue.full()  # type: ignore[attr-defined]

        assert run_in_forked_child(shutdown_logging) == 0
    finally:
        FILE_HANDLER_RELEASE.set()


def test_shutdown_waits_for_room_in_a_full_file_mode_queue_instead_of_raising(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.logging import shutdown_logging

    FILE_HANDLER_ENTERED.clear()
    FILE_HANDLER_RELEASE.clear()
    FILE_RECORDS.clear()
    path = _config_file(
        tmp_path,
        {
            "version": 1,
            "handlers": {
                "rec": {"()": _BLOCKING_RECORDING},
                "q": {
                    "class": "logging.handlers.QueueHandler",
                    "handlers": ["rec"],
                    "queue": {"()": f"{__name__}.ReleasingQueue", "maxsize": 1},
                },
            },
            "root": {"level": "INFO", "handlers": ["q"]},
        },
    )
    configure_logging(LoggingSettings(log_config=path))
    log = logging.getLogger("swarmq.tests.queued")
    try:
        log.warning("evt_busy")
        assert FILE_HANDLER_ENTERED.wait(5)
        log.warning("evt_queued")
        (handler,) = logging.getLogger().handlers
        assert handler.queue.full()  # type: ignore[attr-defined]

        shutdown_logging()
    finally:
        FILE_HANDLER_RELEASE.set()

    assert [r.getMessage() for r in FILE_RECORDS] == ["evt_busy", "evt_queued"]


def test_a_raising_filter_does_not_kill_the_file_mode_listener(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.logging import shutdown_logging

    configure_logging(LoggingSettings(log_config=_config_file(tmp_path, _queue_config())))
    log = logging.getLogger("swarmq.tests.queued")

    log.warning("evt_poison")
    log.warning("evt_after")
    shutdown_logging()

    assert [r.getMessage() for r in FILE_RECORDS] == ["log_handler_errors", "evt_after"]


def test_a_dead_file_mode_listener_thread_restarts_on_the_next_record(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.logging import shutdown_logging

    configure_logging(LoggingSettings(log_config=_config_file(tmp_path, _queue_config())))
    (handler,) = logging.getLogger().handlers
    listener = handler.listener  # type: ignore[attr-defined]
    # Den Thread beenden, ohne dass der Listener es merkt: wie nach einer
    # ``BaseException`` aus einem Handler.
    listener.queue.put_nowait(listener._sentinel)
    listener._thread.join()

    logging.getLogger("swarmq.tests.queued").warning("evt_after_death")
    shutdown_logging()

    assert [r.getMessage() for r in FILE_RECORDS] == ["evt_after_death"]


def test_a_full_bounded_file_mode_queue_counts_the_drop_without_a_traceback(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str],
) -> None:
    from swarmq.logging import dropped_log_records

    FILE_HANDLER_ENTERED.clear()
    FILE_HANDLER_RELEASE.clear()
    path = _config_file(
        tmp_path,
        {
            "version": 1,
            "handlers": {
                "rec": {"()": _BLOCKING_RECORDING},
                "q": {
                    "class": "logging.handlers.QueueHandler",
                    "handlers": ["rec"],
                    "queue": {"()": "queue.Queue", "maxsize": 1},
                },
            },
            "root": {"level": "INFO", "handlers": ["q"]},
        },
    )
    configure_logging(LoggingSettings(log_config=path))
    log = logging.getLogger("swarmq.tests.queued")
    try:
        log.warning("evt_busy")
        assert FILE_HANDLER_ENTERED.wait(5)
        log.warning("evt_queued")
        before = dropped_log_records()

        log.warning("evt_overflow")

        assert dropped_log_records() - before == 1
        assert "Traceback" not in capsys.readouterr().err
    finally:
        FILE_HANDLER_RELEASE.set()


def test_a_shutdown_racing_a_file_mode_restart_leaves_no_orphaned_thread(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.logging import shutdown_logging

    configure_logging(LoggingSettings(log_config=_config_file(tmp_path, _queue_config())))
    log = logging.getLogger("swarmq.tests.queued")
    log.warning("evt_first")
    (handler,) = logging.getLogger().handlers
    thread = handler.listener._thread  # type: ignore[attr-defined]
    before = set(threading.enumerate())
    join = thread.join

    def join_then_log(timeout: float | None = None) -> None:
        # Der Thread ist beendet, ``stop`` hat ihn aber noch nicht vergessen:
        # Ein Eintrag aus einem anderen Thread startet hier einen neuen.
        join(timeout)
        log.warning("evt_in_window")

    thread.join = join_then_log

    shutdown_logging()

    assert not set(threading.enumerate()) - before
    assert [r.getMessage() for r in FILE_RECORDS] == ["evt_first", "evt_in_window"]


def _bounded_blocking_queue_config() -> dict[str, Any]:
    """dictConfig: ``QueueHandler`` mit Queue der Groesse 1 vor dem
    blockierenden Sammel-Handler."""
    return {
        "version": 1,
        "handlers": {
            "rec": {"()": _BLOCKING_RECORDING},
            "q": {
                "class": "logging.handlers.QueueHandler",
                "handlers": ["rec"],
                "queue": {"()": "queue.Queue", "maxsize": 1},
            },
        },
        "root": {"level": "INFO", "handlers": ["q"]},
    }


def _drop_reports() -> list[logging.LogRecord]:
    return [r for r in FILE_RECORDS if r.getMessage() == "log_records_dropped"]


def test_a_file_mode_listener_reports_drops_after_release(
    tmp_path: pathlib.Path,
) -> None:
    from swarmq.logging import shutdown_logging

    FILE_HANDLER_ENTERED.clear()
    FILE_HANDLER_RELEASE.clear()
    FILE_RECORDS.clear()
    path = _config_file(tmp_path, _bounded_blocking_queue_config())
    configure_logging(LoggingSettings(log_config=path))
    log = logging.getLogger("swarmq.tests.queued")
    try:
        log.warning("evt_busy")
        assert FILE_HANDLER_ENTERED.wait(5)
        log.warning("evt_queued")
        log.warning("evt_overflow")
        log.warning("evt_overflow")
    finally:
        FILE_HANDLER_RELEASE.set()
    shutdown_logging()

    reports = _drop_reports()
    assert [r.count for r in reports] == [2]  # type: ignore[attr-defined]
    assert reports[0].name == "swarmq.logging"
    assert reports[0].levelno == logging.WARNING


GATE_ENTERED: dict[str, threading.Event] = {}
GATE_RELEASE: dict[str, threading.Event] = {}
GATE_SEEN = threading.Condition()


class GateRecordingHandler(RecordingHandler):
    """Haelt den Listener der dictConfig bei jedem Record ``evt_hold_*`` fest,
    bis dessen Freigabe in ``GATE_RELEASE`` gesetzt ist."""

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        if msg.startswith("evt_hold_"):
            GATE_ENTERED[msg].set()
            GATE_RELEASE[msg].wait()
        with GATE_SEEN:
            super().emit(record)
            GATE_SEEN.notify_all()


def _wait_for_file_record(msg: str) -> bool:
    with GATE_SEEN:
        return GATE_SEEN.wait_for(
            lambda: msg in [r.getMessage() for r in FILE_RECORDS], timeout=1
        )


def test_stopping_a_file_mode_listener_reports_held_back_drops(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swarmq.logging import shutdown_logging

    monkeypatch.setattr("swarmq.logging_file.time.monotonic", lambda: 100.0)
    FILE_RECORDS.clear()
    config = _bounded_blocking_queue_config()
    config["handlers"]["rec"] = {"()": f"{__name__}.GateRecordingHandler"}
    configure_logging(LoggingSettings(log_config=_config_file(tmp_path, config)))
    log = logging.getLogger("swarmq.tests.queued")
    rounds = ("evt_hold_1", "evt_hold_2")
    for hold in rounds:
        GATE_ENTERED[hold] = threading.Event()
        GATE_RELEASE[hold] = threading.Event()
    try:
        for hold in rounds:
            log.warning(hold)
            assert GATE_ENTERED[hold].wait(1)
            log.warning("evt_fill_%s", hold)
            log.warning("evt_overflow")
            GATE_RELEASE[hold].set()
            assert _wait_for_file_record(f"evt_fill_{hold}")
        assert [r.count for r in _drop_reports()] == [1]  # type: ignore[attr-defined]

        shutdown_logging()
    finally:
        for event in GATE_RELEASE.values():
            event.set()

    assert [r.count for r in _drop_reports()] == [1, 2]  # type: ignore[attr-defined]
