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
import io
import json
import logging
import logging.handlers
import pathlib
import sys
import threading
import types
import weakref
from collections.abc import Iterator

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
