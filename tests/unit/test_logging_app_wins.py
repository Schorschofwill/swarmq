"""Regel "App gewinnt" im Worker-Subprozess (Plan 2026-10-06-0831, U7, KTD4).

Das Entry richtet das Logging vor dem App-Import ein. Was die App beim Import
oder in ihrem ``--configure``-Hook selbst einrichtet, gewinnt; swarmq meldet
es und passt sich an. Getrieben ueber ``run_worker_async``, die Stelle, an der
der App-Import passiert.
"""

from __future__ import annotations

# Test-Liste:
# 1. [x] Die App ersetzt beim Import per ``dictConfig`` die Root-Handler:
#        danach laeuft kein swarmq-Listener mehr, und
#        ``logging_reconfigured_by_app`` erscheint genau einmal -- bei den
#        Handlern der App.
# 2. [x] Die dictConfig der App ohne ``disable_existing_loggers`` schaltet
#        bestehende ``swarmq.*``-Logger stumm: swarmq schaltet sie wieder ein
#        und nennt die Zahl im Event; swarmq-Events erscheinen danach trotzdem.
# 3. [x] Die App haengt beim Import einen Handler an Root: swarmqs Handler
#        bleibt, ``logging_handlers_added_by_app`` (WARNING) nennt den Typ.
# 4. [x] Ein ``--configure``-Hook, der die Root-Handler ersetzt, loest dieselbe
#        Reaktion aus wie ein Ersatz beim Import -- und nur einmal.
# 5. [x] ``run_worker_async`` ruft ``configure_logging`` nicht auf; die Handler
#        der App bleiben nach ``SwarmQ(...)`` bestehen.
# 6. [x] Ohne Einrichtung durch swarmq (``--no-configure-logging``) meldet
#        swarmq nichts und fasst nichts an.
# Offen fuer U8: Nach dem Ersatz haelt ein blockierender Root-Handler der App
# einen swarmq-Logaufruf im Event-Loop nicht auf (R6, Opt-in-Queue KTD10).
import argparse
import io
import itertools
import logging
import pathlib
import sys
import threading
import weakref
from collections.abc import Iterator

import pytest

from swarmq import cli
from swarmq.exceptions import ConfigurationError
from swarmq.logging import LISTENER_THREAD_NAME, configure_logging, get_logger
from swarmq.logging_settings import LoggingSettings
from swarmq.task import Task

APP_RECORDS: list[logging.LogRecord] = []
"""Ziel des ``AppHandler`` -- die Handler der App."""


class AppHandler(logging.Handler):
    """Per dictConfig (``"()"``) referenzierbarer Handler der App."""

    def emit(self, record: logging.LogRecord) -> None:
        APP_RECORDS.append(record)


_APP_HANDLER = f"{__name__}.AppHandler"
_MODULE_IDS = itertools.count()


@pytest.fixture(autouse=True)
def isolated_app_logging(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """dictConfig schliesst alle bekannten Handler und deaktiviert Logger:
    Registry leeren, ``disabled``-Flags danach zuruecksetzen. Die Task-
    Registry startet leer, damit ``run_worker_async`` nach dem Import mit
    "no tasks registered" endet, ohne Broker."""
    monkeypatch.setattr(logging, "_handlerList", [])
    monkeypatch.setattr(logging, "_handlers", weakref.WeakValueDictionary())
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    loggers = logging.Logger.manager.loggerDict
    disabled = {
        name: lg.disabled for name, lg in loggers.items() if isinstance(lg, logging.Logger)
    }
    saved_tasks = list(Task._registered_tasks)
    Task._clear_registered_tasks()
    APP_RECORDS.clear()
    yield
    Task._registered_tasks[:] = saved_tasks
    for name, lg in list(loggers.items()):
        if isinstance(lg, logging.Logger):
            lg.disabled = disabled.get(name, False)


def _app_module(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, body: str) -> str:
    """Ein frisch importierbares App-Modul mit ``body`` als Modulcode."""
    name = f"swarmq_test_app_logging_{next(_MODULE_IDS)}"
    (tmp_path / f"{name}.py").write_text("import logging\nimport logging.config\n" + body)
    monkeypatch.syspath_prepend(str(tmp_path))
    return name


def _args(module: str, configure: str | None = None) -> argparse.Namespace:
    return argparse.Namespace(
        module=module, queues=["default"], concurrency=1,
        log_level=None, log_format=None, burst=False,
        burst_idle_timeout=60.0, priorities=False,
        metrics_port=None, health_port=None, configure=configure,
    )


def _listener_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == LISTENER_THREAD_NAME]


def _app_events(name: str) -> list[logging.LogRecord]:
    return [r for r in APP_RECORDS if r.getMessage() == name]


_REPLACE_ROOT = f"""
logging.config.dictConfig({{
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {{"app": {{"()": "{_APP_HANDLER}"}}}},
    "root": {{"level": "INFO", "handlers": ["app"]}},
}})
"""


@pytest.mark.timeout(5)
async def test_app_replacing_root_stops_the_listener_and_reports_once(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_logging(LoggingSettings(format="json"))
    get_logger("swarmq.tests.app_wins").warning("evt_before_import")
    assert len(_listener_threads()) == 1
    module = _app_module(tmp_path, monkeypatch, _REPLACE_ROOT)

    with pytest.raises(ConfigurationError, match="no tasks registered"):
        await cli.run_worker_async(_args(module))

    assert _listener_threads() == []
    assert len(_app_events("logging_reconfigured_by_app")) == 1


@pytest.mark.timeout(5)
async def test_disabled_swarmq_loggers_are_reenabled_and_counted(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_logging(LoggingSettings(format="json"))
    existing = logging.getLogger("swarmq.tests.app_wins.existing")
    module = _app_module(
        tmp_path, monkeypatch,
        _REPLACE_ROOT.replace('"disable_existing_loggers": False,', ""),
    )

    with pytest.raises(ConfigurationError, match="no tasks registered"):
        await cli.run_worker_async(_args(module))
    get_logger("swarmq.tests.app_wins.existing").warning("evt_after_import")

    assert not existing.disabled
    (event,) = _app_events("logging_reconfigured_by_app")
    assert event.reenabled_loggers >= 1  # type: ignore[attr-defined]
    assert len(_app_events("evt_after_import")) == 1


@pytest.mark.timeout(5)
async def test_added_root_handler_is_reported_and_kept(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, object]],
) -> None:
    configure_logging(LoggingSettings(format="json"))
    (swarmq_handler,) = (
        h for h in logging.getLogger().handlers if type(h).__module__ == "swarmq.logging"
    )
    module = _app_module(
        tmp_path, monkeypatch,
        f"import {__name__} as t\nlogging.getLogger().addHandler(t.AppHandler())\n",
    )

    with pytest.raises(ConfigurationError, match="no tasks registered"):
        await cli.run_worker_async(_args(module))

    assert swarmq_handler in logging.getLogger().handlers
    (event,) = [e for e in swarmq_logs if e["event"] == "logging_handlers_added_by_app"]
    assert event["log_level"] == "warning"
    assert event["handler_types"] == ["AppHandler"]
    assert not [e for e in swarmq_logs if e["event"] == "logging_reconfigured_by_app"]


def _fake_swarmq(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ein Task ist registriert und ``SwarmQ`` ersetzt: kein Broker, kein
    Logging-Aufbau in der App."""

    class _Probe(Task):
        from swarmq.task import TaskInfo

        task_info = TaskInfo(name="app_wins_probe", queues=["default"])

        async def run(self) -> None:
            pass

    class _FakeApp:
        def __init__(self, broker: object, backend: object, **kwargs: object) -> None:
            pass

        async def __aenter__(self) -> _FakeApp:
            return self

        async def __aexit__(self, *exc: object) -> bool:
            return False

        async def start_worker(self) -> None:
            return None

    monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://127.0.0.1:1/")
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://127.0.0.1:1/0")
    monkeypatch.setattr("swarmq.app.SwarmQ", _FakeApp)


_HOOK_REPLACES_ROOT = "def setup(app):\n" + "".join(
    f"    {line}\n" for line in _REPLACE_ROOT.strip().splitlines()
)


@pytest.mark.timeout(5)
async def test_configure_hook_replacing_root_reacts_like_an_import(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_logging(LoggingSettings(format="json"))
    get_logger("swarmq.tests.app_wins").warning("evt_before_import")
    _fake_swarmq(monkeypatch)
    module = _app_module(tmp_path, monkeypatch, _HOOK_REPLACES_ROOT)

    await cli.run_worker_async(_args(module, configure=f"{module}:setup"))

    assert _listener_threads() == []
    assert len(_app_events("logging_reconfigured_by_app")) == 1


@pytest.mark.timeout(5)
async def test_replacement_at_import_and_in_the_hook_is_reported_once(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_logging(LoggingSettings(format="json"))
    _fake_swarmq(monkeypatch)
    module = _app_module(tmp_path, monkeypatch, _REPLACE_ROOT + _HOOK_REPLACES_ROOT)

    await cli.run_worker_async(_args(module, configure=f"{module}:setup"))

    assert len(_app_events("logging_reconfigured_by_app")) == 1


@pytest.mark.timeout(5)
async def test_app_handlers_survive_swarmq_construction(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KTD4: ``run_worker_async`` richtet nichts ein, und ``SwarmQ(...)``
    ueberschreibt die Einrichtung der App nicht."""
    from unittest.mock import AsyncMock

    from swarmq.app import SwarmQ

    configure_logging(LoggingSettings(format="json"))
    _fake_swarmq(monkeypatch)
    monkeypatch.setattr("swarmq.app.SwarmQ", SwarmQ)  # die echte App
    monkeypatch.setattr(SwarmQ, "__aenter__", AsyncMock())
    monkeypatch.setattr(SwarmQ, "__aexit__", AsyncMock(return_value=False))
    monkeypatch.setattr(SwarmQ, "start_worker", AsyncMock())
    module = _app_module(tmp_path, monkeypatch, _REPLACE_ROOT)

    await cli.run_worker_async(_args(module))

    assert [type(h).__name__ for h in logging.getLogger().handlers] == ["AppHandler"]


@pytest.mark.timeout(5)
async def test_without_swarmq_setup_nothing_is_reported_or_touched(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, object]],
) -> None:
    """R12: Unter ``--no-configure-logging`` gehoert das Logging der App --
    auch stummgeschaltete ``swarmq.*``-Logger bleiben ihre Entscheidung."""
    configure_logging(LoggingSettings(configure=False))
    existing = logging.getLogger("swarmq.tests.app_wins.quiet")
    module = _app_module(
        tmp_path, monkeypatch,
        _REPLACE_ROOT + 'logging.getLogger("swarmq.tests.app_wins.quiet").disabled = True\n',
    )

    with pytest.raises(ConfigurationError, match="no tasks registered"):
        await cli.run_worker_async(_args(module))

    assert existing.disabled
    assert not [e for e in swarmq_logs if str(e["event"]).startswith("logging_")]
