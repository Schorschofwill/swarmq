"""Tests fuer swarmq.worker.compute.preload (Plan 2026-09-08-001, U8: R28, KTD10).

Test-Liste:
- Import mit SWARMQ_MAIN_MODULE=pkg.mod registriert sys.modules["__main__"] als dieses Modul
- Import mit SWARMQ_MAIN_MODULE=__main__ laesst sys.modules["__main__"] unveraendert
- Import ohne SWARMQ_MAIN_MODULE laesst sys.modules["__main__"] unveraendert
- Import mit SWARMQ_MAIN_MODULE=pkg.__main__ laesst sys.modules["__main__"] unveraendert
  (ein Paket-__main__ wuerde beim Import ungeschuetzt laufen)
Plan 2026-10-06-1811, U6 (R6, KTD6):
- Der Import markiert den Prozess als Fork-Quelle, bevor das Hauptmodul
  importiert wird: Ein Hauptmodul, das die Opt-in-Queue einschaltet und
  danach auf ``swarmq.*`` loggt, startet keinen Listener-Thread.
Plan 2026-10-06-1811, U9 (R12, KTD9) -- Sentinel an Root:
- [x] Mit SWARMQ_FORKSERVER_LOG_SENTINEL=1 ist ``basicConfig`` im Hauptmodul
  wirkungslos: kein StreamHandler an Root, Root-Level unveraendert.
- [x] Ein WARNING aus dem Forkserver erscheint genau einmal auf stderr
  (ueber ``logging.lastResort``), ein INFO nicht; kein Thread startet.
- [x] Ohne die Variable (``configure_logging=False``) gibt es keinen Sentinel:
  ``basicConfig`` im Hauptmodul wirkt.
- [x] Hat Root schon Handler (eigenes Hauptmodul per Pfad, KTD6-Grenze),
  haengt der Preload keinen Sentinel an.
"""

from __future__ import annotations

import importlib
import logging
import sys
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

from swarmq.worker.compute.forkserver import MAIN_MODULE_ENV, SENTINEL_ENV

PRELOAD = "swarmq.worker.compute.preload"
FIXTURE_MAIN = "u8_main_fixture"
FIXTURE_PKG = "u8_pkg"
FIXTURE_OPTIN_MAIN = "u6_optin_main_fixture"
FIXTURE_BASICCONFIG_MAIN = "u9_basicconfig_main_fixture"


@pytest.fixture
def fresh_modules(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Preload-Modul und Hauptmodul-Fixture je Test frisch importierbar; ``__main__`` wiederherstellen."""
    (tmp_path / f"{FIXTURE_MAIN}.py").write_text("EXECUTIONS = 1\n", encoding="utf-8")
    (tmp_path / f"{FIXTURE_OPTIN_MAIN}.py").write_text(
        "import logging\n"
        "from swarmq.logging import _enable_log_queue\n"
        "_enable_log_queue('INFO')\n"
        "logging.getLogger('swarmq.tests.preload').info('main_imported')\n",
        encoding="utf-8",
    )
    (tmp_path / f"{FIXTURE_BASICCONFIG_MAIN}.py").write_text(
        "import logging\n"
        "logging.basicConfig(level=logging.INFO, format='APPFMT|%(message)s')\n"
        "logging.getLogger('u9.forkserver').setLevel(logging.DEBUG)\n"
        "logging.getLogger('u9.forkserver').warning('fs_warning')\n"
        "logging.getLogger('u9.forkserver').info('fs_info')\n",
        encoding="utf-8",
    )
    (tmp_path / FIXTURE_PKG).mkdir()
    (tmp_path / FIXTURE_PKG / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / FIXTURE_PKG / "__main__.py").write_text(
        'raise SystemExit("package __main__ must never run inside the forkserver")\n', encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setitem(sys.modules, "__main__", sys.modules["__main__"])
    stale = (
        PRELOAD,
        FIXTURE_MAIN,
        FIXTURE_OPTIN_MAIN,
        FIXTURE_BASICCONFIG_MAIN,
        FIXTURE_PKG,
        f"{FIXTURE_PKG}.__main__",
    )
    for name in stale:
        sys.modules.pop(name, None)
    try:
        yield
    finally:
        for name in stale:
            sys.modules.pop(name, None)


def test_import_registers_named_main_module_as_dunder_main(fresh_modules, monkeypatch):
    monkeypatch.setenv(MAIN_MODULE_ENV, FIXTURE_MAIN)
    importlib.import_module(PRELOAD)
    main = sys.modules["__main__"]
    assert main is sys.modules[FIXTURE_MAIN]
    assert main.__spec__ is not None and main.__spec__.name == FIXTURE_MAIN  # so prueft spawn._fixup_main_from_name
    assert main.EXECUTIONS == 1


def test_import_with_dunder_main_name_leaves_main_untouched(fresh_modules, monkeypatch):
    monkeypatch.setenv(MAIN_MODULE_ENV, "__main__")
    before = sys.modules["__main__"]
    importlib.import_module(PRELOAD)
    assert sys.modules["__main__"] is before
    assert FIXTURE_MAIN not in sys.modules


def test_import_without_env_leaves_main_untouched(fresh_modules, monkeypatch):
    monkeypatch.delenv(MAIN_MODULE_ENV, raising=False)
    before = sys.modules["__main__"]
    importlib.import_module(PRELOAD)
    assert sys.modules["__main__"] is before


def test_import_with_package_main_name_leaves_main_untouched(fresh_modules, monkeypatch):
    """``paket.__main__`` laeuft beim Import ungeschuetzt; ``_fixup_main_from_name`` fasst es ohnehin nicht an."""
    monkeypatch.setenv(MAIN_MODULE_ENV, f"{FIXTURE_PKG}.__main__")
    before = sys.modules["__main__"]
    importlib.import_module(PRELOAD)
    assert sys.modules["__main__"] is before
    assert f"{FIXTURE_PKG}.__main__" not in sys.modules


def test_import_marks_the_fork_source_before_the_main_module_logs(fresh_modules, monkeypatch):
    from swarmq.logging import LISTENER_THREAD_NAME

    monkeypatch.setenv(MAIN_MODULE_ENV, FIXTURE_OPTIN_MAIN)
    seen: list[str] = []
    collect = logging.Handler()
    collect.emit = lambda record: seen.append(record.getMessage())  # type: ignore[method-assign]
    logging.getLogger().addHandler(collect)

    importlib.import_module(PRELOAD)

    assert seen == ["main_imported"]
    assert not [t for t in threading.enumerate() if t.name == LISTENER_THREAD_NAME]


def _forkserver_root() -> logging.Logger:
    """Root ohne Handler, wie im frischen Forkserver -- im Testkoerper, denn
    pytest haengt seine Handler erst zur Call-Phase an."""
    root = logging.getLogger()
    root.handlers[:] = []
    return root


def test_with_the_sentinel_a_basicconfig_in_the_main_module_has_no_effect(fresh_modules, monkeypatch):
    monkeypatch.setenv(MAIN_MODULE_ENV, FIXTURE_BASICCONFIG_MAIN)
    monkeypatch.setenv(SENTINEL_ENV, "1")
    root = _forkserver_root()
    level = root.level

    importlib.import_module(PRELOAD)

    assert not [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
    assert root.level == level


def test_the_sentinel_passes_a_forkserver_warning_to_last_resort_exactly_once(fresh_modules, monkeypatch, capsys):
    monkeypatch.setenv(MAIN_MODULE_ENV, FIXTURE_BASICCONFIG_MAIN)
    monkeypatch.setenv(SENTINEL_ENV, "1")
    _forkserver_root()
    threads = threading.active_count()

    importlib.import_module(PRELOAD)

    err = capsys.readouterr().err
    assert err.count("fs_warning") == 1
    assert "fs_info" not in err
    assert threading.active_count() == threads


def test_without_the_sentinel_variable_a_basicconfig_in_the_main_module_applies(fresh_modules, monkeypatch):
    monkeypatch.setenv(MAIN_MODULE_ENV, FIXTURE_BASICCONFIG_MAIN)
    monkeypatch.setenv(SENTINEL_ENV, "0")  # --no-configure-logging
    root = _forkserver_root()

    importlib.import_module(PRELOAD)

    [handler] = root.handlers
    assert isinstance(handler, logging.StreamHandler)
    assert root.level == logging.INFO


def test_root_handlers_from_a_main_module_started_by_path_get_no_sentinel(fresh_modules, monkeypatch):
    """KTD6-Grenze: ``python app.py`` importiert das Hauptmodul vor dem
    Preload; hat es Root schon eingerichtet, gewinnt es wie bisher."""
    monkeypatch.setenv(MAIN_MODULE_ENV, FIXTURE_MAIN)
    monkeypatch.setenv(SENTINEL_ENV, "1")
    root = _forkserver_root()
    app_handler = logging.StreamHandler()
    root.addHandler(app_handler)

    importlib.import_module(PRELOAD)

    assert root.handlers == [app_handler]
