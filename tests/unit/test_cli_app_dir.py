"""``--app-dir`` fuer ``swarmq worker`` und ``swarmq inspect`` (Plan
2026-10-12-1105, MR 6, P6, Entscheidung E1 = C).

Das Console-Script hat in ``sys.path[0]`` das ``bin`` des venv, nicht das
Arbeitsverzeichnis. ``apply_app_dir`` stellt das App-Verzeichnis voran wie
``python -m`` (nicht unter ``-P``/``PYTHONSAFEPATH``); ein ausdrueckliches
``--app-dir`` gilt immer. Die autouse-Fixture ``_restore_sys_path`` in
``tests/unit/conftest.py`` stellt ``sys.path`` nach jedem Test wieder her.

Test-Liste:
1. ausdrueckliches Verzeichnis → absoluter Pfad an ``sys.path[0]``
2. idempotent an Stelle 0; steht der Pfad nur weiter hinten, kommt er
   zusaetzlich nach vorn
3. Default nach E1 C: ohne Flag → Arbeitsverzeichnis; ``safe_path=True`` →
   nichts; ``--app-dir`` unter ``safe_path`` → gesetzt
3a. ``os.getcwd()`` wirft ``FileNotFoundError`` → nichts, kein Fehler
4. ``worker --app-dir /gibt/es/nicht`` → argparse-Fehler, Exit 2, Pfad genannt
4a. dasselbe mit ``--check`` → JSON-Fehler auf stdout, Exit 1
4b. ``_is_check_invocation`` kennt ``--app-dir`` als Wert-Option
5. ``worker --check`` findet ein Modul im Arbeitsverzeichnis
6. ``inspect`` findet ein Modul im Arbeitsverzeichnis
7. ImportError von ``--module`` nennt ``--app-dir`` und das Verzeichnis
7a. dasselbe fuer ``inspect`` (B25)
7b. fehlt nur eine Abhaengigkeit des gefundenen Moduls, kein Hinweis (wie worker)
8. ``--reload``-Neustart: ``main`` mit dem Start-argv setzt den Pfad erneut
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pytest

from swarmq.cli import _is_check_invocation, _shared, main, run_worker_async


def test_explicit_app_dir_is_prepended_as_absolute_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", ["/venv/bin", "/lib"])
    monkeypatch.chdir(tmp_path.parent)

    _shared.apply_app_dir(argparse.Namespace(app_dir=tmp_path.name))

    assert sys.path == [str(tmp_path), "/venv/bin", "/lib"]


def test_app_dir_already_first_is_not_duplicated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", [str(tmp_path), "/venv/bin"])

    _shared.apply_app_dir(argparse.Namespace(app_dir=str(tmp_path)))

    assert sys.path == [str(tmp_path), "/venv/bin"]


def test_app_dir_further_back_is_also_put_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", ["/venv/bin", str(tmp_path)])

    _shared.apply_app_dir(argparse.Namespace(app_dir=str(tmp_path)))

    assert sys.path == [str(tmp_path), "/venv/bin", str(tmp_path)]


def test_without_flag_the_working_directory_is_prepended(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", ["/venv/bin"])
    monkeypatch.chdir(tmp_path)

    _shared.apply_app_dir(argparse.Namespace(app_dir=None), safe_path=False)

    assert sys.path == [str(tmp_path), "/venv/bin"]


def test_safe_path_without_flag_prepends_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", ["/venv/bin"])
    monkeypatch.chdir(tmp_path)

    _shared.apply_app_dir(argparse.Namespace(app_dir=None), safe_path=True)

    assert sys.path == ["/venv/bin"]


def test_explicit_app_dir_applies_under_safe_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", ["/venv/bin"])

    _shared.apply_app_dir(argparse.Namespace(app_dir=str(tmp_path)), safe_path=True)

    assert sys.path == [str(tmp_path), "/venv/bin"]


def test_deleted_working_directory_prepends_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "path", ["/venv/bin"])

    def gone() -> str:
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr("os.getcwd", gone)

    _shared.apply_app_dir(argparse.Namespace(app_dir=None), safe_path=False)

    assert sys.path == ["/venv/bin"]


def test_commands_without_app_dir_option_keep_sys_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``schedule``, ``dlq`` und ``check`` importieren kein App-Modul."""
    monkeypatch.setattr(sys, "path", ["/venv/bin"])
    monkeypatch.chdir(tmp_path)

    _shared.apply_app_dir(argparse.Namespace(command="dlq"), safe_path=False)

    assert sys.path == ["/venv/bin"]


def test_worker_rejects_missing_app_dir(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["worker", "--app-dir", "/gibt/es/nicht", "--module", "m"])

    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "argument --app-dir:" in err
    assert "/gibt/es/nicht" in err


def test_worker_check_renders_missing_app_dir_as_json(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["worker", "--app-dir", "/gibt/es/nicht", "--module", "m", "--check"])

    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["error_type"] == "ArgparseError"
    assert "/gibt/es/nicht" in payload["error"]


def test_check_detector_reads_app_dir_as_value_option() -> None:
    assert _is_check_invocation(["worker", "--app-dir", "d", "--check"]) is True
    # ``--check`` als Wert von ``--app-dir`` ist kein Flag (wie ``--task --check``).
    assert _is_check_invocation(["worker", "--app-dir", "--check", "--module", "m"]) is False


@pytest.fixture
def app_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Arbeitsverzeichnis mit App-Modulen; ``sys.path[0]`` ist ein fremdes
    ``bin`` wie beim Console-Script."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", ["/venv/bin", *(p for p in sys.path if p and p != str(tmp_path))])
    return tmp_path


_TASK_MODULE = """
from swarmq.task import Task, TaskInfo


class AppDirTask(Task):
    task_info = TaskInfo(name="{name}", queues=["default"])

    async def run(self) -> None:
        return None
"""


def test_worker_check_finds_module_in_working_directory(app_cwd: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (app_cwd / "appmod_check.py").write_text(_TASK_MODULE.format(name="appdir.check"))

    code = main(["worker", "--module", "appmod_check", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert (code, payload["ok"]) == (0, True), payload
    assert payload["module_origin"] == str(app_cwd / "appmod_check.py")


def test_inspect_finds_module_in_working_directory(app_cwd: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (app_cwd / "appmod_inspect.py").write_text(_TASK_MODULE.format(name="appdir.inspect"))

    code = main(["inspect", "--module", "appmod_inspect", "--format", "json"])

    out = capsys.readouterr()
    assert code == 0, out.err
    assert "appdir.inspect" in [entry["name"] for entry in json.loads(out.out)]


def test_worker_check_import_error_names_app_dir(app_cwd: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["worker", "--module", "appmod_missing_check", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert "--app-dir" in payload["error"]
    assert str(app_cwd) in payload["error"]


def test_inspect_missing_module_names_app_dir(app_cwd: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """B25: ``inspect`` nennt wie ``worker --check`` ``--app-dir`` und das
    zuerst durchsuchte Verzeichnis."""
    code = main(["inspect", "--module", "appmod_missing_inspect"])

    err = capsys.readouterr().err
    assert code != 0
    assert "--app-dir" in err
    assert str(app_cwd) in err


def test_inspect_missing_dependency_gets_no_app_dir_hint(app_cwd: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Das Modul wurde gefunden; ``--app-dir`` wuerde nicht helfen."""
    (app_cwd / "appmod_dep_inspect.py").write_text("import appmod_dep_fehlt_inspect\n")

    code = main(["inspect", "--module", "appmod_dep_inspect"])

    err = capsys.readouterr().err
    assert code != 0
    assert "appmod_dep_fehlt_inspect" in err
    assert "--app-dir" not in err


@pytest.mark.timeout(5)
async def test_worker_child_import_error_names_inherited_app_dir(app_cwd: Path) -> None:
    sys.path.insert(0, str(app_cwd))  # im Supervisor-Kind geerbt (Spike S2)

    with pytest.raises(ModuleNotFoundError, match="--app-dir") as exc_info:
        await run_worker_async(argparse.Namespace(module="appmod_missing_worker"))

    assert str(app_cwd) in str(exc_info.value)
    assert exc_info.value.name == "appmod_missing_worker"


def test_reload_reexec_applies_app_dir_again(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``--reload``: der Supervisor startet per ``os.execvpe`` mit
    ``sys.orig_argv`` neu; der neue Prozess laeuft wieder durch ``main``."""
    app_dir = tmp_path / "app"
    app_dir.mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://x")
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://x")
    seen: list[str] = []

    def supervisor(*_args: object, **_kwargs: object) -> int:
        seen.append(sys.path[0])
        return 0

    monkeypatch.setattr("swarmq.process_manager.run.run_multi_process_worker", supervisor)
    argv = ["worker", "--module", "appmod_reload", "--app-dir", "app", "--reload"]
    fresh_path = ["/venv/bin", *(p for p in sys.path if p)]

    for _start in ("first", "after_execvpe"):
        monkeypatch.setattr(sys, "path", list(fresh_path))
        assert main(argv) == 0

    assert seen == [str(app_dir), str(app_dir)]
