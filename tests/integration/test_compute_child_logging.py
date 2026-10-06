"""Logging der Rechenkinder mit echtem Forkserver (Plan 2026-10-06-0831, U7 Teil b).

Ohne Broker: ``_compute_logging_driver`` startet Forkserver und ``SlotPool`` in
einem eigenen Prozess und fuehrt Jobs in frischen (``child_once``) und
wiederverwendeten Kindern (``child_loop``) aus. Das Vorlademodul erzeugt beim
Import einen swarmq-Logger und loggt einmal.

Test-Liste:
1. [x] Forkserver: Nach dem Vorladen eines Moduls, das ``swarmq.logging``
       importiert, einen swarmq-Logger erzeugt und einmal loggt, besteht der
       Thread-Check (genau ein Thread, AE6-Vorstufe; der Opt-in folgt in U8).
2. [x] Eingebaut aus den Job-Feldern: JSON und Level gelten in frischen und
       wiederverwendeten Kindern, jede Task-Zeile erscheint genau einmal.
3. [x] Datei-Modus mit ``QueueHandler``: die Kinder schreiben ueber die
       dictConfig aus dem Job, auch der zweite Job im wiederverwendeten Kind
       (Leerung pro Job, Listener startet wieder).
4. [x] Die App richtet beim Vorladen Root-Handler ein: die Kinder behalten
       sie, und ``logging_reconfigured_by_app`` erscheint einmal je Pool.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.timeout(60)

_REPO_ROOT = Path(__file__).resolve().parents[2]

_TASK_MODULE = '''
import logging
import logging.config

from swarmq.logging import get_logger
from swarmq.task import Task, TaskInfo

get_logger("swarmq.tests.preload").warning("preload_imported")
{app_setup}

class ChildLoggingTask(Task):
    task_info = TaskInfo(name="child_logging_task", queues=["q"])

    async def run(self, tag: str) -> str:
        logging.getLogger("app.task").warning("task_line_%s", tag)
        logging.getLogger("app.task").info("info_line_%s", tag)
        return tag
'''


def _run(tmp_path: Path, spec: dict[str, Any], app_setup: str = "") -> tuple[dict[str, Any], str]:
    """Treiber starten; liefert die Ergebniszeile und das stderr aller Prozesse."""
    module = f"child_logging_app_{os.getpid()}_{tmp_path.name}".replace("-", "_")
    (tmp_path / f"{module}.py").write_text(_TASK_MODULE.format(app_setup=textwrap.dedent(app_setup)))
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(tmp_path), str(_REPO_ROOT)]))
    proc = subprocess.run(
        [sys.executable, "-m", "tests.integration._compute_logging_driver", module, json.dumps(spec)],
        cwd=_REPO_ROOT, env=env, capture_output=True, text=True, timeout=50,
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    return json.loads(proc.stdout.strip().splitlines()[-1]), proc.stderr


def _json_events(stderr: str) -> list[str]:
    events = []
    for line in stderr.splitlines():
        if line.startswith("{"):
            events.append(json.loads(line)["event"])
    return events


def test_builtin_children_follow_the_job_and_the_preload_keeps_one_thread(tmp_path: Path) -> None:
    out, stderr = _run(
        tmp_path, {"use_di": [False, False, True, True], "job": {"log_json": True, "log_level": "WARNING"}},
    )

    assert out["threads"] in (1, None)  # None: kein /proc
    assert out["results"] == ["j0", "j1", "j2", "j3"]
    task_lines = [e for e in _json_events(stderr) if e.startswith(("task_line", "info_line"))]
    assert sorted(task_lines) == ["task_line_j0", "task_line_j1", "task_line_j2", "task_line_j3"]


def test_file_mode_children_write_through_the_job_dictconfig(tmp_path: Path) -> None:
    log_file = tmp_path / "children.log"
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"plain": {"format": "%(message)s"}},
        "handlers": {
            "file": {"class": "logging.FileHandler", "filename": str(log_file), "formatter": "plain"},
            "q": {"class": "logging.handlers.QueueHandler", "handlers": ["file"]},
        },
        "root": {"level": "INFO", "handlers": ["q"]},
    }
    job = {"log_config": "/never/read.json", "log_config_json": json.dumps(config)}

    out, _ = _run(tmp_path, {"use_di": [False, True, True], "job": job})

    assert out["results"] == ["j0", "j1", "j2"]
    lines = [line for line in log_file.read_text().splitlines() if "_line_" in line]
    assert sorted(lines) == sorted(
        f"{kind}_line_j{i}" for i in range(3) for kind in ("task", "info")
    )


def test_app_root_handlers_from_the_preload_win_and_are_reported_once_per_pool(tmp_path: Path) -> None:
    app_log = tmp_path / "app.log"
    app_setup = f"""
    logging.config.dictConfig({{
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {{"plain": {{"format": "%(message)s"}}}},
        "handlers": {{"app": {{
            "class": "logging.FileHandler", "filename": {str(app_log)!r}, "formatter": "plain",
        }}}},
        "root": {{"level": "WARNING", "handlers": ["app"]}},
    }})
    """

    out, stderr = _run(
        tmp_path, {"use_di": [False, False, True, True], "job": {"log_json": True}}, app_setup,
    )

    assert out["threads"] in (1, None)
    assert out["results"] == ["j0", "j1", "j2", "j3"]
    lines = [line for line in app_log.read_text().splitlines() if "_line_" in line]
    assert sorted(lines) == [f"task_line_j{i}" for i in range(4)]
    assert _json_events(stderr).count("logging_reconfigured_by_app") == 1
    assert not any(e.startswith("task_line") for e in _json_events(stderr))
